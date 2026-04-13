import * as core from '@actions/core'
import * as crypto from 'crypto'
import { sleep } from './utils'
import {
  execPodStep,
  execPodStepOutput,
  execPodStepWithRetry,
  getPodByName
} from './index'
import { RPC_SERVER_SCRIPT } from './rpc-server-script'

interface RpcStatusResponse {
  id: string
  status: 'idle' | 'running' | 'completed' | 'failed'
  exit_code: number | null
}

const HEALTH_POLL_INTERVAL_MS = 1000
const HEALTH_TIMEOUT_MS = 30000
const HEARTBEAT_INTERVAL_MS = 3000
const MAX_DEPLOY_ATTEMPTS = 3
const HEARTBEAT_GRACE_MS = 60000
const LOG_POLL_INTERVAL_MS = 200
const FETCH_TIMEOUT_MS = 10000
const PORT_FILE = '/tmp/rpc-server.port'
const PORT_DISCOVER_TIMEOUT_MS = 5000
const PORT_DISCOVER_POLL_MS = 200

function rpcUrl(podIp: string, port: number, path: string): string {
  return `http://${podIp}:${port}${path}`
}

async function healthCheck(
  podName: string,
  containerName: string,
  port: number
): Promise<boolean> {
  try {
    const exitCode = await execPodStep(
      [
        'sh',
        '-c',
        `python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${port}/health')"`
      ],
      podName,
      containerName
    )
    return exitCode === 0
  } catch {
    return false
  }
}

async function discoverPort(
  podName: string,
  containerName: string
): Promise<number | null> {
  const startTime = Date.now()
  while (Date.now() - startTime < PORT_DISCOVER_TIMEOUT_MS) {
    try {
      const { exitCode, stdout } = await execPodStepOutput(
        ['cat', PORT_FILE],
        podName,
        containerName
      )
      if (exitCode === 0 && stdout) {
        const port = parseInt(stdout, 10)
        if (!isNaN(port) && port > 0) {
          core.debug(`Discovered RPC server port: ${port}`)
          return port
        }
      }
    } catch {
      // Port file may not exist yet, keep polling
    }
    await sleep(PORT_DISCOVER_POLL_MS)
  }
  return null
}

export async function deployRpcServer(
  podName: string,
  containerName: string,
  token: string
): Promise<{ podIp: string; port: number }> {
  const pod = await getPodByName(podName)
  const podIp = pod.status?.podIP
  if (!podIp) {
    throw new Error(`Pod ${podName} has no IP address`)
  }

  try {
    await execPodStepWithRetry(
      ['python3', '--version'],
      podName,
      containerName,
      'check python3'
    )
  } catch {
    throw new Error('image not compatible: python3 is a required dependency')
  }

  const encoded = Buffer.from(RPC_SERVER_SCRIPT).toString('base64')
  await execPodStepWithRetry(
    [
      'sh',
      '-c',
      `echo '${encoded}' | base64 -d > /tmp/rpc-server.py && chmod +x /tmp/rpc-server.py`
    ],
    podName,
    containerName,
    'write rpc server'
  )

  for (let attempt = 1; attempt <= MAX_DEPLOY_ATTEMPTS; attempt++) {
    core.debug(
      `Starting RPC server (attempt ${attempt}/${MAX_DEPLOY_ATTEMPTS})`
    )

    // Kill any server from a previous failed attempt and clean up port file
    await execPodStep(
      ['sh', '-c', `pkill -f 'python3 /tmp/rpc-server.py' 2>/dev/null; rm -f ${PORT_FILE}`],
      podName,
      containerName
    )

    // Start server with port 0 — OS assigns a free port
    await execPodStepWithRetry(
      [
        'sh',
        '-c',
        `nohup python3 /tmp/rpc-server.py --port 0 --token ${token} > /tmp/rpc-server.log 2>&1 & echo $!`
      ],
      podName,
      containerName,
      'start rpc server'
    )

    // Discover the actual port assigned by the OS
    const port = await discoverPort(podName, containerName)
    if (port === null) {
      core.debug(
        `Failed to discover RPC server port, attempt ${attempt}/${MAX_DEPLOY_ATTEMPTS}`
      )
      continue
    }

    const startTime = Date.now()
    let healthy = false
    while (Date.now() - startTime < HEALTH_TIMEOUT_MS) {
      if (await healthCheck(podName, containerName, port)) {
        core.debug(`RPC server healthy after ${Date.now() - startTime}ms`)
        healthy = true
        break
      }
      await sleep(HEALTH_POLL_INTERVAL_MS)
    }

    if (healthy) {
      return { podIp, port }
    }

    core.debug(
      `RPC server failed health check on port ${port}, attempt ${attempt}/${MAX_DEPLOY_ATTEMPTS}`
    )
  }

  throw new Error(
    `RPC server failed to become healthy after ${MAX_DEPLOY_ATTEMPTS} attempts`
  )
}

export async function rpcPodStep(
  podIp: string,
  port: number,
  scriptPath: string,
  token: string
): Promise<number> {
  const id = crypto.randomUUID()
  const headers = { 'X-Auth-Token': token, 'Content-Type': 'application/json' }

  // Retry /exec on network errors and 5xx; throw immediately on 4xx
  const maxExecAttempts = 3
  const execRetryDelayMs = 1000
  for (let attempt = 1; attempt <= maxExecAttempts; attempt++) {
    let execResp: Response
    try {
      execResp = await fetch(rpcUrl(podIp, port, '/exec'), {
        method: 'POST',
        headers,
        body: JSON.stringify({ id, path: scriptPath }),
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })
    } catch (fetchErr) {
      if (attempt < maxExecAttempts) {
        core.debug(
          `RPC /exec network error (attempt ${attempt}/${maxExecAttempts}): ${fetchErr}`
        )
        await sleep(execRetryDelayMs)
        continue
      }
      throw new Error(
        `RPC /exec network error after ${maxExecAttempts} attempts: ${fetchErr}`
      )
    }

    if (execResp.ok) {
      break
    }

    if (execResp.status >= 400 && execResp.status < 500) {
      const body = await execResp.text().catch(() => '')
      throw new Error(`RPC /exec failed (${execResp.status}): ${body}`)
    }

    if (attempt < maxExecAttempts) {
      core.debug(
        `RPC /exec server error ${execResp.status} (attempt ${attempt}/${maxExecAttempts}), retrying`
      )
      await sleep(execRetryDelayMs)
      continue
    }

    const body = await execResp.text().catch(() => '')
    throw new Error(
      `RPC /exec failed after ${maxExecAttempts} attempts (${execResp.status}): ${body}`
    )
  }

  let lastSuccessfulHeartbeat = Date.now()
  const heartbeatInterval = setInterval(async () => {
    try {
      const resp = await fetch(rpcUrl(podIp, port, '/heartbeat'), {
        method: 'POST',
        headers: { 'X-Auth-Token': token },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })
      if (resp.ok) {
        lastSuccessfulHeartbeat = Date.now()
      } else if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        clearInterval(heartbeatInterval)
      }
    } catch {
      if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        clearInterval(heartbeatInterval)
      }
    }
  }, HEARTBEAT_INTERVAL_MS)

  let stdoutOffset = 0
  let stderrOffset = 0
  let exitCode = -1

  try {
    while (true) {
      if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        throw new Error('RPC heartbeat failed for 60s — infrastructure failure')
      }

      // Log streaming — stdout and stderr in parallel
      try {
        const [stdoutData, stderrData] = await Promise.all([
          fetchLogStream(podIp, port, token, 'stdout', stdoutOffset),
          fetchLogStream(podIp, port, token, 'stderr', stderrOffset)
        ])
        if (stdoutData.byteLength > 0) {
          process.stdout.write(Buffer.from(stdoutData))
          stdoutOffset += stdoutData.byteLength
        }
        if (stderrData.byteLength > 0) {
          process.stderr.write(Buffer.from(stderrData))
          stderrOffset += stderrData.byteLength
        }
      } catch {
        core.debug('log fetch failed, will retry')
      }

      // Status polling
      try {
        const statusResp = await fetch(rpcUrl(podIp, port, '/status'), {
          headers: { 'X-Auth-Token': token },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
        })
        if (statusResp.ok) {
          const status = (await statusResp.json()) as RpcStatusResponse
          if (status.status === 'completed' || status.status === 'failed') {
            exitCode = status.exit_code ?? -1
            break
          }
        }
      } catch {
        core.debug('status fetch failed, will retry')
      }

      await sleep(LOG_POLL_INTERVAL_MS)
    }

    // Final log flush — drain all remaining data from both streams
    try {
      let flushing = true
      while (flushing) {
        const [stdoutData, stderrData] = await Promise.all([
          fetchLogStream(podIp, port, token, 'stdout', stdoutOffset),
          fetchLogStream(podIp, port, token, 'stderr', stderrOffset)
        ])
        flushing = false
        if (stdoutData.byteLength > 0) {
          process.stdout.write(Buffer.from(stdoutData))
          stdoutOffset += stdoutData.byteLength
          flushing = true
        }
        if (stderrData.byteLength > 0) {
          process.stderr.write(Buffer.from(stderrData))
          stderrOffset += stderrData.byteLength
          flushing = true
        }
      }
    } catch {
      core.debug('final log flush failed')
    }

    return exitCode
  } finally {
    clearInterval(heartbeatInterval)
  }
}

async function fetchLogStream(
  podIp: string,
  port: number,
  token: string,
  stream: 'stdout' | 'stderr',
  offset: number
): Promise<ArrayBuffer> {
  const resp = await fetch(
    rpcUrl(podIp, port, `/logs?stream=${stream}&offset=${offset}`),
    {
      headers: { 'X-Auth-Token': token },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
    }
  )
  if (!resp.ok) {
    return new ArrayBuffer(0)
  }
  return resp.arrayBuffer()
}
