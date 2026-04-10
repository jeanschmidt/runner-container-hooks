import * as core from '@actions/core'
import * as crypto from 'crypto'
import { sleep } from './utils'
import { execPodStepWithRetry, getPodByName } from './index'
import { RPC_SERVER_SCRIPT } from './rpc-server-script'

const RPC_PORT = 8080
const HEALTH_POLL_INTERVAL_MS = 1000
const HEALTH_TIMEOUT_MS = 30000
const HEARTBEAT_INTERVAL_MS = 1000
const HEARTBEAT_GRACE_MS = 60000
const LOG_POLL_INTERVAL_MS = 200
const FETCH_TIMEOUT_MS = 10000

function rpcUrl(podIp: string, port: number, path: string): string {
  return `http://${podIp}:${port}${path}`
}

async function healthCheck(podIp: string, port: number): Promise<boolean> {
  try {
    const resp = await fetch(rpcUrl(podIp, port, '/health'), {
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
    })
    return resp.status === 200
  } catch {
    return false
  }
}

export async function deployRpcServer(
  podName: string,
  containerName: string
): Promise<{ podIp: string; port: number }> {
  const pod = await getPodByName(podName)
  const podIp = pod.status?.podIP
  if (!podIp) {
    throw new Error(`Pod ${podName} has no IP address`)
  }

  if (await healthCheck(podIp, RPC_PORT)) {
    core.debug('RPC server already running, skipping deploy')
    return { podIp, port: RPC_PORT }
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

  await execPodStepWithRetry(
    [
      'sh',
      '-c',
      `nohup python3 /tmp/rpc-server.py --port ${RPC_PORT} > /tmp/rpc-server.log 2>&1 & echo $!`
    ],
    podName,
    containerName,
    'start rpc server'
  )

  const startTime = Date.now()
  while (Date.now() - startTime < HEALTH_TIMEOUT_MS) {
    if (await healthCheck(podIp, RPC_PORT)) {
      core.debug(`RPC server healthy after ${Date.now() - startTime}ms`)
      return { podIp, port: RPC_PORT }
    }
    await sleep(HEALTH_POLL_INTERVAL_MS)
  }

  throw new Error(
    `RPC server failed to become healthy after ${HEALTH_TIMEOUT_MS}ms`
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

  const execResp = await fetch(rpcUrl(podIp, port, '/exec'), {
    method: 'POST',
    headers,
    body: JSON.stringify({ id, path: scriptPath }),
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
  })
  if (!execResp.ok) {
    const body = await execResp.text().catch(() => '')
    throw new Error(`RPC /exec failed (${execResp.status}): ${body}`)
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
        // The while loop below will also detect this via the flag
      }
    } catch {
      if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        clearInterval(heartbeatInterval)
      }
    }
  }, HEARTBEAT_INTERVAL_MS)

  let byteOffset = 0
  let exitCode = -1

  try {
    while (true) {
      if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        throw new Error('RPC heartbeat failed for 60s — infrastructure failure')
      }

      // Log streaming
      try {
        const logResp = await fetch(
          rpcUrl(podIp, port, `/logs?offset=${byteOffset}`),
          {
            headers: { 'X-Auth-Token': token },
            signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
          }
        )
        if (logResp.ok) {
          const logData = await logResp.arrayBuffer()
          if (logData.byteLength > 0) {
            process.stdout.write(Buffer.from(logData))
            byteOffset += logData.byteLength
          }
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
          const status = await statusResp.json()
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

    // Final log flush
    try {
      const logResp = await fetch(
        rpcUrl(podIp, port, `/logs?offset=${byteOffset}`),
        {
          headers: { 'X-Auth-Token': token },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
        }
      )
      if (logResp.ok) {
        const logData = await logResp.arrayBuffer()
        if (logData.byteLength > 0) {
          process.stdout.write(Buffer.from(logData))
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
