// Pure helpers for diagnosing why the in-pod RPC server died. Kept free of
// @kubernetes/client-node and @actions/core imports so the parsing/classifying
// logic can be unit-tested directly (the I/O — exec, pod lookup — lives in
// rpc.ts and feeds these functions).

export interface PodFailureDiagnostic {
  cause:
    | 'container-oom'
    | 'pod-evicted'
    | 'node-failure'
    // The RPC server (a child process, not the container's PID 1) vanished
    // while the container stayed up. The cgroup probe tells these apart:
    | 'cgroup-oom-group' // whole-cgroup kill (memory.oom.group=1); oom_score_adj moot
    | 'cgroup-oom' // a process in the cgroup was OOM-killed (per-process)
    | 'rpc-process-crash' // no OOM recorded — server-side crash (e.g. panic)
    | 'rpc-process-died' // server gone but the probe couldn't run — cause unknown
    | 'unknown'
  exitCode: number
  message: string
}

// Probe the container's cgroup once the RPC server has vanished but PID 1 is
// still up (which keeps /sys/fs/cgroup readable). Emits KEY=VALUE lines, then
// the server log after a ---LOG--- marker. oom_kill / oom_group_kill are
// cumulative kernel counters, so reading them post-mortem is valid; memory.peak
// is the high-water mark that survives the kill (memory.current already dropped).
export const CONTAINER_DEATH_PROBE = `
set +e
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
  echo CGROUP=v2
  awk '/^oom_kill /{print "OOM_KILL="$2} /^oom_group_kill /{print "OOM_GROUP_KILL="$2}' /sys/fs/cgroup/memory.events 2>/dev/null
  printf 'MEM_PEAK=%s\\n' "$(cat /sys/fs/cgroup/memory.peak 2>/dev/null)"
  printf 'MEM_MAX=%s\\n' "$(cat /sys/fs/cgroup/memory.max 2>/dev/null)"
else
  echo CGROUP=v1
  awk '/oom_kill /{print "OOM_KILL="$2}' /sys/fs/cgroup/memory/memory.oom_control 2>/dev/null
  printf 'MEM_PEAK=%s\\n' "$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes 2>/dev/null)"
  printf 'MEM_MAX=%s\\n' "$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null)"
fi
echo ---LOG---
tail -30 /tmp/rpc-server.log 2>/dev/null
`

export interface ContainerDeathEvidence {
  oomKill: number
  oomGroupKill: number
  memPeak: number | null
  memMax: number | null // null when "max" (unlimited) or unreadable
  log: string
}

export function parseContainerDeathProbe(out: string): ContainerDeathEvidence {
  const [head, ...logParts] = out.split('---LOG---')
  const fields = new Map<string, string>()
  for (const line of head.split('\n')) {
    const eq = line.indexOf('=')
    if (eq > 0) fields.set(line.slice(0, eq).trim(), line.slice(eq + 1).trim())
  }
  const num = (k: string): number | null => {
    const v = fields.get(k)
    if (!v || v === 'max') return null
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  }
  return {
    oomKill: num('OOM_KILL') ?? 0,
    oomGroupKill: num('OOM_GROUP_KILL') ?? 0,
    memPeak: num('MEM_PEAK'),
    memMax: num('MEM_MAX'),
    log: logParts.join('---LOG---').trim()
  }
}

// Classify a vanished RPC server from the cgroup evidence. The priority matters:
// a whole-cgroup group-kill also bumps oom_kill, so it must be checked first.
export function classifyContainerDeath(
  ev: ContainerDeathEvidence
): PodFailureDiagnostic {
  const memCtx =
    ev.memPeak !== null && ev.memMax !== null
      ? ` (memory.peak=${ev.memPeak} / memory.max=${ev.memMax})`
      : ''
  const logCtx = ev.log ? `\nServer log:\n${ev.log}` : ''

  if (ev.oomGroupKill > 0) {
    return {
      cause: 'cgroup-oom-group',
      exitCode: 137,
      message:
        `RPC server killed by a whole-cgroup OOM kill ` +
        `(memory.oom.group=1, oom_group_kill=${ev.oomGroupKill})${memCtx}. ` +
        `The kernel kills every process in the cgroup together in this mode, ` +
        `so the server's oom_score_adj cannot protect it — reduce the job's ` +
        `memory use or raise the container limit.${logCtx}`
    }
  }
  if (ev.oomKill > 0) {
    return {
      cause: 'cgroup-oom',
      exitCode: 137,
      message:
        `RPC server process OOM-killed inside the container ` +
        `(oom_kill=${ev.oomKill})${memCtx}.${logCtx}`
    }
  }
  return {
    cause: 'rpc-process-crash',
    exitCode: -1,
    message:
      `RPC server process died with no OOM recorded in the container cgroup ` +
      `(oom_kill=0)${memCtx} — this points to a server-side crash, not memory ` +
      `pressure. Check the server log below for a panic.${logCtx}`
  }
}
