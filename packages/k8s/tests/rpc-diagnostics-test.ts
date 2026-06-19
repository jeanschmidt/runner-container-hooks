import {
  classifyContainerDeath,
  parseContainerDeathProbe
} from '../src/k8s/rpc-diagnostics'

// These strings mirror exactly what CONTAINER_DEATH_PROBE prints to stdout
// (KEY=VALUE lines, then a ---LOG--- marker, then the server log tail), so the
// tests exercise the real contract between the probe script and the parser.

describe('parseContainerDeathProbe', () => {
  it('parses a cgroup v2 probe with a whole-cgroup OOM kill', () => {
    const out = [
      'CGROUP=v2',
      'OOM_KILL=3',
      'OOM_GROUP_KILL=1',
      'MEM_PEAK=2097152000',
      'MEM_MAX=2097152000',
      '---LOG---',
      "thread 'main' panicked",
      'FATAL rpc-server panic: boom'
    ].join('\n')

    expect(parseContainerDeathProbe(out)).toEqual({
      oomKill: 3,
      oomGroupKill: 1,
      memPeak: 2097152000,
      memMax: 2097152000,
      log: "thread 'main' panicked\nFATAL rpc-server panic: boom"
    })
  })

  it('treats memory.max "max" (unlimited) as null', () => {
    const out = [
      'CGROUP=v2',
      'MEM_PEAK=123',
      'MEM_MAX=max',
      '---LOG---',
      ''
    ].join('\n')
    const ev = parseContainerDeathProbe(out)
    expect(ev.memPeak).toBe(123)
    expect(ev.memMax).toBeNull()
  })

  it('defaults missing OOM counters to 0 (cgroup v1, no oom_group_kill)', () => {
    const out = [
      'CGROUP=v1',
      'OOM_KILL=2',
      'MEM_PEAK=500',
      'MEM_MAX=1000',
      '---LOG---'
    ].join('\n')
    const ev = parseContainerDeathProbe(out)
    expect(ev.oomKill).toBe(2)
    expect(ev.oomGroupKill).toBe(0) // line absent → default 0, not NaN
  })

  it('returns zeros/empty when the probe produced nothing (exec failed)', () => {
    const ev = parseContainerDeathProbe('')
    expect(ev).toEqual({
      oomKill: 0,
      oomGroupKill: 0,
      memPeak: null,
      memMax: null,
      log: ''
    })
  })

  it('preserves a log body that itself contains the ---LOG--- marker', () => {
    const out = ['OOM_KILL=0', '---LOG---', 'noise ---LOG--- still log'].join(
      '\n'
    )
    expect(parseContainerDeathProbe(out).log).toBe('noise ---LOG--- still log')
  })
})

describe('classifyContainerDeath', () => {
  const base = {
    oomKill: 0,
    oomGroupKill: 0,
    memPeak: null,
    memMax: null,
    log: ''
  }

  it('flags a whole-cgroup kill and notes oom_score_adj cannot help', () => {
    const d = classifyContainerDeath({ ...base, oomGroupKill: 1, oomKill: 4 })
    expect(d.cause).toBe('cgroup-oom-group')
    expect(d.exitCode).toBe(137)
    expect(d.message).toMatch(/oom_score_adj cannot protect/)
  })

  it('prioritises the group kill even when oom_kill is also set', () => {
    // A group kill bumps oom_kill too, so order matters.
    expect(
      classifyContainerDeath({ ...base, oomGroupKill: 1, oomKill: 9 }).cause
    ).toBe('cgroup-oom-group')
  })

  it('reports a per-process OOM when only oom_kill is set', () => {
    const d = classifyContainerDeath({ ...base, oomKill: 1 })
    expect(d.cause).toBe('cgroup-oom')
    expect(d.exitCode).toBe(137)
  })

  it('reports a crash (not OOM) when no kill is recorded', () => {
    const d = classifyContainerDeath({ ...base, oomKill: 0 })
    expect(d.cause).toBe('rpc-process-crash')
    expect(d.exitCode).toBe(-1)
    expect(d.message).toMatch(/server-side crash/)
  })

  it('includes the memory.peak/max context only when both are known', () => {
    expect(
      classifyContainerDeath({
        ...base,
        oomKill: 1,
        memPeak: 800,
        memMax: 1000
      }).message
    ).toContain('memory.peak=800 / memory.max=1000')
    expect(
      classifyContainerDeath({ ...base, oomKill: 1, memMax: null }).message
    ).not.toContain('memory.peak')
  })

  it('appends the captured server log when present', () => {
    const d = classifyContainerDeath({
      ...base,
      oomKill: 0,
      log: 'panic trace'
    })
    expect(d.message).toContain('Server log:\npanic trace')
  })
})

describe('parse + classify (end to end of the pure pipeline)', () => {
  it('classifies a real-looking crash probe as a crash, surfacing the panic', () => {
    const probe = [
      'CGROUP=v2',
      'OOM_KILL=0',
      'OOM_GROUP_KILL=0',
      'MEM_PEAK=104857600', // 100 MiB peak
      'MEM_MAX=2147483648', // 2 GiB limit — nowhere near the limit
      '---LOG---',
      "FATAL rpc-server panic: thread 'unnamed' panicked at src/main.rs:123"
    ].join('\n')

    const d = classifyContainerDeath(parseContainerDeathProbe(probe))
    expect(d.cause).toBe('rpc-process-crash')
    expect(d.message).toContain('memory.peak=104857600 / memory.max=2147483648')
    expect(d.message).toContain('FATAL rpc-server panic')
  })
})
