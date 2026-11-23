import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { resolveUserVolumeSourcePath } from '../src/k8s/utils'

describe('resolveUserVolumeSourcePath', () => {
  const env = { ...process.env }
  let base: string
  let work: string

  beforeEach(() => {
    base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'volumes-')))
    work = path.join(base, '_work')
    for (const dir of [
      'repo/repo/data',
      'repo/repo-evil',
      'cache',
      '_actions'
    ]) {
      fs.mkdirSync(path.join(work, dir), { recursive: true })
    }
    fs.mkdirSync(path.join(base, 'outside'))
    fs.symlinkSync(path.join(base, 'outside'), path.join(work, '_actions/link'))
    process.env.RUNNER_WORKSPACE = path.join(work, 'repo')
    process.env.GITHUB_WORKSPACE = path.join(work, 'repo/repo')
  })

  afterEach(() => {
    process.env = { ...env }
    fs.rmSync(base, { recursive: true, force: true })
  })

  it('accepts an absolute path inside GITHUB_WORKSPACE', () => {
    expect(resolveUserVolumeSourcePath(path.join(work, 'repo/repo'))).toBe(
      path.join(work, 'repo/repo')
    )
    expect(resolveUserVolumeSourcePath(path.join(work, 'repo/repo/data'))).toBe(
      path.join(work, 'repo/repo/data')
    )
  })

  it('accepts a path relative to the work folder', () => {
    expect(resolveUserVolumeSourcePath('cache')).toBe(path.join(work, 'cache'))
  })

  it.each([
    () => path.join(base, 'outside'),
    () => path.join(work, 'repo/repo-evil'),
    () => path.join(work, 'repo/repo/../../../outside'),
    () => '../outside',
    () => '..',
    () => '_actions/link'
  ])('rejects a source outside the work folder (%#)', source => {
    expect(() => resolveUserVolumeSourcePath(source())).toThrow(
      'Volume mounts outside of the work folder are not supported'
    )
  })
})
