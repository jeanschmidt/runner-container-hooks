// Real-cluster test for the python3 apt-install fallback added in this PR.
// Runs prepareJob + runScriptStep against `ubuntu:24.04`, which ships
// without python3 in its base layer. Without the fallback, deployRpcServer
// would throw "image not compatible: python3 is a required dependency"
// before any step could execute. With the fallback, the hook runs
// `apt-get install -y --no-install-recommends python3` and the script step
// executes successfully.
//
// This test must run against a real kind cluster — see the k8s-tests job
// in .github/workflows/build.yaml.

import * as fs from 'fs'
import { cleanupJob, prepareJob, runScriptStep } from '../src/hooks'
import { TestHelper } from './test-setup'
import { PrepareJobArgs, RunScriptStepArgs } from 'hooklib'

jest.useRealTimers()

let testHelper: TestHelper
let prepareJobOutputData: any
let runScriptStepDefinition: { args: RunScriptStepArgs }

describe('apt-install python3 fallback on ubuntu:24.04', () => {
  beforeEach(async () => {
    testHelper = new TestHelper()
    await testHelper.initialize()

    const prepareJobOutputFilePath = testHelper.createFile(
      'prepare-job-output.json'
    )
    const prepareJobData = testHelper.getPrepareJobDefinition()

    // Override the workflow container image to a minimal base image that does
    // not have python3 preinstalled. ubuntu:24.04 specifically dropped
    // python3-minimal from its base layer.
    prepareJobData.args.container.image = 'ubuntu:24.04'
    // Drop any service images so the test focuses on the job container.
    prepareJobData.args.services = []

    runScriptStepDefinition = testHelper.getRunScriptStepDefinition() as {
      args: RunScriptStepArgs
    }

    await prepareJob(
      prepareJobData.args as PrepareJobArgs,
      prepareJobOutputFilePath
    )
    const outputContent = fs.readFileSync(prepareJobOutputFilePath)
    prepareJobOutputData = JSON.parse(outputContent.toString())
  }, 180000) // generous timeout: apt-get update + install adds 10–30s

  afterEach(async () => {
    await cleanupJob()
    await testHelper.cleanup()
  })

  it('should run a simple script step inside ubuntu:24.04 (python3 installed via apt fallback)', async () => {
    // Replace the script step with a trivial command — the goal of this test
    // is not to validate script execution, but to confirm the RPC server
    // deployment succeeded against an image without python3.
    runScriptStepDefinition.args.entryPoint = 'sh'
    runScriptStepDefinition.args.entryPointArgs = ['-c', "'echo hello'"]

    const exitCode = await runScriptStep(
      runScriptStepDefinition.args,
      prepareJobOutputData.state
    )
    expect(exitCode).toBe(0)
  }, 180000)
})
