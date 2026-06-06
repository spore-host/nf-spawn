package io.nextflow.spawn

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus

import java.nio.file.Files
import java.nio.file.Path

@Slf4j
@CompileStatic
class SpawnTaskHandler extends TaskHandler {

    private final SpawnExecutor executor
    private String instanceName
    private Process launchProcess

    SpawnTaskHandler(TaskRun task, SpawnExecutor executor) {
        super(task)
        this.executor = executor
    }

    @Override
    void submit() {
        // Derive a short, valid instance name from the task hash
        instanceName = "nf-${task.hash.toString().take(12)}"

        // Get configuration from task ext properties. TaskConfig.ext is an
        // untyped map, so under @CompileStatic we access it via a Map cast +
        // subscript rather than dotted property access (which fails static
        // type checking — see #3).
        Map ext = (task.config.ext ?: [:]) as Map
        String instanceType = (ext.instanceType ?: 't3.medium') as String
        String region       = (ext.region       ?: 'us-east-1') as String
        String ttl          = (ext.ttl          ?: '2h') as String
        boolean spot        = ext.spot ? true : false
        String ami          = (ext.ami ?: '') as String
        int volumeSize      = (ext.volumeSize ?: 0) as int

        log.info "Submitting task '${task.name}' to spawn instance '${instanceName}' (${instanceType} in ${region})"

        // task.workDir is the directory Nextflow reads back after the task to
        // bind outputs (.exitcode + declared output files). For the spawn
        // executor it is an S3 URI (e.g. s3://bucket/work/ab/cdef...); the
        // staging script syncs it to the instance, runs the task there, and
        // syncs results back so Nextflow can finalize the task (#14).
        String workDirUri = task.workDirStr

        // Write the staging script to a temp file shipped as instance user-data.
        Path scriptFile = Files.createTempFile("nf-spawn-${instanceName}-", ".sh")
        scriptFile.toFile().text = buildStagingScript(workDirUri, region, task.script)
        scriptFile.toFile().setExecutable(true)

        // Build the spawn launch command
        List<String> cmd = buildLaunchCommand(instanceName, instanceType, region, ttl, spot, scriptFile.toString(), ami, volumeSize)

        log.debug "spawn launch command: ${cmd.join(' ')}"

        ProcessBuilder pb = new ProcessBuilder(cmd)
        pb.redirectErrorStream(true)
        pb.environment().putAll(System.getenv())

        launchProcess = pb.start()
        String output = launchProcess.inputStream.text
        int exitCode  = launchProcess.waitFor()

        if (exitCode != 0) {
            throw new AbortOperationException("spawn launch failed (exit $exitCode) for task '${task.name}':\n${output}")
        }

        log.debug "spawn launch output for '${instanceName}':\n${output}"
        // Leave status SUBMITTED. Nextflow's TaskPollingMonitor drives the
        // SUBMITTED→RUNNING transition via checkIfRunning(); pre-setting RUNNING
        // here bypasses notifyTaskStart(), so the task is never counted as
        // running and the monitor's barrier exits after dumpInterval with
        // runningCount=0 (#31).
        status = TaskStatus.SUBMITTED
    }

    // checkIfRunning answers "has the task started running?" — the first time it
    // returns true, the monitor records the start (notifyTaskStart) and counts
    // the task RUNNING. It must NOT drive completion (that's checkIfCompleted);
    // driving completion here is what skipped the running accounting (#31).
    @Override
    boolean checkIfRunning() {
        if (status != TaskStatus.SUBMITTED) {
            return status == TaskStatus.RUNNING
        }
        // --check-complete exits 2 (running), 0/1 (done), 3 (not yet reachable).
        // Any of 0/1/2 means the instance is up and the task is underway →
        // transition to RUNNING. 3 means spored/SSH isn't ready yet — stay SUBMITTED.
        int rc = spawnCheckComplete()
        if (rc == 3) {
            return false
        }
        status = TaskStatus.RUNNING
        return true
    }

    // checkIfCompleted is polled while the task is RUNNING, for the full task
    // duration. It returns true only when the workload has actually finished.
    @Override
    boolean checkIfCompleted() {
        if (status != TaskStatus.RUNNING) {
            return status == TaskStatus.COMPLETED
        }
        int rc = spawnCheckComplete()
        switch (rc) {
            case 0:  // completed successfully
                log.info "Task '${task.name}' completed on instance '${instanceName}'"
                task.exitStatus = 0
                status = TaskStatus.COMPLETED
                return true
            case 1:  // failed or cancelled
                log.warn "Task '${task.name}' failed on instance '${instanceName}'"
                task.exitStatus = 1
                status = TaskStatus.COMPLETED
                return true
            default:  // 2 = still running, 3 = transient query error — keep polling
                return false
        }
    }

    // Nextflow 26.x: the abstract hook is protected killTask() — the public
    // kill() on the base now delegates to it (#9).
    @Override
    protected void killTask() {
        log.info "Terminating spawn instance '${instanceName}' for task '${task.name}'"
        try {
            new ProcessBuilder(['spawn', 'cancel', instanceName])
                .redirectErrorStream(true)
                .start()
                .waitFor()
        } catch (Exception e) {
            log.warn "Failed to cancel instance '${instanceName}': ${e.message}"
        }
    }

    // --- private helpers ---

    // buildLaunchCommand assembles the `spawn launch` argv.
    //
    // The task script is passed via --user-data-file (which reads the file's
    // contents) rather than --user-data, whose value is treated as INLINE
    // user-data unless it begins with '@'. Passing a bare path to --user-data
    // baked the path string itself into user-data, so the task script never
    // executed on the instance (#13).
    //
    // -y auto-approves the cost estimate. spawn only reads stdin for approval
    // on a TTY, so this is a no-op for the current pipe invocation, but it
    // guards against any future code path that would otherwise block waiting on
    // a confirmation we can never answer from a ProcessBuilder (#18).
    //
    // --ami is passed only when ext.ami is set. Otherwise spawn auto-detects the
    // AMI via ssm:GetParameter, which requires the instance/caller role to hold
    // that SSM permission (spawn#38); an explicit AMI avoids that dependency.
    //
    // --volume-size is passed only when ext.volumeSize > 0. spawn#25 already
    // floors the root volume at the AMI snapshot minimum automatically, so this
    // is NOT needed just to fit a large baked AMI — it's for requesting EXTRA
    // working space beyond that minimum. spawn keeps the larger of the two.
    @groovy.transform.PackageScope
    static List<String> buildLaunchCommand(String instanceName, String instanceType,
                                           String region, String ttl, boolean spot,
                                           String scriptPath, String ami, int volumeSize) {
        List<String> cmd = [
            'spawn', 'launch', instanceName,
            '--instance-type', instanceType,
            '--region', region,
            '--ttl', ttl,
            '--on-complete', 'terminate',
            '--user-data-file', scriptPath,
            '--wait-for-running=false',
            '--wait-for-ssh=false',
            '-y',
        ]
        if (ami) {
            cmd.addAll(['--ami', ami])
        }
        if (volumeSize > 0) {
            cmd.addAll(['--volume-size', volumeSize.toString()])
        }
        if (spot) {
            cmd << '--spot'
        }
        return cmd
    }

    // buildStagingScript produces the instance user-data script that returns
    // task results to Nextflow (#14). The flow is:
    //   1. sync the S3 work dir down to a local dir (inputs Nextflow staged),
    //   2. write .command.sh from the task script and run it, capturing
    //      .command.out / .command.err and the REAL exit code in .exitcode,
    //   3. sync the local dir back up to the S3 work dir — outputs first, then
    //      .exitcode LAST, so Nextflow never observes a completed exitcode
    //      before the output files have landed.
    // Nextflow reads .exitcode + declared outputs from the (S3) work dir to
    // finalize the task; without this the task's outputs never bind and
    // downstream tasks stay PENDING forever.
    //
    // Uses `aws s3 sync` (AWS CLI is present on AL2023 instances; the instance
    // role carries S3 access). scp-before-terminate was rejected: it is lossy
    // under --on-complete terminate, whereas S3 sync is idempotent and
    // object-atomic.
    @groovy.transform.PackageScope
    static String buildStagingScript(String workDirUri, String region, String taskScript) {
        StringBuilder sb = new StringBuilder('#!/bin/bash\n')
        sb << 'set -uo pipefail\n\n'
        sb << "WORKDIR_S3=${shellQuote(workDirUri)}\n"
        sb << "AWS_REGION=${shellQuote(region)}\n"
        // Stage on the EBS root volume, NOT /tmp. On AL2023 /tmp is a tmpfs
        // (RAM-backed, ~1-2 GB), so large inputs (e.g. multi-GB SRA files) fail
        // with "No space left on device" long before the 80 GB root fills (#27).
        sb << 'LOCAL_DIR=/var/lib/nf-work\n\n'
        sb << 'sudo mkdir -p "${LOCAL_DIR}" && sudo chown "$(id -u):$(id -g)" "${LOCAL_DIR}"\n'

        // 1. Stage inputs down from the S3 work dir.
        sb << 'aws s3 sync "${WORKDIR_S3}" "${LOCAL_DIR}/" --region "${AWS_REGION}" --quiet\n'
        sb << 'cd "${LOCAL_DIR}"\n\n'

        // 2. Materialize and run the task script; capture streams + real exit code.
        sb << "cat > .command.sh <<'NF_SPAWN_TASK_EOF'\n"
        sb << taskScript
        sb << '\nNF_SPAWN_TASK_EOF\n'
        sb << 'chmod +x .command.sh\n'
        sb << 'bash .command.sh 1>.command.out 2>.command.err\n'
        sb << 'TASK_RC=$?\n'
        sb << 'echo "${TASK_RC}" > .exitcode\n\n'

        // 3. Sync outputs back FIRST (exclude .exitcode), then upload .exitcode
        //    alone so its appearance always trails the outputs.
        sb << 'aws s3 sync "${LOCAL_DIR}/" "${WORKDIR_S3}" --region "${AWS_REGION}" --exclude ".exitcode" --quiet\n'
        sb << 'aws s3 cp .exitcode "${WORKDIR_S3%/}/.exitcode" --region "${AWS_REGION}" --quiet\n\n'

        // Completion signal for `spawn status --check-complete`, as the genuine
        // LAST step and reflecting the REAL task outcome (#24). Earlier this ran
        // `spored complete --status success` unconditionally, so a completion
        // was signaled even if the task failed or hadn't meaningfully run —
        // tasks looked complete (and successful) right after boot. Now the
        // status is success/failed per ${TASK_RC}, so --check-complete returns
        // 0 only on a genuinely successful task and 1 on failure.
        sb << 'if [ "${TASK_RC}" -eq 0 ]; then COMPLETE_STATUS=success; else COMPLETE_STATUS=failed; fi\n'
        sb << 'spored complete --status "${COMPLETE_STATUS}" 2>/dev/null || touch /tmp/SPAWN_COMPLETE\n'

        return sb.toString()
    }

    // shellQuote single-quotes a value for safe interpolation into the script,
    // escaping embedded single quotes via the '\'' idiom.
    @groovy.transform.PackageScope
    static String shellQuote(String value) {
        return "'" + (value ?: '').replace("'", "'\\''") + "'"
    }

    private int spawnCheckComplete() {
        try {
            Process p = new ProcessBuilder(['spawn', 'status', instanceName, '--check-complete'])
                .redirectErrorStream(true)
                .start()
            p.waitFor()
            return p.exitValue()
        } catch (Exception e) {
            log.debug "Error running spawn status for '${instanceName}': ${e.message}"
            return 3  // error
        }
    }
}
