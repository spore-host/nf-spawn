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

        log.info "Submitting task '${task.name}' to spawn instance '${instanceName}' (${instanceType} in ${region})"

        // Write the task script to a temp file
        Path scriptFile = Files.createTempFile("nf-spawn-${instanceName}-", ".sh")
        scriptFile.toFile().text = buildTaskScript()
        scriptFile.toFile().setExecutable(true)

        // Build the spawn launch command
        List<String> cmd = [
            'spawn', 'launch', instanceName,
            '--instance-type', instanceType,
            '--region', region,
            '--ttl', ttl,
            '--on-complete', 'terminate',
            '--user-data', scriptFile.toString(),
            '--wait-for-running=false',
            '--wait-for-ssh=false',
        ]
        if (spot) {
            cmd << '--spot'
        }

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
        status = TaskStatus.RUNNING
    }

    @Override
    boolean checkIfRunning() {
        if (status != TaskStatus.RUNNING) return false

        int rc = spawnCheckComplete()
        switch (rc) {
            case 2:  // still running
                return true
            case 0:  // completed successfully
                log.info "Task '${task.name}' completed on instance '${instanceName}'"
                task.exitStatus = 0
                status = TaskStatus.COMPLETED
                return false
            case 1:  // failed or cancelled
                log.warn "Task '${task.name}' failed on instance '${instanceName}'"
                task.exitStatus = 1
                status = TaskStatus.COMPLETED
                return false
            default:  // error querying status — keep polling
                log.debug "spawn status query error (exit $rc) for '${instanceName}', will retry"
                return true
        }
    }

    @Override
    boolean checkIfCompleted() {
        return status == TaskStatus.COMPLETED
    }

    @Override
    void kill() {
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

    private String buildTaskScript() {
        // Build a shell script that runs the Nextflow task script and signals completion
        StringBuilder sb = new StringBuilder('#!/bin/bash\nset -euo pipefail\n\n')

        // Stage inputs from S3 work directory if configured
        String workDir = task.workDir?.toString() ?: '/tmp/nf-work'
        sb << "mkdir -p '${workDir}'\n"
        sb << "cd '${workDir}'\n\n"

        // Write and execute the task script
        sb << 'cat > .command.sh << \'NFTASKEOF\'\n'
        sb << task.script
        sb << '\nNFTASKEOF\n'
        sb << 'chmod +x .command.sh\n\n'
        sb << 'bash .command.sh\n\n'

        // Signal completion to spored
        sb << 'spored complete --status success 2>/dev/null || touch /tmp/SPAWN_COMPLETE\n'

        return sb.toString()
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
