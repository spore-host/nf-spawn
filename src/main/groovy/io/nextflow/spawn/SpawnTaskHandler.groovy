package io.nextflow.spawn

import groovy.json.JsonOutput
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
    // The spawn task id (also the instance name + completion-record key). Captured
    // at submit() so completion can be polled via `spawn task status` even after
    // the instance has terminated.
    private String taskId
    private Process launchProcess
    // Captured at submit() so completion can also be detected from the durable S3
    // work dir (.exitcode) even after the instance has terminated (#34).
    private String workDirUri
    private String region

    SpawnTaskHandler(TaskRun task, SpawnExecutor executor) {
        super(task)
        this.executor = executor
    }

    @Override
    void submit() {
        // Derive a short, valid task id (= instance name + completion-record key)
        // from the task hash.
        taskId = "nf-${task.hash.toString().take(12)}"

        // Get configuration from task ext properties. TaskConfig.ext is an
        // untyped map, so under @CompileStatic we access it via a Map cast +
        // subscript rather than dotted property access (which fails static
        // type checking — see #3).
        Map ext = (task.config.ext ?: [:]) as Map
        String instanceType = (ext.instanceType ?: 't3.medium') as String
        this.region         = (ext.region       ?: 'us-east-1') as String
        String region       = this.region
        String ttl          = (ext.ttl          ?: '2h') as String
        boolean spot        = ext.spot ? true : false
        String ami          = (ext.ami ?: '') as String
        int volumeSize      = (ext.volumeSize ?: 0) as int
        // Pin the task to a specific AZ (#62). Needed for Fast Snapshot Restore:
        // FSR is per-AZ, so a volume created from an FSR-warmed snapshot is only
        // fast-restored in the AZ where FSR is enabled — elsewhere it lazy-loads
        // blocks from S3 (~6-8 MB/s). Empty → spawn picks placement as before.
        String az           = (ext.az ?: '') as String

        // Validate the constrained-format directives before they flow into the
        // `spawn launch` argv and the user-data script (#59). These have known,
        // narrow shapes (instance-type token, region/AZ slug, duration, ami-id),
        // so a malformed value is a config error — reject it up front with a
        // clear message rather than passing garbage to the CLI or the instance.
        // NOTE: ext.setup / ext.packages / ext.container / ext.runOptions are
        // deliberately NOT validated — they are arbitrary, config-author-supplied
        // and run AS ROOT on the instance (see validateExtDirectives docs). That
        // is trusted input, the same trust model as `ext.args`/`script` on any
        // Nextflow executor; nf-spawn does not sandbox it.
        validateExtDirectives(instanceType, region, az, ttl, ami)

        // Attach pre-populated EBS data volumes from snapshots, mounted read-only
        // by default — so a large reference DB (e.g. Kraken2) lives in a
        // re-snapshottable volume on a stock AMI instead of being baked into a
        // custom AMI (#45 → spawn#144). Each becomes a `spawn launch --attach-volume`.
        List<String> attachVolumes = parseVolumeSpecs(ext.volumes)

        // Mount a SHARED reference filesystem — FSx for Lustre or EFS — into the
        // task (#67). For a stable reference DB read by a WIDE fan-out (e.g.
        // taxprofiler's Kraken2/MetaPhlAn DBs over 30-100 samples), a shared FS is
        // the right primitive: one copy, N concurrent read-only mounts, no
        // per-volume Fast-Snapshot-Restore credit cliff that the ext.volumes path
        // hits at scale. Pre-create the FS once (`spawn fsx create` / out of band)
        // and reference it by id here; each task mounts the SAME filesystem. Empty
        // → no shared FS (today's behavior). Each becomes `spawn launch
        // --fsx-id`/`--efs-id`.
        Map fsx = parseSharedFs(ext.fsx, '/fsx')
        Map efs = parseSharedFs(ext.efs, '/efs')

        log.info "Submitting task '${task.name}' via `spawn task run` '${taskId}' (${instanceType} in ${region})"

        // task.workDir is the directory Nextflow reads back after the task to
        // bind outputs (.exitcode + declared output files). For the spawn
        // executor it is an S3 URI (e.g. s3://bucket/work/ab/cdef...); the
        // staging script syncs it to the instance, runs the task there, and
        // syncs results back so Nextflow can finalize the task (#14).
        this.workDirUri = task.workDirStr
        String workDirUri = this.workDirUri

        // The spawn executor runs each task on a separate instance, so the work
        // dir MUST be a shared S3 location both sides can reach. A missing or
        // non-S3 workDir (e.g. the default local ./work) can't be synced — fail
        // fast with a clear message instead of launching an instance that hangs
        // the task in RUNNING while the .exitcode probe never resolves (#59).
        if (!workDirUri || !workDirUri.startsWith('s3://')) {
            throw new AbortOperationException(
                "nf-spawn requires an S3 work directory (set `workDir = 's3://<bucket>/<prefix>'` " +
                "in nextflow.config); got ${workDirUri ?: '(unset)'}")
        }

        // The container directive (if any): run the task inside it via Docker so
        // tools that live only in the image (the norm for nf-core modules) are
        // found, instead of on the bare OS (#30). Empty/null → run on the OS.
        String container = (task.container ?: '') as String

        // Resolve docker run options so the container honors `docker.runOptions`
        // (e.g. `--user root`) and the per-process `containerOptions` directive —
        // dropping them made the container run as the image's default user and
        // fail to write the work dir (#39).
        String runOptions = resolveDockerRunOptions(task)

        // Bind-mount each ext.volumes mount path INTO the task container, at the
        // same path. The volume is mounted on the host by `spawn launch
        // --attach-volume`, but the task runs in Docker and would otherwise only
        // see the work dir — so a reference DB at /opt/databases/x is invisible to
        // the tool. Mounting it through (read-only when the volume is :ro) lets a
        // `path`-input DB symlinked into the work dir resolve to the real bytes
        // inside the container — zero copy (#49 / nf-spawn#51).
        String volumeMounts = volumeBindMounts(ext.volumes)
        if (volumeMounts) {
            runOptions = (runOptions ? runOptions + ' ' : '') + volumeMounts
        }
        // Same for the shared FSx/EFS mount (#67): the FS is mounted on the host
        // by spawn, but the task runs in Docker and would otherwise not see it.
        // Bind it through read-only at the same path so a `path`-input DB
        // symlinked to the mount resolves to the real bytes inside the container —
        // exactly as ext.volumes does.
        String sharedFsMounts = sharedFsBindMounts([fsx, efs])
        if (sharedFsMounts) {
            runOptions = (runOptions ? runOptions + ' ' : '') + sharedFsMounts
        }

        // Resolve the per-task setup/bootstrap that runs BEFORE the task, so a
        // STOCK AL2023 AMI works without baking Docker/tools into a custom AMI
        // (#47): auto-ensure Docker when a container is used, plus `ext.packages`
        // (dnf install) and an arbitrary `ext.setup` command.
        String setup = buildSetupScript(ext, container)

        // The mount paths a declared `path` input can be symlinked to instead of
        // copied (the #55 zero-copy short-circuit): both the ext.volumes mounts AND
        // the shared FSx/EFS mount (#67). Without feeding the shared-FS mount in
        // here, tasks would mount the FS but a staged db_path (e.g. taxprofiler
        // copies it into the S3 work area) would still localize from S3 instead of
        // using the mount. A stage-name basename matching a dir under any of these
        // mounts resolves zero-copy.
        List<String> mountPaths = volumeMountPaths(ext.volumes) + sharedFsMountPaths([fsx, efs])

        // The staging script IS the task command: it stages the S3 work dir down,
        // localizes declared inputs (#37), runs the (optionally containerized) task
        // capturing .exitcode, and syncs outputs back. Under `spawn task run` this
        // runs as the instance's login user (spawn wraps it in `su - <user>`); the
        // script sudo's its own privileged steps, exactly as it did under user-data.
        // spawn owns launch, sizing, the durable completion record, and
        // self-termination — nf-spawn no longer calls `spawn launch` or builds a
        // scoped policy; it hands spawn a TaskSpec (spawn#386 adapter migration).
        String stagingScript = buildStagingScript(workDirUri, region, task.script, container, task.getInputFilesMap(), runOptions, setup, mountPaths, false)

        Map spec = buildTaskSpec(taskId, stagingScript, instanceType, ttl, spot, workDirUri,
                                 ami, az, attachVolumes, fsx, efs)

        // Write the TaskSpec JSON to a temp file; delete it once spawn has read it
        // (in a finally) rather than leaving it in /tmp (#59).
        Path specFile = Files.createTempFile("nf-spawn-${taskId}-", ".json")
        specFile.toFile().deleteOnExit() // backstop if the finally is bypassed
        String output
        int exitCode
        try {
            specFile.toFile().text = JsonOutput.toJson(spec)

            // Launch DETACHED (no --wait): spawn sizes, launches, and the instance
            // writes its own completion record. Nextflow's TaskPollingMonitor drives
            // completion via checkIfCompleted() polling `spawn task status`, so a
            // blocking --wait would stall the monitor thread (#31).
            List<String> cmd = buildTaskRunCommand(specFile.toString(), region)
            log.debug "spawn task run command: ${cmd.join(' ')}"

            ProcessBuilder pb = new ProcessBuilder(cmd)
            pb.redirectErrorStream(true)
            launchProcess = pb.start()
            output = launchProcess.inputStream.text
            exitCode = launchProcess.waitFor()
        } finally {
            Files.deleteIfExists(specFile)
        }

        if (exitCode != 0) {
            throw new AbortOperationException("spawn task run failed (exit $exitCode) for task '${task.name}':\n${output}")
        }

        log.debug "spawn task run output for '${taskId}':\n${output}"
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
        // Fast-finishing tasks may produce the completion record before we ever
        // observe RUNNING. If the durable signal is already present, promote to
        // RUNNING so the next checkIfCompleted() tick finalizes it.
        if (readExitCodeFromS3() != null) {
            status = TaskStatus.RUNNING
            return true
        }
        // `spawn task status --check-complete` exits 0/1 (done), 2 (running), 3
        // (no record / error). 0/1/2 all mean the task exists and is underway →
        // transition to RUNNING. 3 means the completion record isn't there yet —
        // stay SUBMITTED (the instance is still sizing/booting).
        int rc = spawnCheckComplete()
        if (rc == 3) {
            return false
        }
        status = TaskStatus.RUNNING
        return true
    }

    // checkIfCompleted is polled while the task is RUNNING, for the full task
    // duration. Completion is detected by the presence of the durable
    // `.exitcode` object in the S3 work dir — NOT by SSH-ing the instance.
    //
    // Why not `spawn status --check-complete` (#34): that command SSHes into the
    // instance to ask spored. But the task launches with `--on-complete
    // terminate`, so the moment the task signals completion spored terminates
    // the box — and every subsequent check SSHes a dead instance, returning
    // "unreachable" forever, so the task never completes. The staging script
    // already uploads `.exitcode` LAST (after outputs), so its appearance in S3
    // is the authoritative, instance-independent "task finished" signal and
    // carries the real exit status.
    @Override
    boolean checkIfCompleted() {
        if (status != TaskStatus.RUNNING) {
            return status == TaskStatus.COMPLETED
        }
        Integer exit = readExitCodeFromS3()
        if (exit == null) {
            return false  // .exitcode not present yet — task still running
        }
        if (exit == 0) {
            log.info "Task '${task.name}' completed (exit 0) on instance '${taskId}'"
        } else {
            log.warn "Task '${task.name}' failed (exit ${exit}) on instance '${taskId}'"
        }
        task.exitStatus = exit
        status = TaskStatus.COMPLETED
        return true
    }

    // readExitCodeFromS3 fetches <workDir>/.exitcode and returns its integer
    // value, or null if it isn't present yet (task still running) or can't be
    // parsed. Streams the object to stdout via `aws s3 cp <uri> -`.
    private Integer readExitCodeFromS3() {
        if (!workDirUri) {
            return null
        }
        List<String> cmd = buildExitcodeProbeCommand(workDirUri, region)
        try {
            Process p = new ProcessBuilder(cmd).start()
            String out = p.inputStream.text
            int rc = p.waitFor()
            if (rc != 0) {
                // Non-zero almost always means the object doesn't exist yet
                // (NoSuchKey / 404) → task not finished. Keep polling.
                return null
            }
            return parseExitCode(out)
        } catch (Exception e) {
            log.debug "Error reading .exitcode for '${taskId}': ${e.message}"
            return null
        }
    }

    // Nextflow 26.x: the abstract hook is protected killTask() — the public
    // kill() on the base now delegates to it (#9).
    @Override
    protected void killTask() {
        log.info "Terminating spawn instance '${taskId}' (${region}) for task '${task.name}'"

        // Use `spawn terminate`, not `spawn cancel`: cancel operates on parameter
        // sweeps, so it never destroyed the per-task instance — the box billed
        // until its TTL. terminate takes the region from AWS_REGION (it has no
        // --region flag), so we scope the subprocess env to this.region; without
        // it terminate can target the wrong region and not find the instance
        // (the `nf-<hash>` name is not globally unique). -y skips the
        // irreversible-termination confirmation we can't answer over a pipe.
        // A failed terminate leaks a billable instance, so treat a non-zero exit
        // as an error, not a warning (#58).
        List<String> cmd = buildTerminateCommand(taskId)
        try {
            ProcessBuilder pb = new ProcessBuilder(cmd)
            pb.redirectErrorStream(true)
            pb.environment().putAll(System.getenv())
            pb.environment().put('AWS_REGION', region)
            pb.environment().put('AWS_DEFAULT_REGION', region)

            Process p = pb.start()
            String out = p.inputStream.text
            int rc = p.waitFor()
            if (rc != 0) {
                log.error "spawn terminate failed (exit ${rc}) for instance '${taskId}' in ${region} — " +
                          "the instance may still be running and billing until its TTL. Output:\n${out}"
            } else {
                log.info "Terminated spawn instance '${taskId}' (${region})"
            }
        } catch (Exception e) {
            log.error "Failed to terminate instance '${taskId}' in ${region}: ${e.message} — " +
                      "the instance may still be running and billing until its TTL."
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
    //
    // --attach-volume (repeatable) attaches a pre-populated EBS volume from a
    // snapshot; each value is `snap-xxx:/mount[:ro|:rw]` (spawn#144). Used to
    // mount large reference data on a stock AMI (#45).
    //
    // --az is passed only when ext.az is set, pinning the instance to that
    // availability zone (#62). Otherwise spawn chooses placement, preserving the
    // prior default. This matters for Fast Snapshot Restore, which is per-AZ.
    //
    // --fsx-id / --efs-id (+ their --*-mount-point) are passed only when ext.fsx /
    // ext.efs are set (#67), mounting a SHARED reference filesystem the user
    // pre-created. Empty maps → neither flag, preserving the prior default. Only
    // the existing-filesystem (id) form is supported here: nf-spawn launches one
    // instance per task, so --fsx-create would create one filesystem PER task — a
    // fan-out footgun — whereas a single pre-built FS is mounted by every task.
    // validateExtDirectives fails fast on malformed values for the constrained
    // ext.* directives (#59). These have known, narrow shapes and flow into the
    // `spawn launch` argv and the user-data script, so a bad value is a config
    // error worth catching before an instance is launched.
    //
    // Only fixed-format fields are checked. It deliberately does NOT touch
    // ext.setup / ext.command / ext.packages / ext.container / ext.runOptions:
    // those are arbitrary user-supplied strings that run AS ROOT on the instance
    // (ext.setup is spliced into the user-data bootstrap; container/runOptions
    // into the `docker run` line). That is trusted input by design — the same
    // trust model as `ext.args` and the task `script` on any Nextflow executor,
    // where the pipeline/config author already controls what executes. nf-spawn
    // does not (and cannot meaningfully) sandbox it; validation here would give a
    // false sense of security. Document it as a root-RCE surface instead.
    @groovy.transform.PackageScope
    static void validateExtDirectives(String instanceType, String region, String az, String ttl, String ami) {
        // EC2 instance type: family+size like "c7i.2xlarge", "t3.medium", "p5.48xlarge".
        if (instanceType && !(instanceType ==~ /[a-z][a-z0-9\-]*\.[a-z0-9]+/)) {
            throw new AbortOperationException("ext.instanceType ${asLiteral(instanceType)} is not a valid EC2 instance type (e.g. c7i.2xlarge)")
        }
        // AWS region slug like "us-east-1", "ap-southeast-2".
        if (region && !(region ==~ /[a-z]{2}-[a-z]+-\d/)) {
            throw new AbortOperationException("ext.region ${asLiteral(region)} is not a valid AWS region (e.g. us-east-1)")
        }
        // AZ is a region slug plus a trailing letter, e.g. "us-east-1a".
        if (az && !(az ==~ /[a-z]{2}-[a-z]+-\d[a-z]/)) {
            throw new AbortOperationException("ext.az ${asLiteral(az)} is not a valid availability zone (e.g. us-east-1a)")
        }
        // TTL: a Go-style duration of digits+unit, e.g. "2h", "90m", "36h", "7d".
        if (ttl && !(ttl ==~ /\d+[smhdw]/)) {
            throw new AbortOperationException("ext.ttl ${asLiteral(ttl)} is not a valid duration (e.g. 2h, 90m, 7d)")
        }
        // AMI id: "ami-" + hex.
        if (ami && !(ami ==~ /ami-[0-9a-f]+/)) {
            throw new AbortOperationException("ext.ami ${asLiteral(ami)} is not a valid AMI id (e.g. ami-0abc123...)")
        }
    }

    // asLiteral quotes a value for an error message, making an injected/whitespace
    // value visible rather than blending into the sentence.
    private static String asLiteral(String s) { '"' + s + '"' }

    // buildTaskSpec maps a task to the spawn TaskSpec contract (spawn#386). The
    // staging script becomes the command (wrapped in `bash -lc` so spawn runs it
    // verbatim, host or container-agnostic — nf-spawn does its OWN docker run
    // inside the script, so spec.container is deliberately NOT set). ext.* launch
    // directives map onto the placement block (spawn ≥ 0.84.0); ext.instanceType
    // is an EXACT type, so it pins resources.instance_type (spawn#413) — a
    // family-only hint would size to the cheapest in-family (t3.medium→t3.nano).
    // The workDir bucket is declared in resources.s3_read_write so the scoped
    // instance profile grants the ListBucket + object access the staging script's
    // `aws s3 sync`/`cp` need (nf-spawn does the I/O, not spawn's wrapper — so
    // there are no inputs/outputs manifests).
    @groovy.transform.PackageScope
    static Map buildTaskSpec(String taskId, String stagingScript, String instanceType,
                             String ttl, boolean spot, String workDirUri,
                             String ami, String az, List<String> attachVolumes,
                             Map fsx, Map efs) {
        Map<String, Object> resources = [:]
        if (instanceType) {
            resources.instance_type = instanceType
        }
        if (spot) {
            resources.purchase = 'spot'
            resources.fallback = 'on_demand'
        }
        String bucket = s3BucketUri(workDirUri)
        if (bucket) {
            resources.s3_read_write = [bucket]
        }

        Map<String, Object> placement = [:]
        if (ami) placement.ami = ami
        if (az)  placement.availability_zone = az
        List<Map> volumes = []
        for (String v : (attachVolumes ?: [])) {
            // v is "snap-xxx:/mount[:ro|:rw]" (parseVolumeSpecs output).
            Map vr = parseAttachVolumeSpec(v)
            if (vr) volumes << vr
        }
        if (volumes) placement.volumes = volumes
        if (fsx?.id) placement.fsx_lustre_id = fsx.id as String
        if (efs?.id) placement.efs_id = efs.id as String

        Map<String, Object> spec = [
            task_id  : taskId,
            command  : ['/bin/bash', '-lc', stagingScript],
            resources: resources,
            lifecycle: [ttl: ttl, on_complete: 'terminate'],
        ]
        if (placement) spec.placement = placement
        return spec
    }

    // s3BucketUri returns the bucket-root URI (s3://bucket) of an s3:// URI, or ''
    // — declared in resources.s3_read_write so spawn scopes the grant to the whole
    // work bucket.
    @groovy.transform.PackageScope
    static String s3BucketUri(String uri) {
        if (!uri?.startsWith('s3://')) return ''
        String rest = uri.substring('s3://'.length())
        int slash = rest.indexOf('/')
        String bucket = slash >= 0 ? rest.substring(0, slash) : rest
        return bucket ? "s3://${bucket}".toString() : ''
    }

    // parseAttachVolumeSpec parses a "snap-xxx:/mount[:ro|:rw]" attach-volume value
    // into a placement.volumes entry {snapshot, mount_path, read_only}.
    @groovy.transform.PackageScope
    static Map parseAttachVolumeSpec(String v) {
        if (!v) return null
        List<String> parts = v.split(':') as List
        if (parts.size() < 2) return null
        String snap = parts[0]
        String mount = parts[1]
        boolean ro = parts.size() >= 3 && parts[2] == 'ro'
        return [snapshot: snap, mount_path: mount, read_only: ro]
    }

    // buildTaskRunCommand assembles the `spawn task run` argv (detached — no
    // --wait; completion is polled via `spawn task status`).
    @groovy.transform.PackageScope
    static List<String> buildTaskRunCommand(String specPath, String region) {
        return ['spawn', 'task', 'run', '--spec', specPath, '--region', region]
    }

    @groovy.transform.PackageScope
    static List<String> buildLaunchCommand(String instanceName, String instanceType,
                                           String region, String ttl, boolean spot,
                                           String scriptPath, String ami, int volumeSize,
                                           List<String> attachVolumes = [], String az = '',
                                           Map fsx = [:], Map efs = [:]) {
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
        for (String v : (attachVolumes ?: [])) {
            cmd.addAll(['--attach-volume', v])
        }
        if (az) {
            cmd.addAll(['--az', az])
        }
        if (fsx?.id) {
            cmd.addAll(['--fsx-id', fsx.id as String])
            if (fsx.mount) {
                cmd.addAll(['--fsx-mount-point', fsx.mount as String])
            }
        }
        if (efs?.id) {
            cmd.addAll(['--efs-id', efs.id as String])
            if (efs.mount) {
                cmd.addAll(['--efs-mount-point', efs.mount as String])
            }
        }
        if (spot) {
            cmd << '--spot'
        }
        return cmd
    }

    // parseSharedFs normalizes the `ext.fsx` / `ext.efs` directive into a map with
    // `id`, `mount`, and `paths` keys (#67). It accepts:
    //   - a bare filesystem id string:  ext.fsx = 'fs-0abc'         → mount defaults
    //   - a map: [ id: 'fs-0abc', mount: '/fsx', paths: ['kraken2'] ]
    // `mount` defaults to defaultMount (/fsx or /efs). `paths` is an optional list
    // of subdirectory names under the mount that a declared `path` input may be
    // symlinked to (the #55 zero-copy short-circuit) — e.g. paths:['kraken2'] makes
    // a `kraken2` stage-name input resolve to <mount>/kraken2. Empty/null → [:] (no
    // shared FS). Only the existing-filesystem (id) form is supported; a `create`
    // key is rejected with a pointer to the follow-up, since per-task creation
    // would multiply filesystems across a fan-out.
    @groovy.transform.PackageScope
    static Map parseSharedFs(Object spec, String defaultMount) {
        if (!spec) return [:]
        if (spec instanceof CharSequence) {
            String id = spec.toString().trim()
            return id ? [id: id, mount: defaultMount, paths: []] : [:]
        }
        if (!(spec instanceof Map)) {
            throw new AbortOperationException("ext.fsx/ext.efs must be a filesystem-id string or a map with 'id', got: ${spec.getClass().name}")
        }
        Map m = spec as Map
        if (m.create) {
            throw new AbortOperationException(
                "ext.fsx create form is not supported: nf-spawn launches one instance per task, " +
                "so --fsx-create would create one filesystem per task across a fan-out. Pre-create a " +
                "shared filesystem (spawn fsx create) and reference it by id: ext.fsx = 'fs-xxx'. " +
                "(per-run ephemeral shared FS is tracked as a follow-up.)")
        }
        String id = (m.id ?: m.fsxId ?: m.efsId ?: '') as String
        if (!id) {
            throw new AbortOperationException("ext.fsx/ext.efs map needs an 'id' (fs-xxx): ${m}")
        }
        String mount = (m.mount ?: m.mountPoint ?: defaultMount) as String
        List<String> paths = []
        Object p = m.paths ?: m.path
        if (p instanceof List) {
            paths = (p as List).collect { (it as String).trim() }.findAll { it }
        } else if (p instanceof CharSequence) {
            String s = p.toString().trim()
            if (s) paths = [s]
        }
        return [id: id, mount: mount, paths: paths]
    }

    // sharedFsBindMounts turns parsed ext.fsx/ext.efs maps into `docker run -v`
    // read-only bind-mount flags so the host-mounted shared filesystem is visible
    // INSIDE the task container at the same path (#67) — mirroring volumeBindMounts
    // for ext.volumes. Reference data is read-only by nature here, so the mount is
    // always :ro. Returns '' when no shared FS is set.
    @groovy.transform.PackageScope
    static String sharedFsBindMounts(List<Map> fsList) {
        List<String> flags = []
        for (Map fs : (fsList ?: [])) {
            if (!fs?.id || !fs?.mount) continue
            String mount = fs.mount as String
            flags << "-v ${shellQuote(mount + ':' + mount + ':ro')}".toString()
        }
        return flags.join(' ')
    }

    // sharedFsMountPaths returns the symlink-eligible paths for the #55 zero-copy
    // short-circuit from parsed ext.fsx/ext.efs maps (#67). A shared FS mounts at a
    // single root (e.g. /fsx) but typically holds several DBs at /fsx/<name>, so a
    // bare /fsx basename ('fsx') rarely matches a stage name. We therefore expose
    // <mount>/<name> for each declared `paths` entry (so a 'kraken2' input resolves
    // to /fsx/kraken2), and fall back to the bare mount when none are declared (so
    // a DB mounted AT the root, or a single-DB FS, still matches).
    @groovy.transform.PackageScope
    static List<String> sharedFsMountPaths(List<Map> fsList) {
        List<String> out = []
        for (Map fs : (fsList ?: [])) {
            if (!fs?.id || !fs?.mount) continue
            String mount = (fs.mount as String).replaceAll('/+$', '')
            List paths = (fs.paths ?: []) as List
            if (paths) {
                for (Object name : paths) {
                    String n = (name as String).trim()
                    if (n) out << "${mount}/${n}".toString()
                }
            } else {
                out << mount
            }
        }
        return out
    }

    // VolumeSpec is one parsed `ext.volumes` entry: an EBS snapshot attached at a
    // mount path, read-only by default. It's the single typed representation the
    // three volume views (attach-volume args, docker -v flags, mount paths) all
    // derive from, so they can't diverge in parsing or validation (#59).
    @groovy.transform.Immutable
    @groovy.transform.PackageScope
    static class VolumeSpec {
        String snapshot
        String mount
        boolean readOnly
    }

    // parseVolumes is the ONE parser+validator for the `ext.volumes` directive
    // (#45 → spawn#144, consolidated in #59). The directive is a list of maps
    // (a single map is accepted too), each:
    //   [ snapshot: 'snap-0abc', mount: '/opt/databases/kraken2', readOnly: true ]
    // `readOnly` defaults to true (the common case for shared reference data).
    // Null/empty → []. A malformed entry throws (fail fast, once) — the three
    // public views below all go through here, so none can silently skip a bad
    // entry the way the old divergent parsers did.
    @groovy.transform.PackageScope
    static List<VolumeSpec> parseVolumes(Object volumes) {
        if (!volumes) return []
        List rawList
        if (volumes instanceof List) {
            rawList = volumes as List
        } else if (volumes instanceof Map) {
            rawList = [volumes]
        } else {
            throw new AbortOperationException("ext.volumes must be a list of maps (or a single map), got: ${volumes.getClass().name}")
        }
        List<VolumeSpec> specs = []
        for (Object o : rawList) {
            if (!(o instanceof Map)) {
                throw new AbortOperationException("each ext.volumes entry must be a map with 'snapshot' and 'mount', got: ${o}")
            }
            Map m = o as Map
            String snap  = (m.snapshot ?: m.snapshotId ?: '') as String
            String mount = (m.mount ?: m.mountPoint ?: '') as String
            if (!snap || !mount) {
                throw new AbortOperationException("ext.volumes entry needs both 'snapshot' and 'mount': ${m}")
            }
            // readOnly defaults to true; an explicit false → writable.
            boolean readOnly = m.containsKey('readOnly') ? (m.readOnly ? true : false) : true
            specs << new VolumeSpec(snap, mount, readOnly)
        }
        return specs
    }

    // parseVolumeSpecs → `spawn launch --attach-volume` values of the form
    // `snap-xxx:/mount[:ro|:rw]` (#45 → spawn#144).
    @groovy.transform.PackageScope
    static List<String> parseVolumeSpecs(Object volumes) {
        return parseVolumes(volumes).collect { VolumeSpec v ->
            "${v.snapshot}:${v.mount}:${v.readOnly ? 'ro' : 'rw'}".toString()
        }
    }

    // volumeBindMounts → `docker run -v` bind-mount flags so the host-attached
    // reference volume(s) are visible INSIDE the task container at the same path,
    // read-only when the volume is (#49 / nf-spawn#51). Returns '' when empty.
    @groovy.transform.PackageScope
    static String volumeBindMounts(Object volumes) {
        return parseVolumes(volumes).collect { VolumeSpec v ->
            "-v ${shellQuote(v.mount + ':' + v.mount + (v.readOnly ? ':ro' : ''))}".toString()
        }.join(' ')
    }

    // volumeMountPaths → just the mount paths (e.g. ["/opt/databases/kraken2"]),
    // used to match a declared input's stage name against an attached reference
    // volume for the zero-copy symlink short-circuit (#55).
    @groovy.transform.PackageScope
    static List<String> volumeMountPaths(Object volumes) {
        return parseVolumes(volumes).collect { VolumeSpec v -> v.mount }
    }

    // baseName returns the last path segment of p (trailing slashes ignored),
    // e.g. "/opt/databases/metaphlan" -> "metaphlan", "kraken2" -> "kraken2".
    @groovy.transform.PackageScope
    static String baseName(String p) {
        if (!p) return ''
        String s = p
        while (s.length() > 1 && s.endsWith('/')) {
            s = s.substring(0, s.length() - 1)
        }
        int i = s.lastIndexOf('/')
        return i >= 0 ? s.substring(i + 1) : s
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
    // buildSetupScript assembles the per-task bootstrap that runs before staging,
    // so a STOCK AL2023 AMI can run containerized nf-core tasks without baking
    // Docker/tools into a custom AMI (#47). It composes three layers, each
    // optional and skippable, in this order:
    //
    //   1. Auto-ensure Docker — when the process has a `container` directive
    //      (nf-spawn runs it via `docker run`), install + start Docker if it's
    //      not already present. Stock AL2023 has the AWS CLI but no Docker. This
    //      is idempotent (skips when `docker` is on PATH, so a pre-baked AMI pays
    //      nothing) and can be turned off with `ext.ensureDocker = false` for
    //      users who bake their own tools AMI and want zero per-task install.
    //   2. `ext.packages = ['pigz', ...]` → `dnf install -y` of host tools the
    //      task calls directly on the instance.
    //   3. `ext.setup = '<shell>'` → an arbitrary bootstrap command, run last.
    //
    // Returns '' when there's nothing to do (e.g. no container + no packages/setup).
    @groovy.transform.PackageScope
    static String buildSetupScript(Map ext, String container) {
        StringBuilder sb = new StringBuilder()

        // 1. Docker — only when there's a container to run, and not opted out.
        boolean ensureDocker = ext.containsKey('ensureDocker') ? (ext.ensureDocker ? true : false) : true
        if (container?.trim() && ensureDocker) {
            sb << '# nf-spawn: ensure Docker (stock AL2023 has none) — idempotent (#47)\n'
            sb << 'if ! command -v docker >/dev/null 2>&1; then\n'
            sb << '  echo "nf-spawn: installing Docker..." >&2\n'
            sb << '  sudo dnf install -y docker || { echo "nf-spawn: docker install failed" >&2; exit 1; }\n'
            sb << 'fi\n'
            sb << 'sudo systemctl enable --now docker 2>/dev/null || sudo systemctl start docker || { echo "nf-spawn: could not start docker" >&2; exit 1; }\n'
        }

        // 2. ext.packages → dnf install.
        List<String> packages = parsePackages(ext.packages)
        if (packages) {
            String quoted = packages.collect { shellQuote(it) }.join(' ')
            sb << "# nf-spawn: install requested packages (#47)\n"
            sb << "sudo dnf install -y ${quoted} || { echo \"nf-spawn: package install failed\" >&2; exit 1; }\n"
        }

        // 3. ext.setup → arbitrary bootstrap, last.
        String setup = (ext.setup ?: ext.command ?: '') as String
        if (setup?.trim()) {
            sb << "# nf-spawn: ext.setup (#47)\n"
            sb << setup.trim() << '\n'
        }

        return sb.toString()
    }

    // parsePackages normalizes `ext.packages` into a list of package names. Accepts
    // a List or a single String (space/comma-separated). Null/empty → [].
    @groovy.transform.PackageScope
    static List<String> parsePackages(Object packages) {
        if (!packages) return []
        if (packages instanceof List) {
            return (packages as List).collect { (it as String).trim() }.findAll { it }
        }
        return (packages as String).split(/[,\s]+/).collect { it.trim() }.findAll { it }
    }

    // signalCompletion: when true (the legacy `spawn launch` user-data path), the
    // script ends by signaling spored (`spored complete` / touch SPAWN_COMPLETE) so
    // on_complete fires. Under `spawn task run` (spawn#386) the WRAPPER owns
    // completion signaling — writing SPAWN_COMPLETE here mid-command would race the
    // wrapper's own SPAWN_COMPLETE (written after stage-out + completion.json) and
    // could terminate the box before the completion record uploads. So the task-run
    // path passes false and the script instead `exit`s with the task's real code,
    // which the wrapper records.
    @groovy.transform.PackageScope
    static String buildStagingScript(String workDirUri, String region, String taskScript, String container, Map<String, Path> inputs = [:], String runOptions = '', String setup = '', List<String> mountPaths = [], boolean signalCompletion = true) {
        StringBuilder sb = new StringBuilder('#!/bin/bash\n')
        sb << 'set -uo pipefail\n\n'
        sb << "WORKDIR_S3=${shellQuote(workDirUri)}\n"
        sb << "AWS_REGION=${shellQuote(region)}\n"
        // Stage on the EBS root volume, NOT /tmp. On AL2023 /tmp is a tmpfs
        // (RAM-backed, ~1-2 GB), so large inputs (e.g. multi-GB SRA files) fail
        // with "No space left on device" long before the 80 GB root fills (#27).
        sb << 'LOCAL_DIR=/var/lib/nf-work\n\n'
        sb << 'sudo mkdir -p "${LOCAL_DIR}" && sudo chown "$(id -u):$(id -g)" "${LOCAL_DIR}"\n'
        // Only a CONTAINERIZED task needs the work dir world-writable (#59). The
        // staging script runs as root (EC2 user-data), so ${LOCAL_DIR} is
        // root-owned (well, chowned to the invoking uid above); a container image
        // whose default user is NON-root (the norm for biocontainers/nf-core)
        // then can't create .command.sh's symlinks or write outputs → "Permission
        // denied" (#39), so we relax it for that case. A bare-OS task runs as the
        // dir's own owner and needs no relaxation, so we DON'T weaken permissions
        // there. (The dir is on an ephemeral instance terminated after the task.)
        if (container?.trim()) {
            sb << 'chmod 0777 "${LOCAL_DIR}"\n'
        }

        // 0. Per-task setup, BEFORE staging/running — installs Docker + any host
        //    tools so a STOCK AL2023 AMI works without a custom tools-baked AMI
        //    (#47). Idempotent (skip-if-present); empty when nothing to do.
        if (setup) {
            sb << '\n' << setup
        }

        // 1a. Sync this task's own S3 work dir down (.command.sh metadata etc.).
        sb << 'aws s3 sync "${WORKDIR_S3}" "${LOCAL_DIR}/" --region "${AWS_REGION}" --quiet\n'
        sb << 'cd "${LOCAL_DIR}"\n\n'

        // 1b. Localize Nextflow's DECLARED inputs by their real source URI. A
        // task's `path` inputs usually live OUTSIDE its own work dir — they're
        // produced by an upstream process, or come from a samplesheet / channel
        // (often s3://) — so the work-dir sync above never pulls them, and stock
        // nf-core modules run with their inputs missing (#37). Copy each declared
        // input from its source to the local stage name it's referenced by.
        sb << buildInputStaging(inputs, mountPaths)

        // 2. Materialize and run the task script; capture streams + real exit code.
        //    The script is written flush-left (common leading indentation stripped)
        //    the way Nextflow itself writes .command.sh — otherwise a source-indented
        //    `<<-HEREDOC` terminator (e.g. nf-core's `versions.yml` block) stays
        //    indented with spaces, which `<<-` (strips tabs only) can't match, so the
        //    heredoc swallows its own terminator and the file is malformed (#43).
        sb << "cat > .command.sh <<'NF_SPAWN_TASK_EOF'\n"
        sb << dedentTaskScript(taskScript)
        sb << '\nNF_SPAWN_TASK_EOF\n'
        sb << 'chmod +x .command.sh\n'
        sb << buildRunLine(container, runOptions)
        sb << 'TASK_RC=$?\n\n'

        // 3. Sync outputs back FIRST (exclude .exitcode), then upload .exitcode
        //    alone so its appearance always trails the outputs. If the output
        //    up-sync fails, Nextflow would otherwise still see the task's own
        //    exit code (often 0) once .exitcode lands and finalize the task as
        //    successful with MISSING outputs — a silent partial success (#59).
        //    So if the sync fails and the task itself succeeded, record a
        //    non-zero exit code instead, surfacing it as a task failure.
        sb << 'if ! aws s3 sync "${LOCAL_DIR}/" "${WORKDIR_S3}" --region "${AWS_REGION}" --exclude ".exitcode" --quiet; then\n'
        sb << '  echo "nf-spawn: output sync to ${WORKDIR_S3} failed" >&2\n'
        sb << '  if [ "${TASK_RC}" -eq 0 ]; then TASK_RC=75; fi\n'
        sb << 'fi\n'
        sb << 'echo "${TASK_RC}" > .exitcode\n'
        sb << 'aws s3 cp .exitcode "${WORKDIR_S3%/}/.exitcode" --region "${AWS_REGION}" --quiet\n\n'

        // Completion signal for `spawn status --check-complete`, as the genuine
        // LAST step and reflecting the REAL task outcome (#24). Earlier this ran
        // `spored complete --status success` unconditionally, so a completion
        // was signaled even if the task failed or hadn't meaningfully run —
        // tasks looked complete (and successful) right after boot. Now the
        // status is success/failed per ${TASK_RC}, so --check-complete returns
        // 0 only on a genuinely successful task and 1 on failure.
        if (signalCompletion) {
            sb << 'if [ "${TASK_RC}" -eq 0 ]; then COMPLETE_STATUS=success; else COMPLETE_STATUS=failed; fi\n'
            sb << 'spored complete --status "${COMPLETE_STATUS}" 2>/dev/null || touch /tmp/SPAWN_COMPLETE\n'
        } else {
            // task-run path: the wrapper owns completion. Exit with the task's real
            // code so the wrapper's completion record carries the right status.
            sb << 'exit "${TASK_RC}"\n'
        }

        return sb.toString()
    }

    // buildInputStaging emits the commands that localize a task's declared input
    // files onto the instance, keyed by the stage name `.command.sh` references
    // them by (#37). Nextflow resolves each input to a source Path; for the spawn
    // executor (S3 work dir) those are typically `s3://…` URIs from an upstream
    // task, a samplesheet, or a channel. We copy each to ${LOCAL_DIR}/<stageName>
    // so the symlinks `.command.sh` creates resolve.
    //
    // - s3:// sources → `aws s3 cp` (--recursive when the source is a directory).
    // - Any other scheme/local path is left to the work-dir sync (1a); we don't
    //   second-guess it here.
    // Idempotent and best-effort per file; a stage name may include subdirs
    // (Nextflow allows `path`d inputs under a relative dir), so we mkdir -p first.
    @groovy.transform.PackageScope
    static String buildInputStaging(Map<String, Path> inputs, List<String> mountPaths = []) {
        if (!inputs) return ''
        // Index ext.volumes mounts by basename, for the #55 short-circuit below.
        Map<String, String> mountByBase = [:]
        for (String mp : (mountPaths ?: [])) {
            String b = baseName(mp)
            if (b) mountByBase[b] = mp
        }
        StringBuilder sb = new StringBuilder()
        sb << '# Localize declared inputs by source URI (#37 — they live outside this work dir).\n'
        inputs.each { String stageName, Path source ->
            // #55: if this input's stage name matches an attached ext.volumes mount,
            // symlink the stage name → the mount and SKIP the copy — even though
            // Nextflow reports the source as its own S3 stage copy of the DB.
            // Pipelines like taxprofiler STAGE db_path (copy it into the S3 work
            // area), so the source URI is never the mount; matching by basename is
            // what lets a volume-backed reference DB stay zero-copy. The mount is
            // bind-mounted into the container, so the symlink resolves inside it.
            final String mountForStage = mountByBase[baseName(stageName)]
            if (mountForStage) {
                // #59: this match is by BASENAME only, so an input whose stage-name
                // basename coincidentally equals a mount's basename would be
                // symlinked onto the reference volume and its real copy skipped —
                // silently serving the wrong data. Basename matching is intended
                // (taxprofiler-style pipelines stage db_path into S3, so the source
                // URI never equals the mount), but the substitution is surprising,
                // so LOG it (stderr, lands in .command.err / the launch log) so a
                // wrong match is diagnosable rather than silent.
                final destParent = stageName.contains('/') ? stageName.substring(0, stageName.lastIndexOf('/')) : ''
                if (destParent) {
                    sb << "mkdir -p \"\${LOCAL_DIR}/\"${shellQuote(destParent)}\n"
                }
                sb << "echo \"nf-spawn: input '${stageName}' → ext.volumes mount ${mountForStage} (basename match; symlinked, copy skipped)\" >&2\n"
                sb << "if [ -e ${shellQuote(mountForStage)} ]; then ln -sfn ${shellQuote(mountForStage)} \"\${LOCAL_DIR}/\"${shellQuote(stageName)}; else echo \"nf-spawn: ext.volumes mount ${mountForStage} not present for input ${stageName}\" >&2; exit 1; fi\n"
                return
            }
            final uri = normalizeS3Uri(source.toUri().toString())
            if (!uri.startsWith('s3://')) {
                // A LOCAL absolute path (file:// or a bare path). The common case
                // that matters here: an `ext.volumes` reference DB mounted
                // read-only at that path on the task (e.g. /opt/databases/kraken2).
                // A task `path` input is referenced by its STAGE NAME in the work
                // dir, not the absolute path (e.g. nf-core's metaphlan does
                // `find -L metaphlan_db ...`), so we must make the stage name
                // resolve. Symlink stage name → the mounted path (the spawn
                // equivalent of Nextflow's stageInMode=symlink on a shared FS),
                // so the tool reads the DB straight off the read-only volume —
                // zero copy, zero per-task download (#49 / nf-spawn#51).
                //
                // Guarded by an existence test: if the path isn't present on the
                // task (no matching volume), we skip rather than make a dangling
                // link — the work-dir sync still handles a genuinely-local file.
                final localPath = localAbsolutePath(uri)
                if (localPath) {
                    final destParent = stageName.contains('/') ? stageName.substring(0, stageName.lastIndexOf('/')) : ''
                    if (destParent) {
                        sb << "mkdir -p \"\${LOCAL_DIR}/\"${shellQuote(destParent)}\n"
                    }
                    sb << "if [ -e ${shellQuote(localPath)} ]; then ln -sfn ${shellQuote(localPath)} \"\${LOCAL_DIR}/\"${shellQuote(stageName)}; fi\n"
                }
                return
            }
            final destParent = stageName.contains('/') ? stageName.substring(0, stageName.lastIndexOf('/')) : ''
            if (destParent) {
                // ${LOCAL_DIR} must stay OUTSIDE the single quotes so the shell
                // expands it; only the (untrusted) stage name is quoted (#41).
                sb << "mkdir -p \"\${LOCAL_DIR}/\"${shellQuote(destParent)}\n"
            }
            // Directory inputs need `aws s3 cp --recursive`; a single-object cp
            // of a prefix would silently copy nothing. Detect a directory by the
            // resolved Path (Files.isDirectory) — an s3:// directory input doesn't
            // always render with a trailing slash, so the slash alone is
            // unreliable (PR #38 review) — but ALSO honor a trailing slash, since
            // that's unambiguously a directory even if the path can't be stat'd
            // (provider not registered, etc.).
            boolean isDir = uri.endsWith('/')
            if (!isDir) {
                try {
                    isDir = Files.isDirectory(source)
                } catch (Exception ignored) {
                    // can't stat — leave as the trailing-slash verdict (false)
                }
            }
            final recursive = isDir ? ' --recursive' : ''
            // Dest: ${LOCAL_DIR} stays OUTSIDE the single quotes so it expands to
            // /var/lib/nf-work; only the stage name is quoted (#41 bug 2). The
            // copy is guarded so a failed input copy fails loud — the script runs
            // `set -uo pipefail` (not -e), so an un-piped failed `aws s3 cp` would
            // otherwise be silently ignored and the task would run with a missing
            // input, "succeed" with no output, then fail downstream with a
            // confusing MissingFileException (#41 silent-failure).
            sb << "aws s3 cp ${shellQuote(uri)} \"\${LOCAL_DIR}/\"${shellQuote(stageName)} --region \"\${AWS_REGION}\"${recursive} --quiet || { echo \"nf-spawn: failed to stage input ${stageName} from ${uri}\" >&2; exit 1; }\n"
        }
        sb << '\n'
        return sb.toString()
    }

    // normalizeS3Uri repairs the malformed `s3:///bucket/key` (three slashes,
    // empty authority) that nf-amazon's S3 Path renders from `toUri().toString()`
    // — the bucket lands in the path instead of the authority, so `aws s3 cp`
    // parses an empty bucket and the copy fails (#41 bug 1). Collapse a leading
    // `s3:///` to `s3://` so the first path segment becomes the bucket. A
    // well-formed `s3://bucket/key` is returned unchanged.
    @groovy.transform.PackageScope
    static String normalizeS3Uri(String uri) {
        if (uri?.startsWith('s3:///')) {
            return 's3://' + uri.substring('s3:///'.length())
        }
        return uri
    }

    // localAbsolutePath extracts a local absolute filesystem path from an input
    // URI, or '' if the URI isn't a usable local absolute path. Accepts a
    // `file://` URI (the common rendering of a local Path) or a bare absolute
    // path. Anything relative, empty, or another scheme (already handled as s3://
    // upstream) yields '' so the caller skips it (#49 / nf-spawn#51).
    @groovy.transform.PackageScope
    static String localAbsolutePath(String uri) {
        if (!uri) return ''
        String p = uri
        if (p.startsWith('file://')) {
            // file:///opt/x → /opt/x ; tolerate a stray authority (file://host/…)
            p = p.substring('file://'.length())
            int slash = p.indexOf('/')
            if (slash > 0) {
                p = p.substring(slash)   // drop any authority before the first '/'
            }
        }
        return p.startsWith('/') ? p : ''
    }

    // buildRunLine produces the line that executes .command.sh, capturing stdout
    // /stderr. With no container it runs on the bare OS (`bash .command.sh`).
    // When the process has a `container` directive (#30 — the norm for nf-core
    // modules), it runs inside that image via Docker so tools that live only in
    // the container are found. The work dir is bind-mounted at the SAME path
    // (${LOCAL_DIR}) and set as the working dir, so .command.sh's relative paths
    // and the staged inputs/outputs all line up; `docker run` propagates the
    // task's exit code so ${TASK_RC} stays accurate. --rm cleans up the
    // container; the image is pulled on demand (Docker is preinstalled on the AMI).
    //
    // runOptions is spliced in verbatim (the resolved `docker.runOptions` +
    // per-process `containerOptions`). Dropping it meant a pipeline's
    // `docker { runOptions = '--user root' }` was ignored, so the container ran
    // as the image's default (non-root) user and couldn't write the work dir —
    // every stock module that symlinks inputs / writes outputs failed with
    // "Permission denied" (#39).
    // resolveDockerRunOptions gathers the docker run flags Nextflow would apply:
    // the engine-scope `docker.runOptions` (from the task's ContainerConfig) plus
    // the per-process `containerOptions` directive. Returned as a single string to
    // splice into the `docker run` argv. Best-effort and null-safe — any access
    // failure yields '' (run with no extra options) rather than aborting the task.
    @groovy.transform.PackageScope
    static String resolveDockerRunOptions(TaskRun task) {
        List<String> parts = []
        try {
            def cfg = task.getContainerConfig()
            // ContainerConfig is a Map; runOptions is a plain key.
            def ro = (cfg instanceof Map) ? (cfg as Map).get('runOptions') : null
            if (ro) parts << ro.toString().trim()
        } catch (Exception ignored) { }
        try {
            String co = task.config?.getContainerOptions()
            if (co?.trim()) parts << co.trim()
        } catch (Exception ignored) { }
        return parts.findAll { it }.join(' ').trim()
    }

    @groovy.transform.PackageScope
    static String buildRunLine(String container, String runOptions = '') {
        if (!container?.trim()) {
            return 'bash .command.sh 1>.command.out 2>.command.err\n'
        }
        String image = shellQuote(container.trim())
        String opts = runOptions?.trim() ? runOptions.trim() + ' ' : ''
        return 'docker run --rm -v "${LOCAL_DIR}":"${LOCAL_DIR}" -w "${LOCAL_DIR}" ' +
            opts + image + ' bash .command.sh 1>.command.out 2>.command.err\n'
    }

    // buildExitcodeProbeCommand assembles the argv that streams the work dir's
    // `.exitcode` object to stdout. `aws s3 cp <uri> -` exits non-zero (with a
    // NoSuchKey/404) while the object is absent, which the caller treats as
    // "not finished yet". The work dir URI is normalized to a single trailing
    // segment `.exitcode` regardless of whether workDirUri has a trailing slash.
    @groovy.transform.PackageScope
    static List<String> buildExitcodeProbeCommand(String workDirUri, String region) {
        String base = workDirUri?.endsWith('/') ? workDirUri[0..-2] : (workDirUri ?: '')
        String uri = "${base}/.exitcode"
        return ['aws', 's3', 'cp', uri, '-', '--region', (region ?: 'us-east-1')]
    }

    // buildTerminateCommand assembles the argv that destroys a task's instance.
    // Uses `spawn terminate` (cancel is for parameter sweeps and never destroyed
    // the per-task instance — #58) with -y to skip the irreversible-termination
    // confirmation we can't answer over a pipe. Region is NOT a flag here:
    // terminate reads it from AWS_REGION, which killTask sets on the subprocess.
    @groovy.transform.PackageScope
    static List<String> buildTerminateCommand(String instanceName) {
        return ['spawn', 'terminate', instanceName, '-y']
    }

    // parseExitCode extracts the integer exit status from the .exitcode object's
    // contents (the staging script writes a single integer line). Returns null
    // if the content isn't a parseable integer.
    @groovy.transform.PackageScope
    static Integer parseExitCode(String content) {
        if (content == null) {
            return null
        }
        String trimmed = content.trim()
        if (!trimmed) {
            return null
        }
        // Take the first whitespace-delimited token in case of trailing newline.
        String token = trimmed.split(/\s+/)[0]
        if (token ==~ /-?\d+/) {
            return token.toInteger()
        }
        return null
    }

    // dedentTaskScript writes the task body the way Nextflow writes .command.sh:
    // flush-left, with the common leading whitespace shared by every non-blank
    // line removed. nf-core modules indent their process script for readability
    // and rely on a `<<-END_VERSIONS` heredoc whose terminator sits at the SAME
    // indentation — and `<<-` strips leading TABS only. When we embedded the raw
    // (space-indented) source into our staging heredoc, the terminator stayed
    // indented, `<<-` couldn't strip the spaces, and the heredoc swallowed its
    // own terminator → malformed versions.yml → SnakeYAML aborts the session (#43).
    //
    // We strip the longest common leading-whitespace PREFIX (not a column count),
    // so mixed tab/space indentation is handled uniformly and relative indentation
    // within the script is preserved — matching Groovy's stripIndent semantics.
    // Blank lines are ignored when computing the common prefix.
    @groovy.transform.PackageScope
    static String dedentTaskScript(String script) {
        if (!script) return script ?: ''
        final String[] lines = script.split('\n', -1)
        String common = null
        for (String line : lines) {
            if (!line.trim()) continue   // ignore blank/whitespace-only lines
            final String indent = leadingWhitespace(line)
            if (common == null) {
                common = indent
            } else {
                // shrink `common` to the shared prefix of itself and this indent
                int n = 0
                final int max = Math.min(common.length(), indent.length())
                while (n < max && common.charAt(n) == indent.charAt(n)) n++
                common = common.substring(0, n)
            }
            if (common.isEmpty()) break
        }
        if (!common) return script
        final String prefix = common
        StringBuilder out = new StringBuilder()
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) out << '\n'
            final String l = lines[i]
            out << (l.startsWith(prefix) ? l.substring(prefix.length()) : l)
        }
        return out.toString()
    }

    // leadingWhitespace returns the run of spaces/tabs at the start of a line.
    private static String leadingWhitespace(String line) {
        int i = 0
        while (i < line.length() && (line.charAt(i) == ' ' as char || line.charAt(i) == '\t' as char)) i++
        return line.substring(0, i)
    }

    // shellQuote single-quotes a value for safe interpolation into the script,
    // escaping embedded single quotes via the '\'' idiom.
    @groovy.transform.PackageScope
    static String shellQuote(String value) {
        return "'" + (value ?: '').replace("'", "'\\''") + "'"
    }

    private int spawnCheckComplete() {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                ['spawn', 'task', 'status', taskId, '--region', region, '--check-complete'])
            pb.redirectErrorStream(true)
            Process p = pb.start()
            p.inputStream.text  // drain so the process can exit
            p.waitFor()
            return p.exitValue()
        } catch (Exception e) {
            log.debug "Error running spawn task status for '${taskId}': ${e.message}"
            return 3  // error / no record yet
        }
    }
}
