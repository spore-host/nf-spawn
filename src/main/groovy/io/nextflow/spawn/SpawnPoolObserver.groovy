package io.nextflow.spawn

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.exception.AbortOperationException
import nextflow.trace.TraceObserverV2

// SpawnPoolObserver is the run-scoped lifecycle hook (#70 Phase 2). When
// `spawn.pool.enabled`, it provisions the worker pool + task queue once at run
// start (onFlowCreate) and drains them at run end (onFlowComplete) — the piece
// nf-spawn lacked, since the executor is otherwise per-task.
//
// It shells out to the `spawn pool` CLI (spawn#456), the same way the task handler
// shells out to `spawn task run` — nf-spawn is JVM and the pool lives in the Go
// `spawn` binary. The run id + knobs come from SpawnPoolConfig, derived from the
// session, so the handler's per-task `spawn pool submit` targets the same queue
// this observer created without any shared mutable state.
//
// When pool mode is OFF this observer is inert: every hook returns immediately, so
// a non-pool run behaves exactly as before.
@Slf4j
@CompileStatic
class SpawnPoolObserver implements TraceObserverV2 {

    private SpawnPoolConfig poolConfig
    private String region

    // onFlowCreate runs once at the start of the run, before any task submits.
    // It provisions the pool (best-effort/eventual via `spawn pool create`, which
    // uses a PartialCohort) and creates the queue. A provisioning failure that
    // can't reach min-viable aborts the run HERE — before any task is enqueued —
    // rather than letting tasks pile up against a queue with no workers.
    @Override
    void onFlowCreate(Session session) {
        this.poolConfig = SpawnPoolConfig.fromSession(session)
        if (!poolConfig.enabled) {
            return
        }
        String err = poolConfig.validationError()
        if (err) {
            throw new AbortOperationException("nf-spawn pool config invalid: ${err}")
        }
        // Region: reuse the same default the task handler uses (ext.region or
        // us-east-1). The pool is region-scoped; a single region matches the
        // one-queue-per-run model. Read from the spawn config block if present.
        Map cfg = (session?.config ?: [:]) as Map
        Map spawn = (cfg.spawn ?: [:]) as Map
        this.region = (spawn.region ?: 'us-east-1') as String

        log.info "Provisioning spawn worker pool '${poolConfig.runId}': ${poolConfig.workers} × ${poolConfig.instanceType} (min viable ${poolConfig.minViable})"
        List<String> cmd = buildCreateCommand(poolConfig, region)
        runSpawnPool(cmd, "provision pool", true)
        log.info "spawn worker pool '${poolConfig.runId}' ready; tasks will be enqueued to it"
    }

    // onFlowComplete runs at the end of the run (success OR failure), so the queue
    // is always torn down. Workers self-terminate on idle-timeout independently and
    // the reaper backstops a missed drain, so a drain failure is logged, not fatal —
    // failing the run here would be pointless (the run is already over) and could
    // mask the real outcome.
    @Override
    void onFlowComplete() {
        if (poolConfig == null || !poolConfig.enabled) {
            return
        }
        log.info "Draining spawn worker pool '${poolConfig.runId}'"
        List<String> cmd = buildDrainCommand(poolConfig, region)
        try {
            runSpawnPool(cmd, "drain pool", false)
        } catch (Exception e) {
            log.warn "spawn pool drain failed for '${poolConfig.runId}': ${e.message} — " +
                     "workers idle-drain on their own and the reaper backstops, so this is not fatal"
        }
    }

    // buildCreateCommand assembles `spawn pool create` argv from the pool config.
    // PackageScope so a Spock test can assert the argv without running spawn.
    @groovy.transform.PackageScope
    static List<String> buildCreateCommand(SpawnPoolConfig pc, String region) {
        List<String> cmd = ['spawn', 'pool', 'create',
                            '--run-id', pc.runId,
                            '--workers', pc.workers.toString(),
                            '--min-viable', pc.minViable.toString(),
                            '--instance-type', pc.instanceType,
                            '--idle-timeout', pc.idleTimeout,
                            '--ttl', pc.ttl]
        if (pc.spot) {
            cmd << '--spot'
        }
        return cmd
    }

    // buildDrainCommand assembles `spawn pool drain` argv.
    @groovy.transform.PackageScope
    static List<String> buildDrainCommand(SpawnPoolConfig pc, String region) {
        return ['spawn', 'pool', 'drain', '--run-id', pc.runId]
    }

    // runSpawnPool runs a `spawn pool` subprocess, scoping AWS_REGION so the pool
    // targets the run's region (spawn pool has no --region flag; it reads the
    // configured region). fatal=true throws on non-zero exit (provisioning must
    // succeed before tasks submit); fatal=false lets the caller decide.
    private void runSpawnPool(List<String> cmd, String what, boolean fatal) {
        log.debug "spawn pool command: ${cmd.join(' ')}"
        ProcessBuilder pb = new ProcessBuilder(cmd)
        pb.redirectErrorStream(true)
        if (region) {
            pb.environment().put('AWS_REGION', region)
            pb.environment().put('AWS_DEFAULT_REGION', region)
        }
        Process p = pb.start()
        String output = p.inputStream.text
        int rc = p.waitFor()
        if (rc != 0) {
            String msg = "spawn pool ${what} failed (exit ${rc}) for '${poolConfig.runId}':\n${output}"
            if (fatal) {
                throw new AbortOperationException(msg)
            }
            throw new RuntimeException(msg)
        }
        log.debug "spawn pool ${what} output:\n${output}"
    }
}
