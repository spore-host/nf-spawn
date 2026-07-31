package io.nextflow.spawn

import groovy.transform.CompileStatic
import nextflow.Session

// SpawnPoolConfig reads the opt-in `spawn.pool { … }` block from the Nextflow
// session config and derives the run-scoped pool identity (#70 Phase 2).
//
// Pool mode runs a wide fan-out as a set of fungible worker instances draining a
// shared queue, instead of one instance per task. It is OFF by default, so a
// pipeline that doesn't set `spawn.pool.enabled` keeps today's per-task behavior.
//
// Both the observer (which provisions/drains the pool per run) and the task
// handler (which enqueues each task) construct one of these from the SAME session,
// so they agree on the run id and knobs WITHOUT sharing mutable state — the run id
// is a pure function of the session's uniqueId.
@CompileStatic
class SpawnPoolConfig {

    final boolean enabled
    final int workers
    final int minViable
    final String instanceType
    final boolean spot
    final String idleTimeout
    final String ttl
    final String runId

    private SpawnPoolConfig(boolean enabled, int workers, int minViable, String instanceType,
                            boolean spot, String idleTimeout, String ttl, String runId) {
        this.enabled = enabled
        this.workers = workers
        this.minViable = minViable
        this.instanceType = instanceType
        this.spot = spot
        this.idleTimeout = idleTimeout
        this.ttl = ttl
        this.runId = runId
    }

    // fromSession reads `spawn.pool.*` from the session config and derives the run
    // id from the session uniqueId. The config map is untyped (Groovy), so every
    // access is a cast + default, mirroring how the handler reads task.config.ext.
    static SpawnPoolConfig fromSession(Session session) {
        Map cfg = (session?.config ?: [:]) as Map
        Map spawn = (cfg.spawn ?: [:]) as Map
        Map pool = (spawn.pool ?: [:]) as Map

        boolean enabled = pool.enabled ? true : false
        int workers = (pool.workers ?: 0) as int
        int minViable = (pool.minViable ?: 1) as int
        String instanceType = (pool.instanceType ?: '') as String
        boolean spot = pool.spot ? true : false
        String idleTimeout = (pool.idleTimeout ?: '5m') as String
        String ttl = (pool.ttl ?: '4h') as String

        String runId = poolRunId(session)
        return new SpawnPoolConfig(enabled, workers, minViable, instanceType, spot, idleTimeout, ttl, runId)
    }

    // poolRunId derives a stable, SQS-name-safe run id from the session's unique
    // id. SQS queue names allow [A-Za-z0-9_-] up to 80 chars; a UUID
    // (hex + dashes) already fits, so we only need to guard against a null/odd id.
    // Prefixed with "nf-" so a pool queue is identifiable among an account's queues.
    static String poolRunId(Session session) {
        String uid = session?.uniqueId?.toString() ?: 'unknown'
        String safe = uid.replaceAll(/[^A-Za-z0-9_-]/, '-')
        String id = "nf-${safe}"
        return id.length() > 80 ? id.substring(0, 80) : id
    }

    // validate throws when pool mode is enabled but a required knob is missing, so
    // a misconfigured run fails fast at onFlowCreate rather than provisioning a
    // broken pool. Returns a human-readable error message, or null when valid.
    String validationError() {
        if (!enabled) {
            return null
        }
        if (workers < 1) {
            return "spawn.pool.workers must be >= 1 when spawn.pool.enabled"
        }
        if (!instanceType?.trim()) {
            return "spawn.pool.instanceType is required when spawn.pool.enabled"
        }
        return null
    }
}
