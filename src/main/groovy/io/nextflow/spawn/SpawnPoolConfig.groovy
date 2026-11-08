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
    // What the workers' IAM role must reach (#106). The pool's role is created at
    // `spawn pool create`, BEFORE any task spec exists, so spawn cannot derive
    // these from the specs the way `spawn task run` does: they have to be passed
    // as --container / --s3-read / --s3-write at create time.
    final List<String> containers
    final List<String> s3Read
    final List<String> s3Write

    private SpawnPoolConfig(boolean enabled, int workers, int minViable, String instanceType,
                            boolean spot, String idleTimeout, String ttl, String runId,
                            List<String> containers, List<String> s3Read, List<String> s3Write) {
        this.enabled = enabled
        this.workers = workers
        this.minViable = minViable
        this.instanceType = instanceType
        this.spot = spot
        this.idleTimeout = idleTimeout
        this.ttl = ttl
        this.runId = runId
        this.containers = containers
        this.s3Read = s3Read
        this.s3Write = s3Write
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

        // Images: every literal `container` in the process config scope (top level
        // and withName:/withLabel: selectors) plus `spawn.pool.containers` for the
        // ones the config can't show (an image declared inside a module). spawn
        // uses them to decide the ECR pull grant.
        Set<String> containers = new LinkedHashSet<String>(configuredContainers((cfg.process ?: [:]) as Map))
        containers.addAll(stringList(pool.containers))

        // Buckets: the S3 work dir is where every pooled task stages from AND
        // writes to, so it is always a write grant, mirroring the per-task spec's
        // resources.s3_read_write (SpawnTaskHandler.buildTaskSpec). Input buckets
        // outside it are not knowable at run start, so they come from config.
        // Every entry is normalized to s3://<bucket> (IAM grants are bucket-scoped,
        // and spawn drops any /prefix too), so duplicates collapse.
        Set<String> s3Write = new LinkedHashSet<String>()
        String workBucket = workDirBucket(session, cfg)
        if (workBucket) s3Write << workBucket
        for (String b : stringList(pool.s3Write)) { String u = bucketUri(b); if (u) s3Write << u }
        Set<String> s3Read = new LinkedHashSet<String>()
        for (String b : stringList(pool.s3Read)) { String u = bucketUri(b); if (u) s3Read << u }
        s3Read.removeAll(s3Write)   // a write grant already includes read

        return new SpawnPoolConfig(enabled, workers, minViable, instanceType, spot, idleTimeout, ttl, runId,
                                   containers as List<String>, s3Read as List<String>, s3Write as List<String>)
    }

    // configuredContainers returns the literal `container` values in a Nextflow
    // `process { }` config scope: the top-level one and each withName:/withLabel:
    // selector's. A dynamic (closure) value is skipped; it is resolved per task,
    // too late for pool creation, and has to be listed in spawn.pool.containers.
    @groovy.transform.PackageScope
    static List<String> configuredContainers(Map process) {
        Set<String> out = new LinkedHashSet<String>()
        addLiteral(out, process?.get('container'))
        process?.each { Object k, Object v ->
            String key = k?.toString() ?: ''
            if ((key.startsWith('withName:') || key.startsWith('withLabel:')) && v instanceof Map) {
                addLiteral(out, ((Map) v).get('container'))
            }
        }
        return out as List<String>
    }

    private static void addLiteral(Set<String> out, Object v) {
        if (v instanceof CharSequence && v.toString().trim()) {
            out << v.toString().trim()
        }
    }

    // stringList accepts a list or a comma/whitespace-separated string, the two
    // shapes a config value naturally takes.
    @groovy.transform.PackageScope
    static List<String> stringList(Object v) {
        if (v == null) return []
        Collection items = v instanceof Collection ? (Collection) v : (v.toString().split(/[,\s]+/) as List)
        List<String> out = []
        for (Object o : items) {
            String t = o?.toString()?.trim()
            if (t) out << t
        }
        return out
    }

    // bucketUri turns "bucket", "bucket/prefix" or "s3://bucket/prefix" into
    // s3://bucket.
    @groovy.transform.PackageScope
    static String bucketUri(String v) {
        String t = v?.trim() ?: ''
        if (!t) return ''
        return SpawnTaskHandler.s3BucketUri(SpawnTaskHandler.normalizeS3Uri(t.startsWith('s3:') ? t : 's3://' + t))
    }

    // workDirBucket returns s3://<bucket> of the run's work dir, or '' when it is
    // not on S3 (the task handler rejects that run anyway). The session's workDir
    // is authoritative (`-w` overrides config); config.workDir is the fallback.
    @groovy.transform.PackageScope
    static String workDirBucket(Session session, Map cfg) {
        String uri = ''
        try {
            uri = session?.workDir?.toUri()?.toString() ?: ''
        } catch (Exception ignored) {
            // a session without a resolvable work dir; fall back to config
        }
        if (!uri?.startsWith('s3:')) {
            uri = (cfg?.workDir ?: '') as String
        }
        return SpawnTaskHandler.s3BucketUri(SpawnTaskHandler.normalizeS3Uri(uri))
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
