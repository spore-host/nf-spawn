package io.nextflow.spawn

import nextflow.Session
import spock.lang.Specification

// Unit tests for pooled execution (#70 Phase 2): config parsing, run-id
// derivation, and the `spawn pool` argv builders. Pure/static — no AWS, no
// instance, mirroring the rest of the nf-spawn Spock suite.
class SpawnPoolTest extends Specification {

    private Session sessionWith(Map spawnCfg, UUID uid) {
        Session s = Stub(Session)
        s.getConfig() >> [spawn: spawnCfg]
        s.getUniqueId() >> uid
        return s
    }

    def 'pool is disabled by default (no spawn.pool block) — preserves per-task behavior'() {
        given:
        def s = sessionWith([:], UUID.fromString('11111111-2222-3333-4444-555555555555'))

        when:
        def pc = SpawnPoolConfig.fromSession(s)

        then:
        !pc.enabled
        pc.validationError() == null   // disabled config is always valid
    }

    def 'fromSession reads the spawn.pool block'() {
        given:
        def s = sessionWith([pool: [
            enabled     : true,
            workers     : 100,
            minViable   : 10,
            instanceType: 'c7i.large',
            spot        : true,
            idleTimeout : '3m',
            ttl         : '6h',
        ]], UUID.fromString('11111111-2222-3333-4444-555555555555'))

        when:
        def pc = SpawnPoolConfig.fromSession(s)

        then:
        pc.enabled
        pc.workers == 100
        pc.minViable == 10
        pc.instanceType == 'c7i.large'
        pc.spot
        pc.idleTimeout == '3m'
        pc.ttl == '6h'
        pc.validationError() == null
    }

    def 'enabled pool with missing knobs fails validation'() {
        expect:
        SpawnPoolConfig.fromSession(sessionWith([pool: [enabled: true, instanceType: 'c7i.large']], UUID.randomUUID()))
            .validationError() =~ /workers must be >= 1/

        SpawnPoolConfig.fromSession(sessionWith([pool: [enabled: true, workers: 5]], UUID.randomUUID()))
            .validationError() =~ /instanceType is required/
    }

    def 'run id is stable for a fixed session uniqueId and SQS-name-safe'() {
        given:
        def uid = UUID.fromString('abcdef01-2345-6789-abcd-ef0123456789')
        def s1 = sessionWith([pool: [enabled: true, workers: 1, instanceType: 't3.medium']], uid)
        def s2 = sessionWith([pool: [enabled: true, workers: 1, instanceType: 't3.medium']], uid)

        when:
        def id1 = SpawnPoolConfig.fromSession(s1).runId
        def id2 = SpawnPoolConfig.fromSession(s2).runId

        then: 'deterministic for the same session — observer and handler agree without shared state'
        id1 == id2
        id1 == "nf-${uid.toString()}"

        and: 'only SQS-legal characters, within the 80-char limit'
        id1 ==~ /[A-Za-z0-9_-]{1,80}/
    }

    def 'run id is derived even when the session has no uniqueId (defensive fallback)'() {
        given: 'a session whose uniqueId is null (never expected, but must not NPE)'
        Session s = Stub(Session)
        s.getConfig() >> [spawn: [:]]
        s.getUniqueId() >> null

        when:
        def id = SpawnPoolConfig.poolRunId(s)

        then: 'a safe, non-empty, SQS-legal id is still produced'
        id ==~ /[A-Za-z0-9_-]{1,80}/
    }

    def 'buildCreateCommand maps config to spawn pool create argv'() {
        given:
        def s = sessionWith([pool: [
            enabled: true, workers: 50, minViable: 5, instanceType: 'c7i.xlarge',
            spot: true, idleTimeout: '2m', ttl: '3h',
        ]], UUID.fromString('11111111-2222-3333-4444-555555555555'))
        def pc = SpawnPoolConfig.fromSession(s)

        when:
        def cmd = SpawnPoolObserver.buildCreateCommand(pc, 'us-east-1')

        then:
        cmd[0..2] == ['spawn', 'pool', 'create']
        cmd.containsAll(['--run-id', pc.runId])
        cmd.containsAll(['--workers', '50'])
        cmd.containsAll(['--min-viable', '5'])
        cmd.containsAll(['--instance-type', 'c7i.xlarge'])
        cmd.containsAll(['--idle-timeout', '2m'])
        cmd.containsAll(['--ttl', '3h'])
        cmd.contains('--spot')
    }

    def 'buildCreateCommand omits --spot for on-demand pools'() {
        given:
        def s = sessionWith([pool: [enabled: true, workers: 4, instanceType: 'm7i.large', spot: false]],
            UUID.randomUUID())
        def pc = SpawnPoolConfig.fromSession(s)

        expect:
        !SpawnPoolObserver.buildCreateCommand(pc, 'us-east-1').contains('--spot')
    }

    def 'buildDrainCommand targets the run queue'() {
        given:
        def s = sessionWith([pool: [enabled: true, workers: 1, instanceType: 't3.medium']],
            UUID.fromString('abcdef01-2345-6789-abcd-ef0123456789'))
        def pc = SpawnPoolConfig.fromSession(s)

        expect:
        SpawnPoolObserver.buildDrainCommand(pc, 'us-east-1') ==
            ['spawn', 'pool', 'drain', '--run-id', "nf-abcdef01-2345-6789-abcd-ef0123456789"]
    }

    def 'buildPoolSubmitCommand enqueues a spec to the run queue (no launch)'() {
        when:
        def cmd = SpawnTaskHandler.buildPoolSubmitCommand('nf-run-1', '/tmp/spec.json')

        then:
        cmd == ['spawn', 'pool', 'submit', '--run-id', 'nf-run-1', '--spec', '/tmp/spec.json']
        !cmd.contains('run')       // it's `pool submit`, not `task run`
        !cmd.contains('--workers') // submit doesn't provision
    }

    def 'observer factory produces a pool observer'() {
        when:
        def observers = new SpawnPoolObserverFactory().create(Stub(Session))

        then:
        observers.size() == 1
        observers[0] instanceof SpawnPoolObserver
    }
}
