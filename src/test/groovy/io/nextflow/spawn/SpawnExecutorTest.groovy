package io.nextflow.spawn

import nextflow.Session
import nextflow.container.DockerConfig
import nextflow.processor.TaskConfig
import nextflow.processor.TaskHasher
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfig
import spock.lang.Specification

// #107: a changed container image must invalidate -resume. Nextflow's
// TaskHasher adds the container fingerprint only when task.isContainerEnabled(),
// which is true for a container-native executor (the executor runs the
// container itself) or when a container engine is enabled in config. nf-spawn
// runs its own `docker run` and needs no engine block, so it must declare
// itself container-native, as the AWS Batch / Google Batch / K8s executors do.
class SpawnExecutorTest extends Specification {

    def 'the spawn executor is container-native: it runs the container itself (#107)'() {
        expect:
        new SpawnExecutor().isContainerNative()
    }

    private TaskRun taskWith(String image) {
        def session = Mock(Session) {
            getUniqueId() >> UUID.fromString('b69b6eeb-b332-4d2c-9957-c291b15f498c')
            getBinEntries() >> [:]
            getConfig() >> [:]
            // no docker/podman/... block in nextflow.config: no engine is enabled
            getContainerConfig(_) >> new DockerConfig([:])
        }
        def processor = Mock(TaskProcessor) {
            getName() >> 'ALIGN'
            getSession() >> session
            getConfig() >> Mock(ProcessConfig)
            getExecutor() >> new SpawnExecutor()
        }
        TaskRun task = Spy(TaskRun)
        task.processor = processor
        task.config = new TaskConfig()
        // Image resolution is Nextflow's (plugin-loaded resolver); stub its output.
        // isContainerEnabled() and getContainerConfig() stay REAL: they are the
        // path that decides whether the image enters the hash.
        task.getSource() >> 'bwa mem ref.fa reads.fq > out.sam'
        task.getContainer() >> image
        task.getContainerFingerprint() >> image
        return task
    }

    private String hashOf(TaskRun task) {
        TaskHasher hasher = Spy(new TaskHasher(task))
        hasher.getTaskGlobalVars() >> [:]
        hasher.getTaskBinEntries(_) >> []
        return hasher.compute().toString()
    }

    def 'with no container engine configured, the task hash still covers the container image (#107)'() {
        when:
        def a = taskWith('quay.io/biocontainers/bwa:0.7.17--h5bf99c6_8')
        def b = taskWith('quay.io/biocontainers/bwa:0.7.18--he4a0461_0')
        def c = taskWith('quay.io/biocontainers/bwa:0.7.17--h5bf99c6_8')

        then: 'the container counts as enabled, so TaskHasher includes its fingerprint'
        a.isContainerEnabled()

        and: 'a repin changes the hash (so -resume re-runs the task); the same image keeps it'
        hashOf(a) != hashOf(b)
        hashOf(a) == hashOf(c)
    }

    def 'a process with no container is unaffected: not container-enabled (#107)'() {
        expect:
        !taskWith(null).isContainerEnabled()
    }
}
