package io.nextflow.spawn

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import org.pf4j.Extension
import org.pf4j.ExtensionPoint

// @Extension marks this as a pf4j extension so Nextflow's ExecutorFactory
// discovers it. The annotation alone isn't reliable under Groovy (the pf4j
// annotation processor doesn't run), so it's also listed explicitly in
// META-INF/extensions.idx — the convention Nextflow core plugins follow (#7).
@Slf4j
@CompileStatic
@Extension
class SpawnExecutor extends Executor implements ExtensionPoint {

    @Override
    String getName() { return 'spawn' }

    // Must match the base method's visibility (public); declaring it protected
    // assigns weaker access and fails to override (#3).
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        log.debug "Creating SpawnTaskHandler for task: ${task.name}"
        return new SpawnTaskHandler(task, this)
    }

    // Executor declares createTaskMonitor() abstract; provide a polling monitor.
    @Override
    protected TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, name, 100, Duration.of('5 sec'))
    }

    @Override
    boolean isContainerNative() { return false }
}
