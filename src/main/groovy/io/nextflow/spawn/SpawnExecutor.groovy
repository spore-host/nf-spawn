package io.nextflow.spawn

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import org.pf4j.ExtensionPoint

@Slf4j
@CompileStatic
class SpawnExecutor extends Executor implements ExtensionPoint {

    @Override
    String getName() { return 'spawn' }

    @Override
    protected TaskHandler createTaskHandler(TaskRun task) {
        log.debug "Creating SpawnTaskHandler for task: ${task.name}"
        return new SpawnTaskHandler(task, this)
    }

    @Override
    boolean isContainerNative() { return false }
}
