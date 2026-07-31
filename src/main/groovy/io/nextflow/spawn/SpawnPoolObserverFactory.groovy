package io.nextflow.spawn

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceObserverFactoryV2
import org.pf4j.Extension
import org.pf4j.ExtensionPoint

// SpawnPoolObserverFactory registers the run-scoped pool lifecycle observer with
// Nextflow (#70 Phase 2). Nextflow discovers TraceObserverFactoryV2 pf4j
// extensions and calls create(session) once per run; we return the observer,
// which no-ops unless `spawn.pool.enabled`. Registered via the
// nextflowPlugin.extensionPoints list in build.gradle, alongside SpawnExecutor.
@CompileStatic
@Extension
class SpawnPoolObserverFactory implements TraceObserverFactoryV2, ExtensionPoint {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        return [new SpawnPoolObserver()] as Collection<TraceObserverV2>
    }
}
