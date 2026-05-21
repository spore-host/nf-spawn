package io.nextflow.spawn

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

@CompileStatic
class SpawnPlugin extends BasePlugin {
    SpawnPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
}
