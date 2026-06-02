# nf-spawn

A [Nextflow](https://nextflow.io) executor plugin that uses [spore-host/spawn](https://github.com/spore-host/spawn) as the compute fabric — run each pipeline process step on its own ephemeral EC2 instance, purpose-sized and auto-terminated when the task completes.

## Status

⚠️ **Early prototype.** Not production-ready. Contributions welcome.

## How it works

Each Nextflow task is dispatched to a fresh EC2 instance via `spawn launch`. The instance runs the task script, signals completion via `spored complete`, and terminates automatically. Nextflow polls `spawn status --check-complete` to detect when each task finishes.

```
Pipeline process → SpawnTaskHandler.submit()
                     → spawn launch nf-<hash> --instance-type c7g.4xlarge --on-complete terminate
                     → instance runs task script
                     → spored complete (signals done)
                 ← SpawnTaskHandler.checkIfRunning()
                     → spawn status nf-<hash> --check-complete
                     → exit 0 = done, exit 2 = still running
```

## Requirements

- [spawn](https://github.com/spore-host/spawn) CLI installed and on `PATH`
- AWS credentials configured (`~/.aws/credentials`, environment variables, or EC2 instance metadata)
- Nextflow 26.04.x (the version this plugin is built against — see [Build & toolchain](#build--toolchain))

## Installation

Add to `nextflow.config`:

```groovy
plugins {
    id 'nf-spawn@0.2.0'
}
```

Or install locally during development:
```bash
./gradlew installPlugin   # builds and unpacks into ~/.nextflow/plugins/nf-spawn-<version>/
```

## Configuration

```groovy
// nextflow.config
plugins {
    id 'nf-spawn@0.2.0'
}

process {
    executor = 'spawn'

    // Default instance type for all processes
    ext.instanceType = 't3.medium'
    ext.region       = 'us-east-1'
    ext.ttl          = '2h'

    // Per-process overrides
    withName: 'KRAKEN2' {
        ext.instanceType = 'c7g.4xlarge'   // 16 vCPU, 32 GB — Kraken2 DB fits in RAM
        ext.spot         = true
    }
    withName: 'FASTP' {
        ext.instanceType = 't4g.medium'    // cheap QC step
    }
}

// S3 work directory (required for multi-instance pipelines)
workDir = 's3://my-bucket/nextflow-work'
```

## Example pipeline

```nextflow
process FASTP {
    input:  path reads
    output: path 'trimmed.fastq.gz'

    script:
    """
    fastp -i $reads -o trimmed.fastq.gz
    """
}

process KRAKEN2 {
    ext.instanceType = 'c7g.4xlarge'

    input:  path reads
    output: path 'report.txt'

    script:
    """
    kraken2 --db /kraken2-db --output report.txt $reads
    """
}

workflow {
    reads = Channel.fromPath('data/*.fastq.gz')
    FASTP(reads) | KRAKEN2
}
```

## Build & toolchain

nf-spawn is built with the official **[`io.nextflow.nextflow-plugin`](https://github.com/nextflow-io/nextflow-plugin-gradle)** Gradle toolchain, and **tracks a specific Nextflow release**. The target version is declared once in `build.gradle`:

```groovy
nextflowPlugin {
    nextflowVersion = '26.04.3'   // the Nextflow API this plugin is built and tested against
    className       = 'io.nextflow.spawn.SpawnPlugin'
    extensionPoints = ['io.nextflow.spawn.SpawnExecutor']
}
```

That single `nextflowVersion` is the source of truth for the Nextflow API the plugin compiles against. The toolchain resolves the Nextflow API (including 26.x, which Maven Central doesn't carry) from the Seqera Maven repository, and **generates** the plugin manifest (`Plugin-Requires`, `Plugin-Class`, …) and `META-INF/extensions.idx` from this block — so the build can't drift from the Nextflow version it targets. To follow a new Nextflow release, bump `nextflowVersion` and rebuild; do not hand-edit manifests.

Commands:

```bash
./gradlew assemble        # build the plugin zip (build/distributions/nf-spawn-<version>.zip)
./gradlew installPlugin   # build + install into ~/.nextflow/plugins for local testing
./gradlew test            # run tests
```

Requires a JDK (the Gradle toolchain auto-provisions JDK 21 for compilation; sources target Java 17).

## Related

- [spore-host/spawn](https://github.com/spore-host/spawn) — the EC2 lifecycle tool this plugin wraps
- [Nextflow plugin documentation](https://www.nextflow.io/docs/latest/plugins.html)
- [nf-core pipelines](https://nf-co.re) — pipelines that could benefit from per-task instance sizing
