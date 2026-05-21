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
- Nextflow 23.10.0+

## Installation

Add to `nextflow.config`:

```groovy
plugins {
    id 'nf-spawn@0.1.0'
}
```

Or install locally during development:
```bash
./gradlew jar
cp build/libs/nf-spawn-0.1.0.jar ~/.nextflow/plugins/nf-spawn-0.1.0/
```

## Configuration

```groovy
// nextflow.config
plugins {
    id 'nf-spawn@0.1.0'
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

## Building

```bash
./gradlew jar
```

## Related

- [spore-host/spawn](https://github.com/spore-host/spawn) — the EC2 lifecycle tool this plugin wraps
- [Nextflow plugin documentation](https://www.nextflow.io/docs/latest/plugins.html)
- [nf-core pipelines](https://nf-co.re) — pipelines that could benefit from per-task instance sizing
