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

    // Attach a pre-populated EBS volume from a snapshot (e.g. a large reference
    // DB) instead of baking it into a custom AMI. Mounted read-only by default;
    // requires spawn >= 0.46.0.
    withName: 'KRAKEN2_KRAKEN2' {
        ext.instanceType = 'r7g.2xlarge'
        ext.volumes = [[ snapshot: 'snap-0abc', mount: '/opt/databases/kraken2', readOnly: true ]]
    }
}

// S3 work directory (required for multi-instance pipelines)
workDir = 's3://my-bucket/nextflow-work'
```

### Per-process `ext` options

| Option | Default | Description |
|--------|---------|-------------|
| `ext.instanceType` | `t3.medium` | EC2 instance type for the task |
| `ext.region` | `us-east-1` | AWS region |
| `ext.ttl` | `2h` | Max instance lifetime (safety backstop) |
| `ext.spot` | `false` | Launch as a Spot instance |
| `ext.ami` | _(auto)_ | Explicit AMI ID; omit to let spawn auto-detect a stock AMI |
| `ext.volumeSize` | _(AMI min)_ | Extra root EBS size in GiB beyond the AMI minimum |
| `ext.volumes` | _(none)_ | List of `[snapshot:, mount:, readOnly:]` maps — attach EBS data volumes from snapshots (read-only by default), bind-mounted into the task container. Works for both direct reads and staged `path` inputs (symlinked, zero-copy) — see [Delivering reference data](#delivering-reference-data). Requires spawn ≥ 0.46.0 |
| `ext.ensureDocker` | `true` | Auto-install + start Docker on the task instance when a `container` is set (idempotent), so a stock AMI works. Set `false` if your AMI already has Docker |
| `ext.packages` | _(none)_ | Host packages to `dnf install` before the task — a list (`['pigz','ethtool']`) or a space/comma string. For tools the task calls on the instance |
| `ext.setup` | _(none)_ | Arbitrary shell command run on the instance before the task (after Docker/packages) |

> With `ext.volumes` + the setup hooks above, nf-spawn runs on a **stock AL2023
> AMI** — no custom AMI needed. For wide fan-out, a small pre-baked tools AMI
> avoids the per-task install latency.

### Delivering reference data

Large reference data (a Kraken2/MetaPhlAn DB, BLAST index) reaches a task two
ways. Prefer **`ext.volumes`** — it's read straight off an attached read-only
snapshot with **zero copy and zero per-task download**.

**1. `ext.volumes` — a read-only snapshot, mounted (recommended).**
The DB lives on an EBS snapshot (`spawn snapshot create`), attached read-only at
a known path. nf-spawn now makes this work for **both** ways a process consumes
the data:

- **Direct reads** — a tool you invoke yourself with `--db /opt/databases/x`
  reads the mount directly.
- **Staged `path` inputs** — the nf-core `db_path` pattern (e.g. taxprofiler's
  `databases.csv`). When a declared input's **stage-name basename matches an
  `ext.volumes` mount** (e.g. input `metaphlan` ↔ mount `/opt/databases/metaphlan`),
  nf-spawn **symlinks** that stage name → the mount and **skips the copy**, then
  **bind-mounts** the volume into the container — so a tool that does
  `find -L <db>` resolves to the read-only volume, zero-copy. This holds even
  though such pipelines *stage* `db_path` into the Nextflow work area (the match
  is by stage name, not source URI; requires nf-spawn ≥ 0.6.0). **Name the mount
  to match the input** — for taxprofiler, the `db_name` drives the stage name.

  > **The head node also needs the DB at that path.** nf-core pipelines validate
  > `db_path` exists on the head at init (before any task launches); satisfying it
  > needs **no pipeline fork**, just the snapshot mounted there too. How depends on
  > where you run `nextflow run`:
  >
  > - **Head is itself a spawn-launched EC2 instance** (e.g. you launch the
  >   controller with `spawn launch … --attach-volume snap-xxx:/opt/databases/x`):
  >   **nothing to do.** spawn's attached-volume user-data `mkdir -p`s the mount
  >   point and mounts the volume read-only on the head automatically — the same
  >   code path as the tasks. This is the easy case.
  > - **Head is a laptop / non-spawn box:** attach + mount it yourself once —
  >   `spawn snapshot mount snap-xxx /opt/databases/x` (convenience for an EC2
  >   head), or the manual `aws ec2 create-volume --snapshot-id … && attach-volume
  >   … && sudo mount -o ro …`. (A laptop can't attach EBS at all, so run the head
  >   on a small EC2 box, or use option 2 below.)
  >
  > Either way the mount-point directory is created for you on spawn-launched
  > instances; you never pre-create it.

**2. An `s3://` `db_path` — download-per-task fallback.**
If you can't mount on the head (or don't want to), point `db_path` at an `s3://`
URI: nf-spawn's declared-input localization (#37) `aws s3 cp`s it onto each task,
and an `s3://` URI also skips nf-core's head-side existence check. The trade-off
is that **every task downloads the full DB (16–34 GB)** — wasteful for anything
beyond small fan-out. Prefer option 1.

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
