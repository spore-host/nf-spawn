<p align="center">
  <img src="docs/assets/nf-spawn-hero.png" alt="nf-spawn — a Nextflow executor plugin for spore.host" width="820">
</p>

# nf-spawn

A [Nextflow](https://nextflow.io) executor plugin that uses [spore-host/spawn](https://github.com/spore-host/spawn) as the compute fabric — run each pipeline process step on its own ephemeral EC2 instance, purpose-sized and auto-terminated when the task completes.

## Status

⚠️ **Early prototype.** Not production-ready. Contributions welcome.

## How it works

Each Nextflow task is dispatched via **`spawn task run`** (the shared spawn
workflow-adapter contract): nf-spawn builds a TaskSpec — the staging script, the
sized instance type, and a scoped least-privilege IAM profile — and spawn sizes,
launches, and runs it on a fresh ephemeral EC2 instance that self-terminates on
completion. Completion is detected from the **durable `.exitcode` / `completion.json`
that the instance writes to the S3 work dir** (not by SSH-ing the instance, which
has already self-terminated), so a task's result survives the instance.

```
Pipeline process → SpawnTaskHandler.submit()
                     → spawn task run --spec <taskspec.json>   (detached)
                     → instance stages inputs, runs the task, stages outputs,
                       writes completion.json + .exitcode to the S3 work dir,
                       then self-terminates
                 ← SpawnTaskHandler.checkIfCompleted()
                     → reads <workDir>/.exitcode from S3
                     → present = done (with its exit status); absent = still running
```

For a **wide fan-out** (hundreds of short tasks), enable [pool
mode](#pooled-execution-for-wide-fan-out-spawnpool): instead of one instance per
task, a fixed set of reusable workers pull tasks from a shared queue — dispatch
stops being launch-rate-bound and concurrency reaches the pool size you ask for.

## Requirements

- [spawn](https://github.com/spore-host/spawn) CLI installed and on `PATH`
- AWS credentials configured (`~/.aws/credentials`, environment variables, or EC2 instance metadata)
- Nextflow 26.04.x (the version this plugin is built against — see [Build & toolchain](#build--toolchain))

## Installation

> **nf-spawn is not (yet) in the [Nextflow plugin registry](https://github.com/nextflow-io/plugins).**
> That means the common `plugins { id 'nf-spawn@X.Y.Z' }` form Nextflow can
> resolve automatically does **not** work for this plugin — Nextflow would try
> to fetch it from the registry index and fail, since nf-spawn isn't listed
> there. Install the release zip manually instead (below). If/when nf-spawn is
> published to the registry, this section will change to the one-line form.

**1. Download** the release zip for the version you want from the
[Releases page](https://github.com/spore-host/nf-spawn/releases) (or `curl`
it directly — replace `X.Y.Z` with a real release, e.g. `0.10.0`):

```bash
curl -sSL -o nf-spawn-X.Y.Z.zip \
    https://github.com/spore-host/nf-spawn/releases/download/vX.Y.Z/nf-spawn-X.Y.Z.zip
```

**2. Unpack it** into `~/.nextflow/plugins/`:

```bash
mkdir -p ~/.nextflow/plugins
unzip -o nf-spawn-X.Y.Z.zip -d ~/.nextflow/plugins/nf-spawn-X.Y.Z
```

**3. Reference it** in `nextflow.config` using the same version:

```groovy
plugins {
    id 'nf-spawn@X.Y.Z'
}
```

Nextflow resolves a plugin already present under `~/.nextflow/plugins/` before
it ever consults the registry, so this works offline and needs no registry
listing. The version in `nextflow.config` must match the plugin manifest's
`Plugin-Version` exactly (as of #91, the release workflow enforces that every
tagged release's zip reports the version its tag says).

**Building from source instead:**
```bash
./gradlew installPlugin   # builds and unpacks into ~/.nextflow/plugins/nf-spawn-<version>/
```

## Configuration

```groovy
// nextflow.config
plugins {
    id 'nf-spawn@X.Y.Z'   // match the version you installed above
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

### Pooled execution for wide fan-out (`spawn.pool`)

By default nf-spawn launches **one ephemeral instance per task**. That's ideal for
a handful of long tasks, but a wide scatter (hundreds of short tasks — the common
"N samples × one tool" bioinformatics pattern) is limited by *launch rate*: every
task pays a full instance boot, so short tasks self-terminate faster than new ones
launch and concurrency never reaches N.

**Pool mode** instead provisions a fixed set of **fungible worker instances** once
per run that pull tasks from a shared queue and reuse across jobs — per-task cost
drops to stage + run, with no per-task boot. Opt in via `nextflow.config`:

```groovy
spawn {
    pool {
        enabled      = true          // default false → one instance per task
        workers      = 100           // pool size to request (best-effort)
        minViable    = 1             // proceed with at least this many; fewer ⇒ lower parallelism, not failure
        instanceType = 'c7i.large'   // worker type (the pool is homogeneous)
        spot         = false
        idleTimeout  = '5m'          // a worker drains itself after this long idle
        ttl          = '4h'          // per-worker TTL backstop
    }
}
```

- **Best-effort / never hangs:** the pool asks for `workers` but proceeds once
  `minViable` are up — fewer workers just means lower parallelism.
- **Scale to zero:** workers self-terminate on `idleTimeout`; the pool's queue is
  deleted when the run ends, and spawn's reaper backstops any missed teardown.
- **Homogeneous:** all workers share `instanceType`, so per-process
  `ext.instanceType` is **not** honored in pool mode — use it for a run whose wide
  scatter is uniform (turn it off, or run per-task mode, for heterogeneous sizing).
  *other* buckets, grant them at pool creation via `spawn pool create --s3-read
  <bucket>` / `--s3-write <bucket>` (repeatable) so the worker IAM profile can
  reach them.
- **Resilient workers:** each worker runs its pull-loop under a restart-on-error
  supervisor — a transient failure re-execs rather than stranding an idle
  instance, while a clean idle-drain still terminates it (scale-to-zero).
- **Requires spawn ≥ v0.97.0** (the `spawn pool` command with the scoped worker
  IAM, `--s3-read`/`--s3-write`, and resilient workers).

### Per-process `ext` options

| Option | Default | Description |
|--------|---------|-------------|
| `ext.instanceType` | `t3.medium` | EC2 instance type for the task |
| `ext.region` | `us-east-1` | AWS region |
| `ext.az` | _(spawn picks)_ | Pin the task to an availability zone (`--az`). Set this to the AZ where a snapshot's [Fast Snapshot Restore](#delivering-reference-data) is enabled — FSR is per-AZ, so a volume restored in another AZ lazy-loads from S3 instead |
| `ext.ttl` | `2h` | Max instance lifetime (safety backstop) |
| `ext.spot` | `false` | Launch as a Spot instance |
| `ext.ami` | _(auto)_ | Explicit AMI ID; omit to let spawn auto-detect a stock AMI |
| `ext.volumeSize` | _(AMI min)_ | Extra root EBS size in GiB beyond the AMI minimum |
| `ext.volumes` | _(none)_ | List of `[snapshot:, mount:, readOnly:]` maps — attach EBS data volumes from snapshots (read-only by default), bind-mounted into the task container. Works for both direct reads and staged `path` inputs (symlinked, zero-copy) — see [Delivering reference data](#delivering-reference-data). Requires spawn ≥ 0.46.0 |
| `ext.fsx` | _(none)_ | Mount a **shared FSx for Lustre** filesystem by id (`--fsx-id`). `'fs-0abc'` (mount defaults `/fsx`) or `[ id:, mount:, paths: ]`. For one reference DB read by a **wide fan-out** — all tasks mount the same FS (no FSR credit cliff). Declared `paths` (e.g. `['kraken2']` → `/fsx/kraken2`) symlink zero-copy like `ext.volumes`. Pre-create the FS (`spawn fsx create`); the create-per-task form is not supported. Requires spawn ≥ 0.46.0 |
| `ext.efs` | _(none)_ | Mount a **shared EFS** filesystem by id (`--efs-id`), same shape as `ext.fsx` (mount defaults `/efs`). For shared reference data where EFS's elasticity fits better than Lustre |
| `ext.ensureDocker` | `true` | Auto-install + start Docker on the task instance when a `container` is set (idempotent), so a stock AMI works. Set `false` if your AMI already has Docker |
| `ext.packages` | _(none)_ | Host packages to `dnf install` before the task — a list (`['pigz','ethtool']`) or a space/comma string. For tools the task calls on the instance |
| `ext.setup` | _(none)_ | Arbitrary shell command run on the instance before the task (after Docker/packages) |

> With `ext.volumes` + the setup hooks above, nf-spawn runs on a **stock AL2023
> AMI** — no custom AMI needed. For wide fan-out, a small pre-baked tools AMI
> avoids the per-task install latency.

### Delivering reference data

Large reference data (a Kraken2/MetaPhlAn DB, BLAST index) reaches a task three
ways. Pick by fan-out width: **`ext.volumes`** (an attached read-only snapshot,
zero-copy) is simplest at small scale; a shared **`ext.fsx` / `ext.efs`**
filesystem is the answer for wide fan-out where FSR credits run out; an `s3://`
`db_path` is the per-task-download fallback.

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

  > **⚠️ Don't let `db_path` resolve to a *head-local* path foreign to the work
  > dir.** If the head mounts the DB at `/opt/databases/x` **and** `db_path`
  > points at that local path, Nextflow's FilePorter treats it as a foreign file
  > and **bulk-copies the whole DB up to the (S3) work dir on the head, before any
  > task launches** — a 16–34 GB upload that can stall and **deadlock** the run
  > (the `db` channel sits `(queue) OPEN`, no tasks submit; nf-spawn#65). nf-spawn
  > can't prevent this — it does no head work; the copy is Nextflow's own
  > head-side staging. The mount on the head is only there to satisfy nf-core's
  > **existence check** — `db_path` itself should resolve to something the work
  > dir doesn't have to localize: a value that isn't a foreign local path (e.g.
  > the work-dir filesystem or a pre-staged `s3://` URI, option 2), or wire the
  > volume DB so it isn't routed through a head-localized `path` channel at all.
  > The instance-side symlink (above) only engages *after* a task is scheduled, so
  > it can't rescue a run that deadlocks during head-side staging.

**2. A shared `ext.fsx` / `ext.efs` filesystem — one copy, many readers (best for wide fan-out).**
`ext.volumes` (option 1) is the cheapest path at *small* fan-out, but it relies on
**Fast Snapshot Restore** to be fast on cold tasks — and FSR has a per-snapshot
credit bucket (~10 concurrently-warmed volumes). A 30–100-sample run creating ~50
volumes from one DB snapshot at once **drains the credits instantly**, and the
overflow volumes lazy-load from S3 at ~6–8 MB/s (classification crawls). For that
*one stable reference, many concurrent readers* shape, mount a **shared
filesystem** instead:

- Pre-create an FSx for Lustre FS once, S3-backed (lazy-import) so it holds the DB
  with no separate upload: `spawn fsx create …` (or out of band). FSx returns an
  `fs-xxx` id.
- `ext.fsx = 'fs-xxx'` — every task mounts the **same** filesystem read-only at
  `/fsx` (no per-volume credit limit, no copy). Use the map form to place DBs and
  declare which resolve zero-copy: `ext.fsx = [ id: 'fs-xxx', mount: '/fsx', paths: ['kraken2','metaphlan'] ]`.
- A declared `path` input whose stage name matches a `paths` entry (e.g. input
  `kraken2` ↔ `/fsx/kraken2`) is **symlinked, not copied** — the same #55
  zero-copy short-circuit `ext.volumes` gets, so a pipeline that *stages*
  `db_path` still reads it off the mount. The head-node existence-check and
  foreign-`db_path` caveats above apply identically.
- `ext.efs` is the same by id (`fs-xxx`, mount `/efs`) when EFS's elasticity suits
  better than Lustre's throughput.

Only the existing-filesystem (**id**) form is supported. nf-spawn launches one
instance per task, so a `--fsx-create` per task would create one filesystem *per
task* across the fan-out — exactly the multiplication a shared FS is meant to
avoid. Create it once, reference it by id.

**3. An `s3://` `db_path` — download-per-task fallback.**
If you can't mount on the head (or don't want to), point `db_path` at an `s3://`
URI: nf-spawn's declared-input localization (#37) `aws s3 cp`s it onto each task,
and an `s3://` URI also skips nf-core's head-side existence check. The trade-off
is that **every task downloads the full DB (16–34 GB)** — wasteful for anything
beyond small fan-out. Prefer option 1 (small fan-out) or option 2 (wide fan-out).

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
