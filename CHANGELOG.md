# Changelog

All notable changes to **nf-spawn** (the Nextflow `spawn` executor plugin) are
documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.0] - 2026-07-31

### Added
- **Pooled execution mode for wide fan-out — `spawn.pool` (#70).** Opt-in
  (`spawn.pool.enabled = true`), off by default. Instead of launching one
  ephemeral instance per task, nf-spawn provisions a fixed set of **fungible
  worker instances** once per run that drain a shared queue, reusing across jobs —
  so short tasks stop paying a full instance boot each and the fan-out isn't
  capped by launch rate (the root cause measured in #70: ~60/108 concurrent,
  5m44s to dispatch). A new run-scoped lifecycle observer
  (`SpawnPoolObserver`, a Nextflow `TraceObserverV2`) runs `spawn pool create`
  at run start and `spawn pool drain` at run end; `submit()` enqueues each task
  via `spawn pool submit` (S3 stage + one SQS send — no launch) rather than
  `spawn task run`. Completion detection is unchanged — a pooled worker writes the
  same `.exitcode`/`completion.json` to the S3 work dir. Config:
  `spawn.pool { enabled; workers; minViable; instanceType; spot; idleTimeout; ttl }`.
  Provisioning is **best-effort** (asks for `workers`, proceeds with `minViable` —
  fewer workers means lower parallelism, never a failed run), workers self-drain
  on idle-timeout (scale to zero), and the queue is deleted at run end. Pool mode
  is **homogeneous** (one worker `instanceType`), so per-process `ext.instanceType`
  heterogeneity is not honored in pool mode — best for the wide, uniform scatter
  (N samples × one tool). **Requires spawn ≥ v0.97.0** (the `spawn pool` command
  with scoped worker IAM + resilient workers). Design:
  `docs/pool-integration-design.md`.

### Changed
- **Task dispatch is now non-blocking, so wide fan-outs launch in parallel
  (#70).** `submit()` previously blocked until `spawn task run` returned, which
  serialized every launch on Nextflow's single task-monitor thread — a 108-task
  fan-out took ~5m44s just to dispatch and never exceeded ~60 concurrent
  instances. `submit()` now starts `spawn task run` and returns immediately; a
  daemon thread drains its output (so its pipe can't fill and wedge) and reaps
  the exit code, and `checkIfRunning()` picks up the result on the next poll.
  Launch round-trips now overlap. A launch *failure* is surfaced from
  `checkIfRunning()` (which fails the task exactly as before) rather than from
  `submit()`. This is the first, no-new-infrastructure step toward #70; the
  deeper per-task instance-boot cost is addressed by the pooled-worker design
  (spawn `docs/pooled-task-execution-design.md`).

## [0.9.0] - 2026-07-19

### Changed
- **nf-spawn now dispatches each task through `spawn task run`** instead of calling
  `spawn launch` itself (spawn#386 adapter migration). `submit()` builds a spawn
  **TaskSpec** and runs `spawn task run` (detached); `checkIfRunning()` polls
  `spawn task status --check-complete`; completion is still read from the durable
  `.exitcode` in the S3 work dir. spawn now owns the launch, sizing, the durable
  completion record, self-termination, and a **scoped least-privilege instance
  profile** — the work bucket is declared in the TaskSpec's `resources.s3_read_write`
  so the profile grants exactly the S3 access the staging script's
  `aws s3 sync`/`cp` need. nf-spawn keeps its own staging script (input
  localization #37, the zero-copy symlink short-circuits #55/#49, and its own
  `docker run`) — that becomes the TaskSpec `command`, so all the nf-core staging
  behavior is preserved. `ext.instanceType` maps to `resources.instance_type` (an
  **exact** pin, spawn#413), and `ext.ami`/`ext.az`/`ext.volumes`/`ext.fsx`/`ext.efs`
  map onto the TaskSpec `placement` block. **Requires spawn ≥ 0.85.0.**
- `killTask()` now terminates by task id via `spawn terminate <task-id>`.

### Security
- **Validate the constrained `ext.*` directives before launch** (#59). `ext.instanceType`,
  `ext.region`, `ext.az`, `ext.ttl`, and `ext.ami` now fail fast with a clear error
  if malformed (they have known narrow shapes and flow into the `spawn launch`
  argv), instead of passing garbage to the CLI or the instance. NOTE: `ext.setup`,
  `ext.command`, `ext.packages`, `ext.container`, and `ext.runOptions` are
  deliberately **not** validated — they are arbitrary strings that run *as root*
  on the instance by design (the same trust model as `ext.args`/`script` on any
  Nextflow executor); this is documented in the code as a root-RCE surface.
- **Work dir is only made world-writable (`chmod 0777`) for containerized tasks** (#59).
  A bare-OS task runs as the dir's own owner and no longer gets a world-writable
  work dir; the relaxation stays only for the container case (where a non-root
  image user must write into the root-staged dir).
- The staging temp file (which contains the full task script) is now deleted
  after `spawn launch` reads it, instead of accumulating in the system temp dir
  (#59).
- **Pinned all GitHub Actions to commit SHAs** (with version comments) across
  the CI/security/release workflows, and pinned `trivy-action` from `@master`
  to a release. Clears the Semgrep `github-actions-mutable-action-tag` finding
  and hardens the CI supply chain.

### Changed
- CI now runs the Spock test suite (`./gradlew test`), which was previously never
  executed — the build only ran `assemble` (#59).
- Dropped a redundant `ProcessBuilder.environment().putAll(System.getenv())` in
  task submission; ProcessBuilder already inherits the parent environment (#59).
- Internal: `ext.volumes` is now parsed once into a typed `VolumeSpec` list that
  the three views (`--attach-volume` args, docker `-v` flags, mount paths) derive
  from, instead of three separate traversals with divergent error handling (#59).

### Fixed
- The zero-copy symlink substitution for an `ext.volumes`-backed reference DB is
  now logged (stderr) when it fires (#59). The match is by basename, so an input
  whose stage-name basename coincidentally equals a mount's is symlinked onto the
  volume and its copy skipped — announcing each substitution makes a wrong match
  diagnosable instead of silently serving the wrong data.
- A failed output up-sync no longer reports as a successful task (#59). The
  staging script wrote `.exitcode` before syncing outputs to S3, so a sync
  failure still left the task's own (often 0) exit code — Nextflow then
  finalized the task as successful with missing outputs. The `.exitcode` is now
  written after the sync, and a sync failure flips a success code to non-zero.
- nf-spawn now fails fast with a clear message when the work directory is unset
  or not an `s3://` URI, instead of launching an instance whose task hangs in
  RUNNING because the work dir can't be synced (#59).
- Task cancellation now actually terminates the instance (#58). `killTask` ran
  `spawn cancel <name>`, but `cancel` operates on parameter sweeps — it never
  destroyed the per-task instance, which then billed until its TTL (default 2h).
  It now runs `spawn terminate <name> -y`, scopes the subprocess to the task's
  region via `AWS_REGION` (so it can't target the wrong region and miss the
  non-globally-unique `nf-<hash>` instance), and logs a non-zero exit at error
  level as a cost incident instead of discarding it.

### Documentation
- README install/config snippets now reference `nf-spawn@0.8.0` (the current
  release) instead of the stale `@0.2.0`.

## [0.8.0] - 2026-06-16

### Added
- `ext.fsx` / `ext.efs` directives: mount a **shared reference filesystem** (FSx
  for Lustre or EFS) into a task, forwarded to `spawn launch --fsx-id` /
  `--efs-id` (+ `--fsx-mount-point` / `--efs-mount-point`) (#67). This is the
  right primitive for *one stable reference DB read by a wide fan-out* (e.g.
  nf-core/taxprofiler's Kraken2/MetaPhlAn DBs over 30–100 samples): one copy, N
  concurrent read-only mounts, **no per-volume Fast-Snapshot-Restore credit
  cliff** that `ext.volumes` hits at scale. Minimal form `ext.fsx = 'fs-0abc'`
  (mount defaults to `/fsx`); map form `[ id:, mount:, paths: ]`. The mount is
  bind-mounted read-only into the task container, and a declared `path` input
  whose stage name matches a declared `paths` entry (e.g. `paths: ['kraken2']` →
  `/fsx/kraken2`) is symlinked zero-copy — the same #55 short-circuit
  `ext.volumes` gets, so a pipeline that *stages* its `db_path` still reads it off
  the mount. Only the existing-filesystem (**id**) form is supported: nf-spawn
  launches one instance per task, so a per-task `--fsx-create` would create one
  filesystem per task — pre-create a shared FS once (`spawn fsx create`) and
  reference it by id. Requires spawn ≥ 0.46.0 (FSx/EFS mount-by-id).

### Documentation
- `ext.volumes` reference-data guide now warns that `db_path` must not resolve to
  a head-local path foreign to the work dir: Nextflow's FilePorter would bulk-copy
  the whole DB to the (S3) work dir on the head before any task launches and can
  deadlock the run (nf-spawn#65). nf-spawn does no head work, so this is resolved
  in pipeline config, not the plugin.

## [0.7.0] - 2026-06-15

### Added
- `ext.az` directive: pin a task's instance to a specific availability zone,
  forwarded to `spawn launch --az <zone>` (#62). Needed for **Fast Snapshot
  Restore**, which is per-AZ — a volume created from an FSR-warmed snapshot is
  only fast-restored in the AZ where FSR is enabled; elsewhere it lazy-loads
  blocks from S3 (~6–8 MB/s), e.g. ~40 min to load a 16 GB Kraken2 DB. Passed
  only when set, so the default preserves spawn's own placement.

### Security
- Semgrep SAST is now **enforcing** in CI (`--config=auto --error`) rather than
  report-only (#368). The scan (Groovy) was already clean — no findings to triage.

## [0.6.0] - 2026-06-14

### Fixed
- `ext.volumes` is now genuinely zero-copy for pipelines that **stage** their
  `path` DB input (e.g. nf-core/taxprofiler copies `db_path` into the Nextflow
  S3 work area). Previously, 0.5.0 only symlinked an input whose *source URI* was
  the local mount — but a staged input's source is Nextflow's own S3 stage copy,
  never the mount, so nf-spawn fell back to `aws s3 cp` and **downloaded the DB
  per task** (the volume was bind-mounted but unused). A 16 GB DB happened to
  copy fine; a 36 GB one truncated and the tool failed. nf-spawn now matches a
  declared input's **stage-name basename** against the attached `ext.volumes`
  mounts and, on a match, **symlinks the stage name → the mount and skips the
  copy** regardless of the reported source URI. So a volume-backed reference DB
  (Kraken2, MetaPhlAn, …) is read straight off the read-only volume — no
  per-task download — even when the pipeline stages `db_path` (#55).

### Added
- `ext.volumes` reference data now works with **staged `path` inputs** (the
  nf-core `db_path` pattern, e.g. taxprofiler), not just data a process reads
  directly from the mount — with **zero copy and zero per-task download** (#51,
  follow-up to #49):
  - A declared `path` input whose source is a **local absolute path** that exists
    on the task (i.e. an `ext.volumes` mount) is now **symlinked** into the work
    dir under the stage name the task script references — instead of being
    skipped. This is the spawn equivalent of Nextflow's `stageInMode=symlink` on
    a shared filesystem, so a tool that does `find -L <stage_name>` resolves
    straight to the read-only volume.
  - Each `ext.volumes` mount path is now **bind-mounted into the task container**
    (read-only when the volume is), so a containerized tool can actually read the
    DB off the mount.
  Combined with attaching the same read-only snapshot on the **head node** (so
  the pipeline's head-side `exists` validation passes), an unmodified nf-core
  pipeline can be fed a volume-backed DB — no pipeline fork, no per-task download.

### Documentation
- Added a **Delivering reference data** section to the README covering the two
  models (`ext.volumes` for mount-backed DBs incl. staged `path` inputs, and an
  `s3://` `db_path` for the download-per-task fallback) and the head-node-mount
  step that satisfies head-side schema validation (#49, #51).

## [0.4.0] - 2026-06-13

### Added
- Per-task setup hook so a **stock AL2023 AMI** runs containerized tasks without
  baking Docker/tools into a custom AMI (#47):
  - **Docker is auto-ensured** when a process has a `container` directive —
    nf-spawn installs and starts Docker on the task instance if it isn't already
    present (idempotent; a pre-baked AMI with Docker pays nothing). Opt out with
    `ext.ensureDocker = false` if you maintain your own tools AMI.
  - **`ext.packages = ['pigz', 'ethtool']`** installs host tools the task calls
    directly on the instance (via `dnf`). Accepts a list or a space/comma string.
  - **`ext.setup = '<shell>'`** runs an arbitrary bootstrap command before the
    task (runs after the Docker/packages steps).
  Combined with `ext.volumes` (0.3.0), the full **stock AMI + DB-on-EBS +
  tools-at-launch** setup is now reachable — no custom AMI required. (For wide
  fan-out, a small pre-baked tools AMI is still the lower-latency option.)

## [0.3.0] - 2026-06-13

### Added
- New `ext.volumes` process directive attaches pre-populated EBS data volumes
  from snapshots to a task instance, each mounted at a path (read-only by
  default). For example, mounting a Kraken2 reference DB without baking it into a
  custom AMI:
  ```groovy
  process {
      withName: 'KRAKEN2_KRAKEN2' {
          ext.instanceType = 'r7g.2xlarge'
          ext.volumes = [[ snapshot: 'snap-0abc', mount: '/opt/databases/kraken2', readOnly: true ]]
      }
  }
  ```
  Each entry maps to a `spawn launch --attach-volume snap-xxx:/mount[:ro|:rw]`.
  `readOnly` defaults to `true`; a single map (one volume) is also accepted.
  Requires spawn ≥ 0.46.0 (#45, depends on spawn#144).

## [0.2.12] - 2026-06-13

### Fixed
- The task script is now written into `.command.sh` flush-left (common leading
  indentation stripped), the way Nextflow itself writes it. Previously the
  source indentation was preserved, so an nf-core module's space-indented
  `<<-END_VERSIONS` heredoc terminator stayed indented — and `<<-` strips leading
  tabs only, never spaces — so the heredoc swallowed its own terminator and
  produced a malformed `versions.yml`, which aborted the whole Nextflow session
  with a SnakeYAML `while scanning a simple key` error. Essentially every nf-core
  module (they all emit `versions.yml` this way) was affected (#43).

## [0.2.11] - 2026-06-13

### Fixed
- Declared `s3://` inputs are now actually copied onto the instance. Two bugs in
  the generated stage-in commands meant the copy silently never happened: the
  source URI was rendered with an empty bucket authority (`s3:///bucket/key`,
  three slashes) so `aws s3 cp` parsed an empty bucket, and the destination
  single-quoted the literal `${LOCAL_DIR}` so it was passed to `aws` unexpanded
  (the file would have landed in a directory literally named `${LOCAL_DIR}`).
  The URI is now repaired to `s3://bucket/key` and `${LOCAL_DIR}` is left outside
  the quotes so the shell expands it (#41).
- A failed input copy now fails the staging script loud (`|| exit 1`) instead of
  being silently ignored under `set -uo pipefail`, which previously let the task
  run with a missing input, "succeed" with no output, and then fail downstream
  with a confusing `MissingFileException` (#41).

## [0.2.10] - 2026-06-13

### Fixed
- Containerized tasks now honor `docker.runOptions` and the per-process
  `containerOptions` directive (e.g. `--user root`), which were previously
  dropped — so a pipeline that relies on them to run the container as a writable
  user is respected again. The task work dir is also made world-writable
  (`chmod 0777`) before the container runs, so a non-root image user can write
  its outputs into the bind-mounted dir instead of failing with "Permission
  denied" (#39).

## [0.2.9] - 2026-06-13

### Fixed
- Task instances now localize each declared `path` input from its real source
  URI before running the task, instead of only syncing the task's own S3 work
  dir. Inputs produced by an upstream process or coming from a samplesheet /
  channel (`s3://`) were never placed on the instance, so stock nf-core modules
  ran with missing inputs; each declared input is now `aws s3 cp`'d (recursively
  for directories) to the stage name `.command.sh` references (#37).
- Directory inputs are detected via `Files.isDirectory` (with a trailing-slash
  fallback) rather than the trailing slash alone, so an `s3://` directory input
  whose URI has no trailing slash is still copied `--recursive` (PR #38 review).

## [0.2.8]

Baseline. Earlier history is in the
[GitHub Releases](https://github.com/spore-host/nf-spawn/releases) and the
[commit log](https://github.com/spore-host/nf-spawn/commits/main).

---

[Unreleased]: https://github.com/spore-host/nf-spawn/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/spore-host/nf-spawn/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/spore-host/nf-spawn/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/spore-host/nf-spawn/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/spore-host/nf-spawn/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/spore-host/nf-spawn/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/spore-host/nf-spawn/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/spore-host/nf-spawn/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/spore-host/nf-spawn/compare/v0.2.12...v0.3.0
[0.2.12]: https://github.com/spore-host/nf-spawn/compare/v0.2.11...v0.2.12
[0.2.11]: https://github.com/spore-host/nf-spawn/compare/v0.2.10...v0.2.11
[0.2.10]: https://github.com/spore-host/nf-spawn/compare/v0.2.9...v0.2.10
[0.2.9]: https://github.com/spore-host/nf-spawn/compare/v0.2.8...v0.2.9
[0.2.8]: https://github.com/spore-host/nf-spawn/releases/tag/v0.2.8
