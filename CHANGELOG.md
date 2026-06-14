# Changelog

All notable changes to **nf-spawn** (the Nextflow `spawn` executor plugin) are
documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/spore-host/nf-spawn/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/spore-host/nf-spawn/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/spore-host/nf-spawn/compare/v0.2.12...v0.3.0
[0.2.12]: https://github.com/spore-host/nf-spawn/compare/v0.2.11...v0.2.12
[0.2.11]: https://github.com/spore-host/nf-spawn/compare/v0.2.10...v0.2.11
[0.2.10]: https://github.com/spore-host/nf-spawn/compare/v0.2.9...v0.2.10
[0.2.9]: https://github.com/spore-host/nf-spawn/compare/v0.2.8...v0.2.9
[0.2.8]: https://github.com/spore-host/nf-spawn/releases/tag/v0.2.8
