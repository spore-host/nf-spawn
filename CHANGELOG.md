# Changelog

All notable changes to **nf-spawn** (the Nextflow `spawn` executor plugin) are
documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Task instances now localize each declared `path` input from its real source
  URI before running the task, instead of only syncing the task's own S3 work
  dir. Inputs produced by an upstream process or coming from a samplesheet /
  channel (`s3://`) were never placed on the instance, so stock nf-core modules
  ran with missing inputs; each declared input is now `aws s3 cp`'d (recursively
  for directories) to the stage name `.command.sh` references (#37).

## [0.2.8]

Baseline. Earlier history is in the
[GitHub Releases](https://github.com/spore-host/nf-spawn/releases) and the
[commit log](https://github.com/spore-host/nf-spawn/commits/main).

---

[Unreleased]: https://github.com/spore-host/nf-spawn/compare/v0.2.8...HEAD
[0.2.8]: https://github.com/spore-host/nf-spawn/releases/tag/v0.2.8
