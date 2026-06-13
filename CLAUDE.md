# CLAUDE.md — nf-spawn

`nf-spawn` is a **Nextflow executor plugin** that runs pipeline tasks on
ephemeral EC2 instances via [spore-host/spawn](https://github.com/spore-host/spawn).
Part of the spore.host suite.

## Versioning & changelog (required)

This project follows **[Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)**
and keeps a **[Keep a Changelog](https://keepachangelog.com/en/1.1.0/)**-format
`CHANGELOG.md` at the repo root. (This is the spore.host-wide policy — every
repo in the suite.)

**Every change that affects users updates `CHANGELOG.md`** in the same PR:

- Add an entry under `## [Unreleased]`, in the right group — `Added`, `Changed`,
  `Deprecated`, `Removed`, `Fixed`, or `Security` (use `Documentation` for
  docs-only).
- Write for humans: the user-visible effect, not the implementation. Reference
  the issue/PR.

**On release:**

1. Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`; open a fresh empty
   `## [Unreleased]` above it; update the comparison links.
2. Choose `X.Y.Z` by SemVer: MAJOR = breaking, MINOR = backward-compatible
   feature, PATCH = backward-compatible fix. (Pre-1.0, breaking changes bump
   MINOR.)
3. **Bump `version` in `build.gradle` to match** — the release workflow verifies
   the tag equals `build.gradle`'s version and fails if they drift.
4. Tag `vX.Y.Z` → the Release workflow builds and publishes the plugin.

## Build & test

This is a Gradle / Nextflow-plugin project (needs JDK 17+; Nextflow 26.04.x).

- `./gradlew test` — run the Spock test suite
- `./gradlew compileGroovy` — compile
- `./gradlew assemble` — build the plugin zip

`@CompileStatic` is used throughout `SpawnTaskHandler`; `TaskConfig.ext` is an
untyped map, so access it via a `Map` cast + subscript (dotted access fails
static type checking).
