# Security policy

## Reporting a vulnerability

Please **do not** open a public issue for a security problem.

Report it privately through GitHub: go to this repository's **Security** tab and choose
**[Report a vulnerability](https://github.com/spore-host/nf-spawn/security/advisories/new)**.
The report opens a private advisory that only the maintainers can see, and we can
work on the fix there with you before anything is published.

Please include what you found, how to reproduce it, the version or commit, and
the impact you expect. If the issue involves another spore.host project, report it
in the repository where the code lives; we will move it if needed.

## Supported versions

nf-spawn is pre-1.0 (`0.x`). Security fixes go into the **latest release** only; please
reproduce on it before reporting.

## Scope

This repository's code, its release artifacts, and its CI/release workflows. For the
spore.host suite's security model (credentials, IAM, data flow), see the
[spore.host security overview](https://github.com/spore-host/spore-host/blob/main/SECURITY.md).
