# nf-spawn pooled execution — integration design (Phase 2 of #70)

**Status: IMPLEMENTED** (merged, ships in **nf-spawn v0.10.0**; requires **spawn ≥
v0.97.0**). Retained as design rationale, written in the original proposal tense.
The `spawn.pool { … }` config, the `SpawnPoolObserver` run-scoped lifecycle hook,
and the pool-mode `submit()` branch all shipped as described. For current usage
see the nf-spawn README pool section.
**Tracks:** nf-spawn#70 (dispatch-bound fan-out). Built on the spawn-side pool
(spawn#456, `spawn pool` CLI + `spored pool-worker`). Shares the
run-scoped lifecycle hook with nf-spawn#69 (per-run ephemeral FSx).

---

## 1. What Phase 1 gives us, and the gap Phase 2 closes

Phase 1 (spawn#456) built the whole pooled-execution engine in spawn: a
run-scoped SQS queue, fungible workers provisioned as a cohort (`pkg/taskcohort`,
best-effort `PartialCohort`), a worker pull-loop (`spored pool-worker`) that runs
each job via the taskproto protocol in a clean workspace and self-drains on idle,
and a submitter CLI:

```
spawn pool create --run-id R --workers N --instance-type T   # provision + queue
spawn pool submit --run-id R --spec f.json                    # stage + enqueue one task
spawn pool status --run-id R                                  # queue depth
spawn pool drain  --run-id R                                  # delete the queue
```

What Phase 1 does **not** do: nothing tells nf-spawn to *use* the pool. nf-spawn
today launches one `spawn task run` per `submit()`. Phase 2 wires nf-spawn to,
when pool mode is enabled: provision the pool once per run, `submit()` by
**enqueuing** instead of launching, and drain the pool when the run ends.

The blocker the design doc (spawn#454 §6) flagged — "nf-spawn has no run-level
lifecycle hook" — is **resolved**: Nextflow's plugin API exposes exactly the hook
we need (§3).

---

## 2. Confirmed Nextflow API facts (26.04.3, verified against the shipped jar)

- **`nextflow.trace.TraceObserverFactoryV2`** is a pf4j `ExtensionPoint`:
  `Collection<TraceObserverV2> create(Session)`. nf-spawn already registers a pf4j
  extension (`SpawnExecutor` via `@Extension` + `nextflowPlugin.extensionPoints`),
  so adding an observer factory is the same mechanism — add the class to
  `extensionPoints` in `build.gradle`.
- **`TraceObserverV2`** is a Groovy **trait with no-op defaults for every method**,
  so an observer overrides only what it needs:
  - `onFlowCreate(Session)` — run start → **provision the pool + create the queue**.
  - `onFlowComplete()` — run end (success or failure) → **drain the pool**.
- **`Session.getUniqueId()`** returns a stable per-run `UUID`. Both the observer
  (`onFlowCreate(session)`) and the task handler (`executor.getSession()`) can read
  it, so **both sides derive the same run id with no shared mutable state** (§4).
- **`Session.getConfig()`** is the config `Map` — the pool opt-in and knobs live
  under a `spawn.pool` block read from here.
- **`Executor.getSession()`** is available to the handler (the executor holds the
  session), so `SpawnTaskHandler` can reach `uniqueId` + config in `submit()`.

(V2 vs V1: both exist; V2 is the current event-typed interface. We use V2. If a
future Nextflow drops V2 we fall back to V1 — the two lifecycle methods we use
have identical signatures on both.)

---

## 3. Architecture

```
  nextflow.config:  spawn.pool { enabled = true; workers = 100; instanceType = 'c7i.large' }

  onFlowCreate(session) ──▶ SpawnPoolObserver:
                              runID = poolRunID(session.uniqueId)
                              `spawn pool create --run-id R --workers N --instance-type T ...`
  submit(task) ───────────▶ SpawnTaskHandler (pool mode):
                              build TaskSpec (as today) → write spec.json
                              `spawn pool submit --run-id R --spec spec.json`   (enqueue, no launch)
  checkIfCompleted() ─────▶ UNCHANGED: poll <workDir>/.exitcode in S3
  onFlowComplete() ───────▶ SpawnPoolObserver:
                              `spawn pool drain --run-id R`   (delete queue; workers idle-drain to $0)
```

### The JVM↔Go boundary: shell out to `spawn pool` (decided)
nf-spawn is JVM; the pool lives in the Go `spawn` binary. nf-spawn drives it by
**shelling out to the `spawn pool` CLI** — the same mechanism it already uses for
`spawn task run` / `spawn terminate`. No new surface, and the CLI is exactly what
Phase 1 shipped and tested. (Rejected: a bespoke high-rate enqueue path — premature
until a measured enqueue bottleneck; `submit()` is already off the monitor's hot
path after the Phase-0 non-blocking change.)

### Run id: derived, not shared
`poolRunID(session.uniqueId)` = a deterministic slug of the session UUID (SQS
names allow `[A-Za-z0-9_-]{1,80}`; a UUID with dashes fits). The observer creates
the queue under this name; every `submit()` resolves the same name. No static
mutable state shared between the observer and N concurrent handlers — the run id is
a pure function of the session both already hold. This sidesteps the Groovy
static-state and ordering hazards that shared objects would introduce.

---

## 4. `submit()` in pool mode

Pool mode is a branch at the top of `submit()`, gated on `spawn.pool.enabled`:

1. Build the `TaskSpec` exactly as today (same staging script, same `ext.*`
   placement mapping) and write it to the temp spec file.
2. Instead of `spawn task run` (launch), run
   `spawn pool submit --run-id <R> --spec <specfile>` — a fast enqueue (S3 stage +
   one SQS SendMessage; no instance launch). This inherits the Phase-0
   non-blocking pattern: fire it, don't block the monitor thread.
3. Leave status `SUBMITTED`; `checkIfRunning()`/`checkIfCompleted()` are
   **unchanged** — a pooled worker writes the identical `completion.json` + S3
   `.exitcode` the handler already polls. This is the key reuse: the completion
   contract is pool-agnostic.

`killTask()` in pool mode: the per-task instance isn't ours to terminate (workers
are shared). Kill removes the task from the queue if still pending (a future
`spawn pool cancel`), else lets the worker finish; the reaper + idle-drain bound
cost regardless. **Open item:** Phase 1 has no `spawn pool cancel`; for now
`killTask()` in pool mode is a no-op with a logged warning (the task's own
`errorStrategy`/TTL still applies). Adding queue-level cancel is a small Phase-1
follow-up if needed.

---

## 5. Failure & drain semantics (never hang, never leak)

- **Best-effort provisioning:** `spawn pool create` uses a `PartialCohort` — asks
  for N workers, proceeds with `min-viable`. A short pool means lower parallelism,
  not a failed run. `onFlowCreate` failing to reach min-viable is a hard run error
  (surfaced before any task submits), not a silent hang.
- **Drain is unconditional:** `onFlowComplete` runs on both success and failure, so
  the queue is deleted either way. Workers self-terminate on idle-timeout
  independently; the tag-based reaper (`spawn:pool-run-id`/`spawn:role=pool-worker`)
  backstops any worker that outlives its idle window. Three independent teardown
  paths → cost can't leak (existential invariant).
- **Crash safety:** if the Nextflow head dies mid-run (no `onFlowComplete`), the
  queue's `MessageRetentionPeriod` expires it and workers idle-drain — nothing
  runs forever. This mirrors #69's reaper-backstop reasoning.
- **Spot reclamation:** a reclaimed worker's in-flight task redelivers via SQS
  visibility-timeout to another worker; Nextflow's `errorStrategy` covers the task
  layer. (Phase-1 semantics, unchanged.)

---

## 6. Config schema (opt-in; default = today's per-task behavior)

```groovy
spawn {
  pool {
    enabled       = true          // default false → one-instance-per-task (today)
    workers       = 100           // pool size to request (best-effort)
    minViable     = 1             // accept ≥ this many; fewer ⇒ lower parallelism
    instanceType  = 'c7i.large'   // worker type (pool is homogeneous)
    spot          = false
    idleTimeout   = '5m'          // worker drains after this idle
    ttl           = '4h'          // per-worker TTL backstop
  }
}
```

Pool mode is homogeneous (one worker type), so per-process `ext.instanceType`
heterogeneity is **not** honored in pool mode — a documented limitation. A pipeline
with wildly heterogeneous per-process sizing should either not use pool mode or use
it only for its wide, uniform scatter (the common bioinformatics case: N samples ×
one tool). This is called out because it's the one behavior that differs from
per-task mode.

---

## 7. Testing plan

- **Spock unit tests (no AWS):** pool-mode `submit()` builds the right
  `spawn pool submit` argv (spec written, run-id derived from a stub session);
  observer builds the right `create`/`drain` argv from config; run-id derivation
  is stable for a fixed UUID and SQS-name-safe. These mirror the existing
  `buildTaskRunCommand`/`buildTaskSpec` argv tests — pure, fast, `@CompileStatic`.
- **The engine-composition CI** (spore-host#397 / `project_engine_composition_ci`)
  currently defers nf-spawn (JVM/gradle weight). Pool mode doesn't change that
  calculus; the Spock argv tests are the CI gate, as for the rest of nf-spawn.
- **Paid validation (separate, explicitly-authorized step — NOT this session):**
  the real N=108 chr20 run with `spawn.pool.enabled`, TTL set, explicit
  post-run terminate, and an independent leak-check. Success criteria: peak
  concurrency → N (vs ~60 today), dispatch time → seconds (vs 5m44s), and $0 after
  drain. This is the Phase-2 acceptance test the issue's evidence sets up.

---

## 8. Scope / non-goals

- **In scope:** the observer, the pool-mode `submit()` branch, config plumbing,
  run-id derivation, Spock tests, docs. A reviewable nf-spawn PR that stops short
  of the paid run.
- **Not in scope this phase:** the paid N=108 validation (separate authorized
  step); `spawn pool cancel` (Phase-1 follow-up if `killTask` precision is needed);
  heterogeneous pools; the #69 ephemeral-FSx use of the same hook (tracked
  separately, but the observer is structured so #69 can hang off the same
  `onFlowCreate`/`onFlowComplete`).
- **Requires spawn ≥ v0.97.0** (the `spawn pool` CLI with scoped worker IAM +
  resilient workers). The nf-spawn CHANGELOG pins this minimum.
