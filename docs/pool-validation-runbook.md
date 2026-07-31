# Pooled execution — paid validation runbook (#70 Phase 2 acceptance)

**Status:** ready to run **once explicitly authorized** — this launches real,
billable EC2. Cost control is existential: every step below sets a TTL, terminates
explicitly, and independently leak-checks. Do **not** run without a go-ahead.

This is the Phase-2 acceptance test the issue's evidence sets up: prove that pool
mode fixes the dispatch-bound fan-out measured on the real N=108 run.

## Prerequisites

- A **spawn release that includes `spawn pool`** (v0.96.0+), installed and on
  `PATH`. Verify: `spawn pool --help` prints the create/submit/status/drain
  subcommands.
- nf-spawn ≥ the release carrying #76 (pool mode). Verify the plugin id in
  `nextflow.config` matches.
- AWS creds for the launch account (the same account/region the N=108 run used).
  Recommended: `spore-host-dev` profile / a dedicated test account.
- An S3 work bucket the tasks and specs can reach.

## The workload

Reuse the issue's repro: **108 `CALL_VARIANTS` tasks** (one bcftools chr20 call
per genome), the same pipeline that produced the baseline (~60/108 peak, 5m44s to
dispatch). Any wide, uniform single-tool scatter works; chr20 bcftools is ideal
because per-task work is ~0.6s, so the launcher — not the workload — is what's
under test.

## Config (pool mode ON)

```groovy
// nextflow.config
plugins { id 'nf-spawn@<release-with-#76>' }

spawn {
    region = 'us-east-1'
    pool {
        enabled      = true
        workers      = 108          // ask for one worker per task (best-effort)
        minViable    = 20           // proceed once ≥20 are up — never hang the run
        instanceType = 'c7i.large'  // homogeneous pool; sized for the bcftools step
        spot         = true         // cheaper; reclamation is handled (redelivery)
        idleTimeout  = '3m'         // workers drain quickly once the queue empties
        ttl          = '1h'         // hard per-worker backstop (well above the run)
    }
}
process { executor = 'spawn' }
workDir = 's3://<bucket>/pool-validation-work'
```

## Run + measure

```bash
# Stamp a start time and run.
date -u +%FT%TZ
nextflow run <pipeline> -c nextflow.config -with-trace trace.tsv -with-report report.html

# While it runs, in another shell — watch the pool fill and the queue drain:
watch -n5 'spawn pool status --run-id nf-$(nextflow log -q | tail -1)  # or read the run UUID from the nextflow log'
watch -n5 'spawn list --tag spawn:role=pool-worker --state running -o json | jq length'
```

Capture from `trace.tsv` after the run:
- **peak concurrency** — max simultaneous `RUNNING` tasks (or peak live workers).
- **dispatch time** — first `submit` → last `submit` timestamp spread.
- **wall clock** — flow start → complete.

## Success criteria (vs the baseline)

| Metric | Baseline (per-task) | Pool-mode target |
|---|---|---|
| Peak concurrency | ~60 / 108 | → **N** (near 108, bounded by `workers`/capacity) |
| Time to dispatch all tasks | 5m44s | → **seconds** (enqueue is S3 stage + SQS send) |
| Cost after run | (per-task instances self-terminate) | **$0** — pool drained, no live workers |

A pass is: peak materially above ~60 (ideally →108), dispatch collapsed to
seconds, and a clean $0 leak-check.

## Teardown + independent leak-check (MANDATORY)

`onFlowComplete` deletes the queue and workers idle-drain, but **verify
independently** — never trust the happy path with real money:

```bash
RUN=nf-<run-uuid>   # from the nextflow log / pool status

# 1. Any workers still alive for this run?
spawn list --tag spawn:pool-run-id=$RUN --state running
spawn list --tag spawn:role=pool-worker --state running

# 2. If ANY remain, terminate them explicitly (do not wait on idle-timeout).
#    `spawn terminate` targets an instance id/name (or a job-array), NOT a tag, so
#    list the run's workers by tag and terminate each by id (-y skips the confirm
#    we can't answer over a pipe):
for id in $(spawn list --tag spawn:pool-run-id=$RUN --state running -o json | jq -r '.[].instance_id'); do
  spawn terminate "$id" -y
done

# 3. The queue should be gone:
spawn pool status --run-id $RUN    # expect: queue not found / empty

# 4. Broad backstop — any orphaned spawn instances at all in the region:
spawn orphans --region us-east-1
```

Only consider the validation complete when (2)–(4) show **zero live workers and no
queue**. Record the leak-check output alongside the metrics.

## If it underperforms

- **Peak still capped well below N:** check the pool actually reached `workers`
  (`spawn pool status` / worker count during the run). If capacity-limited, the
  `PartialCohort` degraded — lower `workers` or try on-demand; note it, it's
  best-effort by design, not a bug.
- **Dispatch still slow:** confirm `submit()` took the pool branch (logs:
  "enqueued task … to pool"), not the per-task path — i.e. `spawn.pool.enabled`
  was actually read.
- **Tasks stuck RUNNING forever:** a worker isn't writing `completion.json` — check
  a worker's `spored pool-worker` logs (SSM) and that the results bucket
  (`spawn-results-<acct>-<region>`) is writable by the worker's scoped role.

## After a passing run

- Post the measured before/after numbers on nf-spawn#70 and close the phase.
- Consider adding the case study to the adoption docs (real numbers, not projections).
