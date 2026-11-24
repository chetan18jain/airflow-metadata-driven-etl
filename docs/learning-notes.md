# Learning notes — Airflow metadata-driven ETL

What this project taught me, file by file, in my own words. These notes
are the thinking behind the code — the design decisions, the traps, and
the one-line answers I want to be able to give in an interview.

## The one-sentence architecture

Airflow decides **when**, the loader decides **how** data moves, the
gates decide **whether** it's acceptable, and the metadata tells all
three **what**. That's the whole project.

## Metadata-driven: the pattern is code, the differences are data

The pattern from earlier projects — extract, cleanse/merge, validate —
repeats for every table in a real estate, so this project builds the
pattern **once** as a framework. Tables are onboarded by **config, not
code**: one JSON object, three consumers.

- The DAG reads only `name` (task ids + `--table`).
- The loader reads paths, format, `load_type`, watermark, keys.
- The gates read `source_path`, `min_rows`, `not_null_columns` (plus
  the shared `read_source`).

Add a JSON entry and a new task chain appears on the next DAG parse.
Zero new code. The three example tables prove the machinery is
table-agnostic: `orders` and `customers` are the identical incremental
shape with a different key column; `products` is the contrast — a full
load whose `watermark_column: null` is load-bearing documentation (the
full branch never touches it) and whose `primary_keys` are unused by the
overwrite but kept for schema uniformity.

JSON as the store has a clear line: readable, git-diffable, zero infra —
right for dozens of tables in one repo. Hundreds of tables, multiple
teams, or per-environment config earns a DB-backed registry instead.

## The DAG is a factory, not a pipeline

Airflow's scheduler **executes** `dags/metadata_etl_dag.py` repeatedly at
parse time, not just at run time. Top-level code runs on every parse:
`load_tables()` reads the JSON and a loop builds 3 `BashOperator`s per
table — 3 tables, 9 tasks, from ~30 lines. 50 tables would be 150
tasks, still ~30 lines.

Parse-time discipline follows from that: top-level code must be fast and
side-effect-light. A small local JSON read is fine; a DB query or API
call here would run on **every** parse. Heavy work belongs inside tasks.
Flip side: a missing or invalid config is a **parse error** — the DAG
vanishes from the UI with an import error. Config errors are DAG errors
in this design. That's why config validation runs at DAG parse time
(`src/config_validation.py`): a bad `load_type` must fail loudly at
parse, not silently route into the wrong branch at run time. The
original `== "full"` check meant **any** other string — a typo, a
trailing space — silently fell into the incremental branch.

- `@daily`, `start_date` 2026-01-01, `catchup=False`: missed days are
  not backfilled. That's safe **here** because the loader is stateful,
  not date-driven — the next single run still picks up all missed data
  through the watermark.
- Chains are independent — no cross-table edges — so failure isolation
  is structural, not coded. A bad `products` extract fails only its own
  chain.
- Retries (`retries=1`, 5-min delay) are only safe because the loader
  is idempotent. Retry policy and loader design are one decision, not
  two.
- `BashOperator` by design: loads run as `spark-submit` subprocesses,
  the scheduler stays light, and the same loader runs from shell, DAG,
  or anything — fully testable with zero Airflow. The contract is the
  **exit code**: a gate raises, the process exits non-zero, the task
  fails. The costs: no XCom/data passing between tasks and no Airflow
  connections management.
- Task ids come from table names, so duplicate names collide and shell
  characters in a name would break the f-string bash commands — config
  is trusted input, and validation is what makes that trust safe.

## The loader: the target table is its own checkpoint

`src/incremental_load.py` is the heart of the project.

1. **Current watermark** = `max(watermark_column)` read **from the
   target** Delta table. No state table, no checkpoint file, no Airflow
   Variable. Epoch (1970) when the target doesn't exist yet, so the
   first run takes everything through the incremental path.
2. **Batch** = source rows with watermark strictly greater than the
   current max.
3. **MERGE** on the primary keys: matched rows update, new rows insert.
   If the target doesn't exist, a plain initial write.

State derived from the destination can never drift out of sync with the
destination — and the successful MERGE itself advances the max, so the
checkpoint maintains itself. A crash before MERGE commits advances
nothing, so the same batch is re-extracted next run and nothing is
skipped.

### Idempotency — two layers

1. **Watermark layer:** a plain rerun extracts an empty batch; the
   `isEmpty` guard exits cleanly. A normal retry is a no-op.
2. **MERGE layer:** if the same batch *is* re-extracted (MERGE
   committed but the process died before Airflow recorded success), the
   MERGE on primary keys updates the same rows instead of appending
   copies.

Contrast with plain append (a retry duplicates) and full overwrite
(safe but rewrites everything). Earlier projects' `dropDuplicates`
protected only **within** a run; this protects **across** runs.

### The strict-`>` boundary flaw

The filter is `updated_at > max`, strictly. A late row stamped
**exactly** at the current max is never selected — not this run, not
any future run, because the watermark never moves backwards. Silent,
permanent loss, and common with second-granularity timestamps where
many rows share a second.

The fixes are behaviour decisions, not tidy-ups: `>=` plus MERGE
(boundary rows re-merge harmlessly every run, at the cost of
reprocessing however many rows share the max timestamp), or an
overlap/lookback window (reprocess the last hour/day — catches
genuinely late data at a constant reprocessing cost). The loader keeps
strict `>` deliberately and documents it; the daily practice exercise
for 2026-11-24 implements the overlap-window variant.

Two related traps fixed during the daily improvements:

- **Duplicate keys inside one batch crash a Delta MERGE**
  (multiple-source-match error). The loader now dedupes by primary key
  keeping the **latest** row per key — a window `row_number()` ordered
  by watermark descending — because plain `dropDuplicates` keeps an
  arbitrary row, not the latest.
- **The watermark comparison must be typed.** The watermark used to
  come back as a string and be compared via `F.lit(string)` against a
  timestamp column, working only through Spark's implicit cast. It is
  now cast to a timestamp literal.

### Costs that used to hide in the loader

- The batch is a lazy plan, never cached: `isEmpty` reads the source,
  MERGE reads it again, the print's `count()` reads it a third time.
  The loader now persists the batch, counts once, and passes the count
  into the merge prints.
- `whenMatchedUpdateAll` rewrites **all** source columns on a match —
  file churn even for unchanged rows.
- Deletes are **not** propagated: no `whenMatchedDelete`, because a
  source delete is invisible — the row simply never appears in a batch.
  That needs a delete feed, a soft-delete flag, or a full compare.
- `inferSchema` re-infers the source schema every run — a type drift in
  the CSV silently changes the batch schema; production pins a
  `StructType`.
- Full-load tables ignore all of this — overwrite every run. Right for
  a small `products` dimension, wrong for a big table.

## The gates: a process-level contract

`src/quality_gates.py` is one script with two checkpoints, switched by
`--phase`:

- **Pre:** source path exists + source count `>= min_rows` — fail fast
  **before** spending load compute.
- **Post:** target count `>= min_rows` + zero nulls in
  `not_null_columns`.

The post-gate exists because the loader can legitimately merge **zero**
rows on a quiet day and exit cleanly — a broken extract producing
nothing looks identical from the loader's side. Precision matters,
though: the post-gate checks the target's **total** count, so on a
target that already has rows it passes even if this run merged nothing.
It catches empty/missing targets, not no-op merges. A stronger gate
would verify rows merged this run or that the watermark advanced — that
needs state or loader reporting, which is why the simple version is a
defensible scope choice.

- **The failure contract:** checks raise `QualityGateError`;
  unhandled, the process exits non-zero, the `BashOperator` task fails,
  and that table's chain stops. Other tables are unaffected. A custom
  exception makes gate failures read differently in logs from genuine
  crashes, and messages carry table + actual vs expected numbers — in
  this design the Airflow log **is** the alerting system.
- **Reuse done right:** the gates import `get_spark`, `load_config`
  and `read_source` from the loader — **one** definition of how a
  source is read, shared by loader and pre-gate. An earlier project's
  local runner copied its transform and could drift; this can't. The
  cost is coupling (importing the loader pulls in `delta.tables`).
- **Single-pass null counts:** the post-gate used to run one `count()`
  action per not-null column. It now computes all null counts in one
  aggregation (`count(*)` + `sum(isNull)` per column) and reports **all**
  failing columns in one error instead of failing on the first.
- Expectations are metadata too: `min_rows` / `not_null_columns`
  come from `tables.json` with `.get` defaults (1 / `[]`) — tightening
  a gate is a config edit. `not_null_columns` includes `updated_at`
  itself for `orders`/`customers`: a null watermark value in the
  target would poison future watermark comparisons — this gate protects
  the **loader**, not just the data.
- Every gate invocation boots a full Spark session, even for one count
  — in DAG mode that's 2 extra Spark startups per table per day. The
  price of process-level independence.
- One suggested "improvement" was rejected: swapping `os.path.exists`
  for `os.path.isfile` in the source check. Spark CSV sources are
  commonly **directories** of part-files, not single files — `isfile`
  would fail valid directory sources. `exists` is the right check.

## The config contract — audited, not assumed

The contract is inconsistent about absence, and that's worth knowing:
`name` / `source_path` / `target_path` / `load_type` are hard `cfg[...]`
(missing = `KeyError`); `source_format` / `min_rows` /
`not_null_columns` default via `.get` (csv / 1 / `[]`);
`watermark_column` is only touched on the incremental path, so a full
table survives its absence and an incremental one crashes at **run**
time. Some config mistakes explode, others silently default — an
implicit contract, not a validated schema. The validation module added
on 2026-11-19 closes the worst of it, and a JSON Schema in CI is the
natural next step.

The onboarding claim, audited: **true** for the machinery — append an
entry and the parse generates the chain. **Not covered:** the source
extract must exist at `source_path` in the declared shape (provisioning
is upstream's job), the watermark must be a real monotonic-ish
timestamp, and nothing validates the entry before execution.

## Interview lines

- "The DAG is generated from metadata at parse time — onboarding a
  table is a config change, and retries are safe because the load
  underneath is idempotent."
- "The target table is its own checkpoint: read the max, load what's
  newer, MERGE on keys. Retries are free — and the price of the
  strict-greater filter is that rows stamped exactly at the watermark
  are lost unless you add an overlap."
- "A gate is a process-level contract: raise, exit non-zero, and
  Airflow stops exactly one table's chain. And the gate re-reads the
  target itself — it never takes the loader's word that the load
  landed."
