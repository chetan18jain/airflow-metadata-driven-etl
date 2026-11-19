# Airflow Metadata-Driven ETL Framework

A personal portfolio project: a small ETL framework where **tables are
onboarded by configuration, not code**. One Airflow DAG reads
`config/tables.json` and generates a pre-gate -> load -> post-gate task
chain per table; incremental loads use a watermark + Delta `MERGE`.

## How it works

```
config/tables.json                 dags/metadata_etl_dag.py
{ name, source, target,      --->  for each table, generate:
  watermark_column,                  pre_load_gate  (source present, non-empty)
  primary_keys, load_type }          load           (src/incremental_load.py)
                                     post_load_gate (row count, null keys)
```

Each table config declares:

| Field              | Meaning |
|--------------------|---------|
| `source_path` / `target_path` | Extract location and Delta target. |
| `load_type`        | `incremental` (watermark + merge) or `full` (overwrite). |
| `watermark_column` | Column compared against the target max to find new/changed rows. |
| `primary_keys`     | Merge keys for the Delta `MERGE`. |
| `min_rows`, `not_null_columns` | Quality-gate expectations. |

## Add a table (config only)

Append an entry to `config/tables.json`, e.g. a `payments` table with
`watermark_column: "updated_at"` and `primary_keys: ["payment_id"]`.
On the next DAG parse, `payments_pre_load_gate >> payments_load >>
payments_post_load_gate` appears automatically. Nothing else changes.

## Incremental load semantics

`src/incremental_load.py`:

1. Reads the target's current `max(watermark_column)` (epoch for a new
   table).
2. Extracts source rows with a greater watermark.
3. `MERGE`s on the primary keys — matched rows update, new rows
   insert. Re-running the same batch is a no-op in effect, so loads
   are **idempotent** and safe to retry from Airflow.

## Run it locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Loader/gates without Airflow (needs source extracts at the paths
# in config/tables.json, CSV with header):
python src/incremental_load.py --config config/tables.json
python src/quality_gates.py   --config config/tables.json --phase post

# With Airflow:
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
airflow standalone            # then enable the metadata_driven_etl DAG
```

## Design notes

- **Why metadata-driven** — in real estates the same extract -> merge ->
  validate pattern repeats for dozens of tables; config keeps the
  pattern reviewed once and makes onboarding a data change, not a
  code change.
- **Gate placement** — pre-load gates fail fast on missing/empty
  extracts before any Spark load starts; post-load gates verify the
  target actually changed as expected, catching silent empty merges.
- **Failure isolation** — chains are per table and independent; a bad
  `products` extract fails its own chain while `orders` still loads.
- **BashOperator by design** — loads run as `spark-submit` subprocesses
  so the scheduler stays light and the same loader runs unchanged from
  a shell, a DAG, or (with a different operator) ADF/Databricks.

## Layout

```
config/tables.json           table metadata (the framework input)
dags/metadata_etl_dag.py     DAG generated from the config
src/incremental_load.py      watermark extract + Delta MERGE loader
src/quality_gates.py         pre/post load checks
```
