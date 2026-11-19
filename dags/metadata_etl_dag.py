"""Metadata-driven ETL DAG.

Reads config/tables.json and generates one task chain per table:

    pre_load_gate >> load_table >> post_load_gate

Adding a table to the JSON adds its chain on the next DAG parse —
no new DAG code. Tables are independent: one table failing its gate
does not block the others. Loads run the PySpark incremental loader
(watermark + Delta MERGE) as a subprocess so the DAG stays light.
"""
import json, os, sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_HOME = os.environ.get(
    "ETL_PROJECT_HOME",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
CONFIG_PATH = os.path.join(PROJECT_HOME, "config", "tables.json")

# Validate the metadata contract at parse time: a bad entry fails the
# parse loudly here instead of generating a task chain that only fails
# at run time. config_validation is pure stdlib, so this stays cheap.
sys.path.insert(0, os.path.join(PROJECT_HOME, "src"))
from config_validation import validate_tables

def load_tables() -> list:
    with open(CONFIG_PATH, encoding="utf-8") as fh:
        return validate_tables(json.load(fh)["tables"])

default_args = {
    "owner": "chetan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="metadata_driven_etl",
    description="Config-driven incremental ETL into Delta Lake",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "delta", "metadata-driven"],
) as dag:
    for cfg in load_tables():
        name = cfg["name"]
        pre = BashOperator(
            task_id=f"{name}_pre_load_gate",
            bash_command=(
                f"cd {PROJECT_HOME} && python src/quality_gates.py "
                f"--config config/tables.json --phase pre --table {name}"),
        )
        load = BashOperator(
            task_id=f"{name}_load",
            bash_command=(
                f"cd {PROJECT_HOME} && spark-submit src/incremental_load.py "
                f"--config config/tables.json --table {name}"),
        )
        post = BashOperator(
            task_id=f"{name}_post_load_gate",
            bash_command=(
                f"cd {PROJECT_HOME} && python src/quality_gates.py "
                f"--config config/tables.json --phase post --table {name}"),
        )
        pre >> load >> post
