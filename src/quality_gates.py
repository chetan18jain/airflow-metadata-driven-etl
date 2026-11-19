"""Pre/post load quality gates, driven by the same table config.

Pre-load gates check the source extract exists and is non-empty;
post-load gates check the Delta target: minimum row count and no
nulls in the configured key columns. The DAG wires these as tasks
around the load task, so a failed gate stops that table's chain
without blocking other tables.

Runnable standalone:
  python src/quality_gates.py --config config/tables.json --phase post
"""
import argparse, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from incremental_load import get_spark, load_config, read_source
from pyspark.sql import functions as F

class QualityGateError(Exception):
    pass

def pre_load_check(spark, cfg: dict) -> None:
    if not os.path.exists(cfg["source_path"]):
        raise QualityGateError(f"{cfg['name']}: source missing: {cfg['source_path']}")
    rows = read_source(spark, cfg).count()
    if rows < cfg.get("min_rows", 1):
        raise QualityGateError(f"{cfg['name']}: source rows {rows} < min {cfg.get('min_rows', 1)}")
    print(f"[gate:pre] {cfg['name']} OK ({rows} source rows)")

def post_load_check(spark, cfg: dict) -> None:
    target = spark.read.format("delta").load(cfg["target_path"])
    rows = target.count()
    if rows < cfg.get("min_rows", 1):
        raise QualityGateError(f"{cfg['name']}: target rows {rows} < min {cfg.get('min_rows', 1)}")
    for col in cfg.get("not_null_columns", []):
        nulls = target.filter(F.col(col).isNull()).count()
        if nulls:
            raise QualityGateError(f"{cfg['name']}: {nulls} nulls in {col}")
    print(f"[gate:post] {cfg['name']} OK ({rows} target rows)")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/tables.json")
    parser.add_argument("--phase", choices=["pre", "post"], default="post")
    parser.add_argument("--table", default=None)
    args = parser.parse_args()
    spark = get_spark("metadata-quality-gates")
    check = pre_load_check if args.phase == "pre" else post_load_check
    for cfg in load_config(args.config):
        if args.table and cfg["name"] != args.table:
            continue
        check(spark, cfg)
    spark.stop()

if __name__ == "__main__":
    main()
