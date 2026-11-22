"""Pre/post load quality gates, driven by the same table config.

Pre-load gates check the source extract exists and is non-empty;
Post-load gates check the Delta target with a SINGLE aggregation
(row count + one null count per key column, one Spark action) and
report every failing column together, instead of one filter/count
per column that stopped at the first failure.

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
    cols = cfg.get("not_null_columns", [])
    # ONE aggregation, ONE action: row count + a null count per key
    # column. The old code ran target.count() plus one
    # filter(col.isNull()).count() per column — N+1 full scans — and
    # raised on the FIRST failing column, hiding the rest. Now every
    # failing column is reported together in a single error.
    agg_exprs = [F.count("*").alias("__rows")]
    agg_exprs += [F.sum(F.col(c).isNull().cast("int")).alias(c)
                  for c in cols]
    result = target.agg(*agg_exprs).first()
    rows = result["__rows"]
    failures = []
    if rows < cfg.get("min_rows", 1):
        failures.append(f"target rows {rows} < min {cfg.get('min_rows', 1)}")
    for col in cols:
        nulls = result[col]
        if nulls:
            failures.append(f"{nulls} nulls in {col}")
    if failures:
        raise QualityGateError(f"{cfg['name']}: " + "; ".join(failures))
    print(f"[gate:post] {cfg['name']} OK ({rows} target rows)")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/tables.json")
    parser.add_argument("--phase", choices=["pre", "post"], default="post")
    parser.add_argument("--table", default=None)
    args = parser.parse_args()
    spark = get_spark("metadata-quality-gates")
    try:
        check = pre_load_check if args.phase == "pre" else post_load_check
        for cfg in load_config(args.config):
            if args.table and cfg["name"] != args.table:
                continue
            check(spark, cfg)
    finally:
        # A failed gate used to leave the session running (spark.stop()
        # sat after the loop); the finally guarantees it stops even
        # when a gate raises QualityGateError.
        spark.stop()

if __name__ == "__main__":
    main()
