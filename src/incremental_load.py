"""Incremental load: watermark extract + Delta MERGE on primary keys.

For an incremental table the loader:
  1. reads the current target max(watermark) (epoch if table is new),
  2. extracts source rows with watermark > that value,
  3. MERGEs them into the target on the configured primary keys
     (update on match, insert otherwise) — idempotent on rerun.

Full-load tables are simply overwritten. This module is invoked by
the Airflow DAG (one call per table config) and is runnable standalone:

  python src/incremental_load.py --config config/tables.json --table orders
"""
import argparse, json
from config_validation import validate_tables
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EPOCH = "1970-01-01 00:00:00"

def get_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def read_source(spark, cfg: dict):
    reader = spark.read.format(cfg.get("source_format", "csv"))
    if cfg.get("source_format", "csv") == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    return reader.load(cfg["source_path"])

def current_watermark(spark, cfg: dict) -> str:
    target = cfg["target_path"]
    if not DeltaTable.isDeltaTable(spark, target):
        return EPOCH
    row = (spark.read.format("delta").load(target)
           .agg(F.max(cfg["watermark_column"]).alias("wm")).first())
    return str(row["wm"]) if row["wm"] is not None else EPOCH

def merge_into_target(spark, batch, cfg: dict) -> None:
    target_path = cfg["target_path"]
    keys = cfg["primary_keys"]
    if not DeltaTable.isDeltaTable(spark, target_path):
        batch.write.format("delta").save(target_path)
        print(f"{cfg['name']}: initial load, {batch.count()} rows")
        return
    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (target.alias("t")
     .merge(batch.alias("s"), condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    print(f"{cfg['name']}: merged {batch.count()} rows")

def load_table(spark, cfg: dict) -> None:
    source = read_source(spark, cfg)
    if cfg["load_type"] == "full":
        (source.write.format("delta").mode("overwrite")
         .save(cfg["target_path"]))
        print(f"{cfg['name']}: full overwrite, {source.count()} rows")
        return
    if cfg["load_type"] != "incremental":
        # Unreachable when the config passed validate_tables(); kept so a
        # programmatically built cfg can never silently pick a strategy.
        raise ValueError(
            f"{cfg['name']}: unknown load_type {cfg['load_type']!r}")
    watermark = current_watermark(spark, cfg)
    batch = source.filter(F.col(cfg["watermark_column"]) > F.lit(watermark))
    if batch.isEmpty():
        print(f"{cfg['name']}: no rows after watermark {watermark}")
        return
    merge_into_target(spark, batch, cfg)

def load_config(path: str) -> list:
    with open(path, encoding="utf-8") as fh:
        return validate_tables(json.load(fh)["tables"])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/tables.json")
    parser.add_argument("--table", default=None,
                        help="only this table (default: all)")
    args = parser.parse_args()
    spark = get_spark("metadata-incremental-load")
    for cfg in load_config(args.config):
        if args.table and cfg["name"] != args.table:
            continue
        load_table(spark, cfg)
    spark.stop()

if __name__ == "__main__":
    main()
