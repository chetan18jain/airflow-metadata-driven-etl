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
from pyspark.sql.window import Window

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

def dedupe_batch(batch, cfg):
    """Dedupe a batch on the primary keys, keeping the LATEST row per key.

    A batch can legitimately carry the same key twice (e.g. an order
    updated twice between extracts). Delta MERGE fails on that with a
    multiple-source-match error, and plain dropDuplicates() keeps an
    arbitrary row, which can resurrect a stale version. Rank rows
    within each key by the watermark column descending and keep rank 1,
    so the newest version of each record always wins. The helper
    column is dropped before the batch reaches the merge.
    """
    keys = cfg["primary_keys"]
    watermark = cfg["watermark_column"]
    latest_first = Window.partitionBy(*keys).orderBy(
        F.col(watermark).desc())
    return (batch
            .withColumn("_rn", F.row_number().over(latest_first))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

def merge_into_target(spark, batch, cfg: dict, batch_count: int) -> None:
    """MERGE a batch into the Delta target; batch_count is the row count
    already computed (once, by the caller) so no count() action runs here.

    The caller persists the batch before counting, so the MERGE reuses
    the cached plan instead of re-scanning the source and re-running
    the dedupe window.
    """
    target_path = cfg["target_path"]
    keys = cfg["primary_keys"]
    if not DeltaTable.isDeltaTable(spark, target_path):
        batch.write.format("delta").save(target_path)
        print(f"{cfg['name']}: initial load, {batch_count} rows")
        return
    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (target.alias("t")
     .merge(batch.alias("s"), condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    print(f"{cfg['name']}: merged {batch_count} rows")

def load_table(spark, cfg: dict) -> None:
    source = read_source(spark, cfg)
    if cfg["load_type"] == "full":
        # Persist, count once, then write from the cache — the old code
        # called source.count() after the write, a second full scan.
        source = source.persist()
        try:
            row_count = source.count()
            (source.write.format("delta").mode("overwrite")
             .save(cfg["target_path"]))
        finally:
            source.unpersist()
        print(f"{cfg['name']}: full overwrite, {row_count} rows")
        return
    if cfg["load_type"] != "incremental":
        # Unreachable when the config passed validate_tables(); kept so a
        # programmatically built cfg can never silently pick a strategy.
        raise ValueError(
            f"{cfg['name']}: unknown load_type {cfg['load_type']!r}")
    watermark = current_watermark(spark, cfg)
    batch = source.filter(F.col(cfg["watermark_column"]) > F.lit(watermark))
    batch = dedupe_batch(batch, cfg)
    # Persist, then count ONCE: the old code ran batch.isEmpty() and then
    # batch.count() inside merge_into_target — two extra full actions
    # over the deduped batch. The MERGE now reads from the cache.
    batch = batch.persist()
    try:
        batch_count = batch.count()
        if batch_count == 0:
            print(f"{cfg['name']}: no rows after watermark {watermark}")
            return
        merge_into_target(spark, batch, cfg, batch_count)
    finally:
        batch.unpersist()

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
