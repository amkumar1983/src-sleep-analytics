# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Bronze: Raw Device Session Ingestion
# MAGIC Thin orchestration only — all logic lives in `sleepapnea_dp.ingestion.bronze_loader`.
# MAGIC Reads new files from the ADLS `raw` container via Auto Loader and appends to the
# MAGIC Bronze Delta table, fully schema-enforced.

# COMMAND ----------
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"])
env = dbutils.widgets.get("env")

from sleepapnea_dp.ingestion.bronze_loader import (
    read_raw_device_sessions_stream,
    with_ingestion_metadata,
    write_bronze,
)
from sleepapnea_dp.utils.config_loader import load_config
from sleepapnea_dp.utils.spark_session import get_logger

logger = get_logger("bronze_ingestion")
cfg = load_config(env=env, config_root="/Workspace/Repos/<your-repo-checkout>/config")

# COMMAND ----------
raw_path = cfg.container_path("raw")
schema_location = cfg.container_path("schema_inference")
bronze_table = f"{cfg.catalog}.{cfg['unity_catalog']['bronze_schema']}.device_sessions"
checkpoint = cfg.container_path("checkpoints") + "bronze_device_sessions/"

logger.info(f"Reading raw device sessions from {raw_path}")

raw_stream = read_raw_device_sessions_stream(spark, raw_path, schema_location)
enriched = with_ingestion_metadata(raw_stream)

query = write_bronze(
    enriched,
    bronze_table=bronze_table,
    checkpoint_location=checkpoint,
    trigger_once=(cfg["autoloader"]["trigger"] == "availableNow"),
)
query.awaitTermination()

logger.info(f"Bronze write complete -> {bronze_table}")

# COMMAND ----------
row_count = spark.table(bronze_table).count()
dbutils.jobs.taskValues.set(key="bronze_row_count", value=row_count)
print(f"Bronze table {bronze_table} now has {row_count} total rows")

