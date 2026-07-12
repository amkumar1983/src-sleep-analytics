# Databricks notebook source
# MAGIC %md
# MAGIC # 40 - Quality Gate: Bronze -> Quarantine split
# MAGIC Runs before Silver. Splits Bronze records into clean vs. quarantined based on
# MAGIC `sleepapnea_dp.quality.validators`. Fails the job if the quarantine rate exceeds
# MAGIC the configured threshold, rather than silently letting bad clinical data flow downstream.

# COMMAND ----------
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"])
env = dbutils.widgets.get("env")

from sleepapnea_dp.quality.validators import DEVICE_SESSION_RULES, split_clean_and_quarantine
from sleepapnea_dp.utils.config_loader import load_config
from sleepapnea_dp.utils.spark_session import get_logger

logger = get_logger("quality_gate")
cfg = load_config(env=env, config_root="/Workspace/Repos/<your-repo-checkout>/config")

bronze_table = f"{cfg.catalog}.{cfg['unity_catalog']['bronze_schema']}.device_sessions"
quarantine_table = f"{cfg.catalog}.{cfg['quality']['quarantine_table']}"

# COMMAND ----------
bronze_df = spark.table(bronze_table)
clean_df, quarantined_df = split_clean_and_quarantine(bronze_df, DEVICE_SESSION_RULES)

total = bronze_df.count()
bad = quarantined_df.count()
quarantine_pct = bad / total if total else 0.0

(quarantined_df.write.format("delta").mode("append").saveAsTable(quarantine_table))

logger.info(f"Quality gate: {bad}/{total} rows quarantined ({quarantine_pct:.2%})")

threshold = cfg["quality"]["fail_pipeline_on_quarantine_pct_above"]
if quarantine_pct > threshold:
    raise RuntimeError(
        f"Quarantine rate {quarantine_pct:.2%} exceeds threshold {threshold:.2%} — "
        f"failing pipeline. Check {quarantine_table} for rejected records."
    )

# COMMAND ----------
# Persist clean rows to a Delta table (not a temp view) since downstream Silver runs as a
# separate notebook task/job step and temp views do not survive across task boundaries.
bronze_clean_table = f"{cfg.catalog}.{cfg['unity_catalog']['bronze_schema']}.device_sessions_clean"
clean_df.write.format("delta").mode("overwrite").saveAsTable(bronze_clean_table)

dbutils.jobs.taskValues.set(key="clean_row_count", value=clean_df.count())
dbutils.jobs.taskValues.set(key="quarantined_row_count", value=bad)
dbutils.jobs.taskValues.set(key="bronze_clean_table", value=bronze_clean_table)

