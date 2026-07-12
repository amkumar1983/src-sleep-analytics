# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Silver: Cleanse, De-duplicate, De-identify
# MAGIC Reads the quality-gated clean Bronze table, applies the Silver transform (dedupe,
# MAGIC type casting, pseudonymization), writes patient-key-only Silver tables plus an
# MAGIC append to the restricted identity vault table.

# COMMAND ----------
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"])
env = dbutils.widgets.get("env")

from sleepapnea_dp.transformation.identity_vault import build_identity_vault
from sleepapnea_dp.transformation.silver_transform import build_silver_sessions, explode_events, cast_timestamps
from sleepapnea_dp.utils.config_loader import load_config
from sleepapnea_dp.utils.spark_session import get_logger

logger = get_logger("silver_transform")
cfg = load_config(env=env, config_root="/Workspace/Repos/<your-repo-checkout>/config")

bronze_clean_table = dbutils.jobs.taskValues.get(
    taskKey="quality_gate",
    key="bronze_clean_table",
    default=f"{cfg.catalog}.{cfg['unity_catalog']['bronze_schema']}.device_sessions_clean",
)
salt = cfg.secret(dbutils, "pseudonymization_salt")
bronze_clean = spark.table(bronze_clean_table)

# COMMAND ----------
# MAGIC %md ### 1. Update the restricted identity vault first
# MAGIC This is the only table where patient_id (PHI) and patient_key co-exist. It lives in a
# MAGIC separate, tightly-permissioned Unity Catalog schema (`phi_restricted`). New patients
# MAGIC seen in this batch are merged in (insert-only — never overwritten/deleted here).

# COMMAND ----------
identity_vault_table = f"{cfg.catalog}.{cfg['unity_catalog']['identity_vault_schema']}.patient_identity_map"
batch_identities = build_identity_vault(bronze_clean, salt)

if spark.catalog.tableExists(identity_vault_table):
    batch_identities.createOrReplaceTempView("_identity_vault_batch")
    spark.sql(
        f"""
        MERGE INTO {identity_vault_table} t
        USING _identity_vault_batch s
        ON t.patient_key = s.patient_key
        WHEN NOT MATCHED THEN INSERT *
        """
    )
else:
    batch_identities.write.format("delta").saveAsTable(identity_vault_table)

logger.info(f"Identity vault updated -> {identity_vault_table}")

# COMMAND ----------
# MAGIC %md ### 2. Build de-identified Silver session table

# COMMAND ----------
silver_sessions = build_silver_sessions(bronze_clean, salt)
silver_sessions_table = f"{cfg.catalog}.{cfg['unity_catalog']['silver_schema']}.device_sessions"
silver_sessions.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(silver_sessions_table)
logger.info(f"Wrote Silver sessions -> {silver_sessions_table} ({silver_sessions.count()} rows)")

# COMMAND ----------
# MAGIC %md ### 3. Build de-identified Silver event-grain table

# COMMAND ----------
from sleepapnea_dp.utils.phi import pseudonymize_column
from pyspark.sql import functions as F

# explode_events() operates on raw event structs; we pseudonymize patient_id -> patient_key
# here directly so values match the session table's patient_key exactly (same salt, same hash).
events_with_key = explode_events(
    cast_timestamps(bronze_clean).withColumn("patient_key", pseudonymize_column(F.col("patient_id"), salt))
)

silver_events_table = f"{cfg.catalog}.{cfg['unity_catalog']['silver_schema']}.device_events"
events_with_key.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(silver_events_table)
logger.info(f"Wrote Silver events -> {silver_events_table} ({events_with_key.count()} rows)")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="silver_sessions_table", value=silver_sessions_table)
dbutils.jobs.taskValues.set(key="silver_events_table", value=silver_events_table)

