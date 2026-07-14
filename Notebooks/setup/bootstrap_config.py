# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Bootstraps environment config for all downstream notebooks in this job.
# MAGIC Run as the first task in every Databricks Workflow (Bronze, Silver, Gold, Quality).

# COMMAND ----------
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"])
env = dbutils.widgets.get("env")

# COMMAND ----------
# MAGIC %pip install /Workspace/Shared/libs/sleepapnea_dp-0.1.0-py3-none-any.whl
# MAGIC # In practice this wheel is installed as a cluster/job library via the Databricks Asset
# MAGIC # Bundle (infra/databricks/databricks.yml), not pip-installed inline. Left here for
# MAGIC # ad hoc interactive development convenience.

# COMMAND ----------
from sleepapnea_dp.utils.config_loader import load_config

cfg = load_config(env=env, config_root="/Workspace/Repos/<your-repo-checkout>/config")
print(f"Loaded config for env={cfg.env}, catalog={cfg.catalog}")

# COMMAND ----------
spark.sql(f"USE CATALOG {cfg.catalog}")
for schema in [
    cfg["unity_catalog"]["bronze_schema"],
    cfg["unity_catalog"]["silver_schema"],
    cfg["unity_catalog"]["gold_schema"],
    cfg["unity_catalog"]["identity_vault_schema"],
    "quarantine",
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# COMMAND ----------
# Pass config forward to downstream notebook tasks in the same Job via task values
dbutils.jobs.taskValues.set(key="env", value=env)


