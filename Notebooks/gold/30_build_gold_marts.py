# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Gold: Curated Clinical & Operational Aggregates
# MAGIC Builds patient compliance summary, AHI trend, and device fleet health tables from Silver.

# COMMAND ----------
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"])
env = dbutils.widgets.get("env")

from sleepapnea_dp.transformation.gold_transform import (
    build_device_fleet_health,
    build_patient_ahi_trend,
    build_patient_compliance_summary,
)
from sleepapnea_dp.utils.config_loader import load_config
from sleepapnea_dp.utils.spark_session import get_logger

logger = get_logger("gold_transform")
cfg = load_config(env=env, config_root="/Workspace/Repos/<your-repo-checkout>/config")

silver_sessions_table = dbutils.jobs.taskValues.get(
    taskKey="silver_transform",
    key="silver_sessions_table",
    default=f"{cfg.catalog}.{cfg['unity_catalog']['silver_schema']}.device_sessions",
)
silver_sessions = spark.table(silver_sessions_table)
gold_schema = cfg["unity_catalog"]["gold_schema"]

# COMMAND ----------
compliance = build_patient_compliance_summary(silver_sessions)
compliance.write.format("delta").mode("overwrite").saveAsTable(
    f"{cfg.catalog}.{gold_schema}.patient_compliance_summary"
)

# COMMAND ----------
ahi_trend = build_patient_ahi_trend(silver_sessions)
ahi_trend.write.format("delta").mode("overwrite").saveAsTable(
    f"{cfg.catalog}.{gold_schema}.patient_ahi_trend"
)

# COMMAND ----------
fleet_health = build_device_fleet_health(silver_sessions)
fleet_health.write.format("delta").mode("overwrite").saveAsTable(
    f"{cfg.catalog}.{gold_schema}.device_fleet_health"
)

logger.info("Gold layer build complete.")

