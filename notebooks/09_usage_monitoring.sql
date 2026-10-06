-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 09 - Usage monitoring
-- MAGIC Free Edition has daily fair-usage quotas but no "quota remaining" page,
-- MAGIC so this tracks what the project actually uses, from the billing system table.
-- MAGIC If `system.billing` isn't accessible, fall back to job run durations in Jobs & Pipelines -> Runs.

-- COMMAND ----------

-- MAGIC %md ## Daily usage by product (last 7 days)

-- COMMAND ----------

SELECT usage_date,
       billing_origin_product,
       ROUND(SUM(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY ALL
ORDER BY usage_date DESC, dbus DESC

-- COMMAND ----------

-- MAGIC %md ## Biggest consumers: which job or app (last 7 days)

-- COMMAND ----------

SELECT usage_metadata.job_id,
       usage_metadata.app_name,
       billing_origin_product,
       ROUND(SUM(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY ALL
ORDER BY dbus DESC
