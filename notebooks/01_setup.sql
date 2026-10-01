-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Unity Catalog setup
-- MAGIC One catalog, one schema per medallion layer, and a Volume for raw landed files.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS tfl;
CREATE SCHEMA IF NOT EXISTS tfl.bronze;
CREATE SCHEMA IF NOT EXISTS tfl.silver;
CREATE SCHEMA IF NOT EXISTS tfl.gold;
CREATE VOLUME IF NOT EXISTS tfl.bronze.landing;
