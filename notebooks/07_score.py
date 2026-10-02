# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Batch scoring
# MAGIC Loads the `champion` model (or the latest version if none is promoted yet),
# MAGIC scores the most recent hour of `tfl.gold.line_features`, and merges the results
# MAGIC into `tfl.gold.predictions`.
# MAGIC
# MAGIC Predictions are **kept as history** (not overwritten), so later we can compare
# MAGIC what we predicted against what actually happened.

# COMMAND ----------

from datetime import datetime, timezone

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

MODEL_NAME = "tfl.gold.delay_model"
PRED_TABLE = "tfl.gold.predictions"
THRESHOLD = 0.5

CATEGORICAL = ["line_id", "mode"]
NUMERIC = [
    "delay_level", "is_disrupted", "prev_delay_level", "has_planned_work",
    "disrupted_buckets_2h", "max_delay_level_2h",
    "hour_local", "day_of_week", "is_weekend", "is_peak",
    "temp_c", "precip_mm", "rain_mm", "wind_kmh", "weather_code",
]
FEATURES = CATEGORICAL + NUMERIC

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md ## 1. Load the model

# COMMAND ----------

client = MlflowClient()
try:
    version = client.get_model_version_by_alias(MODEL_NAME, "champion").version
    source = "champion"
except Exception:
    version = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
    source = "latest (no champion yet)"

model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version}")
print(f"Using version {version} - {source}")

# COMMAND ----------

# MAGIC %md ## 2. Get the rows to score (latest hour)

# COMMAND ----------

features = spark.read.table("tfl.gold.line_features")
latest = features.agg(F.max("time_bucket")).first()[0]

to_score = (
    features
    .where(F.col("time_bucket") > F.lit(latest) - F.expr("INTERVAL 1 HOUR"))
    .select("line_id", "line_name", "mode", "time_bucket", *[c for c in NUMERIC])
    .toPandas()
)
to_score[NUMERIC] = to_score[NUMERIC].astype("float64")
print(f"Scoring {len(to_score)} rows | latest slot: {latest}")

# COMMAND ----------

# MAGIC %md ## 3. Predict

# COMMAND ----------

to_score["disruption_prob"] = model.predict_proba(to_score[FEATURES])[:, 1]
to_score["predicted_disrupted"] = (to_score["disruption_prob"] >= THRESHOLD).astype(int)
to_score["model_version"] = int(version)
to_score["scored_at"] = datetime.now(timezone.utc)

preds = (
    spark.createDataFrame(
        to_score[["line_id", "line_name", "mode", "time_bucket", "delay_level",
                  "disruption_prob", "predicted_disrupted", "model_version", "scored_at"]]
    )
    .withColumnRenamed("delay_level", "current_delay_level")
    .withColumn("predicted_for", F.col("time_bucket") + F.expr("INTERVAL 1 HOUR"))
)

display(preds.orderBy(F.desc("disruption_prob")))

# COMMAND ----------

# MAGIC %md ## 4. Merge into the predictions table
# MAGIC Keyed on line + slot, so re-running the notebook updates rather than duplicates.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PRED_TABLE} (
  line_id STRING, line_name STRING, mode STRING,
  time_bucket TIMESTAMP, predicted_for TIMESTAMP,
  current_delay_level DOUBLE, disruption_prob DOUBLE, predicted_disrupted INT,
  model_version INT, scored_at TIMESTAMP
)
COMMENT 'Hourly 1-hour-ahead disruption predictions per line. Kept as history for monitoring.'
""")

preds.createOrReplaceTempView("new_preds")
spark.sql(f"""
MERGE INTO {PRED_TABLE} t
USING new_preds s
ON t.line_id = s.line_id AND t.time_bucket = s.time_bucket
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"{PRED_TABLE} now has {spark.table(PRED_TABLE).count():,} rows")
