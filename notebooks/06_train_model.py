# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Train the delay model
# MAGIC Predicts **is_disrupted_next_1h**: will this line be disrupted an hour from now?
# MAGIC
# MAGIC 1. Load labelled rows from `tfl.gold.line_features`
# MAGIC 2. Split by **time** (train on the past, test on the most recent slice) - no leakage
# MAGIC 3. Baseline: "it'll stay the way it is now"
# MAGIC 4. Gradient boosting model, logged to MLflow
# MAGIC 5. Register in Unity Catalog; set the `champion` alias only if it beats the baseline

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

MODEL_NAME = "tfl.gold.delay_model"
TARGET = "is_disrupted_next_1h"
TEST_FRACTION = 0.2

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

# MAGIC %md ## 1. Load labelled data

# COMMAND ----------

df = (
    spark.read.table("tfl.gold.line_features")
    .where(f"{TARGET} IS NOT NULL")
    .select("time_bucket", *FEATURES, TARGET)
    .toPandas()
    .sort_values("time_bucket")
)

# Integers can't hold missing values; floats can. Prevents schema errors at scoring time.
df[NUMERIC] = df[NUMERIC].astype("float64")

print(f"{len(df):,} labelled rows | {df['time_bucket'].nunique()} time slots "
      f"| {df['time_bucket'].min()} -> {df['time_bucket'].max()}")
print(f"Disrupted next hour: {df[TARGET].mean():.1%}")

# COMMAND ----------

# MAGIC %md ## 2. Time-based split

# COMMAND ----------

slots = np.sort(df["time_bucket"].unique())
cutoff = slots[int(len(slots) * (1 - TEST_FRACTION))]

train = df[df["time_bucket"] < cutoff]
test = df[df["time_bucket"] >= cutoff]

X_train, y_train = train[FEATURES], train[TARGET].astype(int)
X_test, y_test = test[FEATURES], test[TARGET].astype(int)

print(f"Train: {len(train):,} rows (before {cutoff}) | Test: {len(test):,} rows")

if y_train.nunique() < 2:
    raise ValueError(
        "Training data only has one class so far - let the ingestion job collect more data "
        "(ideally a few days that include some disruptions) and re-run."
    )

# COMMAND ----------

# MAGIC %md ## 3. Metrics + baseline

# COMMAND ----------

def evaluate(y_true, y_pred, y_score=None, prefix=""):
    m = {
        f"{prefix}f1": f1_score(y_true, y_pred, zero_division=0),
        f"{prefix}precision": precision_score(y_true, y_pred, zero_division=0),
        f"{prefix}recall": recall_score(y_true, y_pred, zero_division=0),
    }
    if y_score is not None and y_true.nunique() == 2:
        m[f"{prefix}roc_auc"] = roc_auc_score(y_true, y_score)
        m[f"{prefix}pr_auc"] = average_precision_score(y_true, y_score)
    return m


# Persistence baseline: whatever the line is doing now, it'll still be doing in an hour
baseline_pred = X_test["is_disrupted"].fillna(0).astype(int)
baseline = evaluate(y_test, baseline_pred, prefix="baseline_")
baseline

# COMMAND ----------

# MAGIC %md ## 4. Train + log to MLflow

# COMMAND ----------

preprocess = ColumnTransformer(
    [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CATEGORICAL)],
    remainder="passthrough",  # numerics pass straight through; the model handles NaNs natively
)

params = {
    "learning_rate": 0.05,
    "max_iter": 300,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 20,
    "class_weight": "balanced",  # disruptions are the minority class
    "random_state": 42,
}

model = Pipeline([
    ("prep", preprocess),
    ("clf", HistGradientBoostingClassifier(
        categorical_features=list(range(len(CATEGORICAL))), **params)),
])

with mlflow.start_run(run_name="hist_gbm") as run:
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    metrics = evaluate(y_test, pred, proba)

    mlflow.log_params({**params, "test_fraction": TEST_FRACTION, "cutoff": str(cutoff),
                       "n_train": len(train), "n_test": len(test)})
    mlflow.log_metrics({**metrics, **baseline})

    beats_baseline = metrics["f1"] > baseline["baseline_f1"]
    mlflow.set_tag("beats_baseline", str(beats_baseline))

    mlflow.sklearn.log_model(
        model,
        name="model",
        signature=infer_signature(X_train, proba),
        input_example=X_train.head(5),
        registered_model_name=MODEL_NAME,
    )

print("Model:   ", {k: round(v, 3) for k, v in metrics.items()})
print("Baseline:", {k: round(v, 3) for k, v in baseline.items()})
print("Beats baseline:", beats_baseline)

# COMMAND ----------

# MAGIC %md ## 5. Promote to `champion` if it earned it

# COMMAND ----------

client = MlflowClient()
latest = max(client.search_model_versions(f"name='{MODEL_NAME}'"), key=lambda v: int(v.version))

if beats_baseline:
    client.set_registered_model_alias(MODEL_NAME, "champion", latest.version)
    print(f"Version {latest.version} is now 'champion'")
else:
    print(f"Version {latest.version} registered but NOT promoted - it doesn't beat the baseline yet.")
