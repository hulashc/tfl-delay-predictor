# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Export public snapshot
# MAGIC Writes the latest forecast as a small JSON file and pushes it to the `snapshot`
# MAGIC branch of the GitHub repo. hulash.com reads that file, so the forecast is public
# MAGIC without exposing the Databricks workspace or keeping anything running.

# COMMAND ----------

import base64
import json
from datetime import datetime, timezone

import pandas as pd
import requests

REPO = "hulashc/tfl-delay-predictor"
BRANCH = "snapshot"
PATH = "latest.json"
THRESHOLD = 0.5

TOKEN = dbutils.secrets.get("tfl", "github_token")
NOW_LABEL = {0: "Good service", 1: "Minor disruption", 2: "Severe delays", 3: "Suspended"}


def iso(ts) -> str:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.isoformat()


def band(p: float) -> str:
    return "likely" if p >= THRESHOLD else "watch" if p >= 0.25 else "clear"

# COMMAND ----------

# MAGIC %md ## 1. Build the snapshot

# COMMAND ----------

latest = spark.sql("""
    SELECT * FROM tfl.gold.predictions
    WHERE time_bucket = (SELECT MAX(time_bucket) FROM tfl.gold.predictions)
""").toPandas()

track = spark.sql("""
    SELECT
      SUM(CASE WHEN p.predicted_disrupted = 1 AND f.is_disrupted = 1 THEN 1 ELSE 0 END) AS caught,
      SUM(CASE WHEN f.is_disrupted = 1 THEN 1 ELSE 0 END)                               AS total,
      SUM(CASE WHEN p.predicted_disrupted = 1 AND f.is_disrupted = 0 THEN 1 ELSE 0 END) AS false_alarms,
      COUNT(f.is_disrupted)                                                             AS checked
    FROM tfl.gold.predictions p
    JOIN tfl.gold.line_features f
      ON f.line_id = p.line_id AND f.time_bucket = p.predicted_for
    WHERE p.predicted_for >= current_timestamp() - INTERVAL 24 HOURS
""").first().asDict()

lines = []
for r in latest.sort_values("line_name").itertuples():
    level = None if pd.isna(r.current_delay_level) else int(r.current_delay_level)
    lines.append({
        "id": r.line_id,
        "name": r.line_name,
        "mode": r.mode,
        "now": NOW_LABEL.get(level, "Planned works or notice"),
        "now_disrupted": bool(level and level >= 1),
        "probability": round(float(r.disruption_prob), 3),
        "band": band(float(r.disruption_prob)),
    })

snapshot = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "scored_at": iso(latest["scored_at"].max()),
    "predicted_for": iso(latest["predicted_for"].max()),
    "model_version": int(latest["model_version"].max()),
    "lines": lines,
    "track_record_24h": {k: int(v or 0) for k, v in track.items()},
}

print(json.dumps(snapshot, indent=2)[:1500])

# COMMAND ----------

# MAGIC %md ## 2. Push to GitHub (`snapshot` branch)

# COMMAND ----------

url = f"https://api.github.com/repos/{REPO}/contents/{PATH}"
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# Updating a file needs its current SHA; creating one doesn't.
existing = requests.get(url, headers=headers, params={"ref": BRANCH}, timeout=20)
sha = existing.json().get("sha") if existing.status_code == 200 else None

body = {
    "message": f"Snapshot for {snapshot['predicted_for']}",
    "content": base64.b64encode(json.dumps(snapshot, indent=2).encode()).decode(),
    "branch": BRANCH,
}
if sha:
    body["sha"] = sha

r = requests.put(url, headers=headers, json=body, timeout=20)
if not r.ok:
    raise RuntimeError(f"GitHub push failed ({r.status_code}): {r.text[:300]}")

print(f"Pushed {len(lines)} lines -> https://raw.githubusercontent.com/{REPO}/{BRANCH}/{PATH}")
