# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Ingest raw data
# MAGIC Polls the TfL Unified API (line status) and Open-Meteo (London weather),
# MAGIC then lands the raw JSON in the `tfl.bronze.landing` Volume.
# MAGIC Auto Loader picks these files up in the next step.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

import requests

CATALOG = "tfl"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

TFL_KEY = dbutils.secrets.get("tfl", "app_key")
MODES = "tube,dlr,overground,elizabeth-line"

TFL_URL = f"https://api.tfl.gov.uk/Line/Mode/{MODES}/Status"
WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=51.5072&longitude=-0.1276"
    "&current=temperature_2m,precipitation,rain,wind_speed_10m,weather_code"
)

# COMMAND ----------

def land(source: str, payload, run_ts: datetime) -> str:
    """Write one raw response to /landing/<source>/YYYY/MM/DD/<timestamp>.json"""
    folder = f"{LANDING}/{source}/{run_ts:%Y/%m/%d}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{run_ts:%Y%m%dT%H%M%SZ}.json"
    record = {
        "ingested_at": run_ts.isoformat(),
        "source": source,
        "payload": payload,
    }
    with open(path, "w") as f:
        json.dump(record, f)
    return path


def fetch(url: str, params: dict | None = None):
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# COMMAND ----------

run_ts = datetime.now(timezone.utc)

line_status = fetch(TFL_URL, params={"app_key": TFL_KEY, "detail": "true"})
weather = fetch(WEATHER_URL)

p1 = land("line_status", line_status, run_ts)
p2 = land("weather", weather, run_ts)

print(f"Landed {len(line_status)} lines -> {p1}")
print(f"Landed weather -> {p2}")
