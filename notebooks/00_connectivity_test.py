# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Connectivity test
# MAGIC Checks that the TfL key is stored correctly and serverless compute can reach the API.
# MAGIC Expected: `length: 32`, `200`, then each tube line with its status.

# COMMAND ----------

import requests

key = dbutils.secrets.get("tfl", "app_key")
print("length:", len(key))  # should be 32 - a 33 usually means a stray character from pasting

r = requests.get(
    "https://api.tfl.gov.uk/Line/Mode/tube/Status",
    params={"app_key": key},
    timeout=10,
)
print(r.status_code)

if r.ok:
    for line in r.json():
        print(line["name"], "-", line["lineStatuses"][0]["statusSeverityDescription"])
else:
    print(r.headers.get("Content-Type"))
    print(r.text[:500])
