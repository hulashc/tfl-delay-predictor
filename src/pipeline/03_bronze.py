"""
Bronze layer - raw files -> Delta, incrementally via Auto Loader.

Each landed JSON file becomes one row: ingested_at, source, payload (as landed),
plus file lineage. No cleaning here - Bronze is the faithful copy of the source.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/tfl/bronze/landing"


def read_landing(source: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING}/{source}/")
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_loaded_at", F.current_timestamp())
    )


@dp.table(
    name="line_status_raw",
    comment="Raw TfL line status snapshots (Tube, DLR, Overground, Elizabeth line), one row per poll.",
)
def line_status_raw():
    return read_landing("line_status")


@dp.table(
    name="weather_raw",
    comment="Raw Open-Meteo current London weather, one row per poll.",
)
def weather_raw():
    return read_landing("weather")
