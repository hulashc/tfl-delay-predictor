"""
Silver layer - cleaned, flattened, typed.

tfl.silver.line_status : one row per line per status per poll, with a delay_level label
tfl.silver.weather     : one row per distinct weather reading (deduped on observed time)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def bucket_15m(col):
    """Snap a timestamp down to its 15-minute bucket (e.g. 15:13 -> 15:00)."""
    return F.timestamp_seconds(F.floor(F.unix_timestamp(col) / 900) * 900)


# TfL statusSeverity -> our delay label. Full list: GET /Line/Meta/Severity
#   0 = good service / no issues
#   1 = minor disruption (minor delays, reduced service, change of frequency, issues reported)
#   2 = severe delays
#   3 = suspended / not running
# Anything else (planned closures, information, special service) -> NULL,
# i.e. excluded from the prediction target.
DELAY_LEVEL = (
    F.when(F.col("status_severity").isin(10, 18), 0)
    .when(F.col("status_severity").isin(9, 7, 14, 17), 1)
    .when(F.col("status_severity") == 6, 2)
    .when(F.col("status_severity").isin(2, 3, 16), 3)
)


@dp.table(
    name="tfl.silver.line_status",
    comment="Flattened TfL line statuses, one row per line per status per poll, with delay_level label.",
)
@dp.expect_or_drop("has_line_id", "line_id IS NOT NULL")
@dp.expect_or_drop("has_severity", "status_severity IS NOT NULL")
def line_status():
    lines = spark.readStream.table("tfl.bronze.line_status_raw").select(
        "ingested_at", F.explode("payload").alias("line")
    )
    return (
        lines.select("ingested_at", "line", F.explode("line.lineStatuses").alias("s"))
        .select(
            "ingested_at",
            bucket_15m("ingested_at").alias("time_bucket"),
            F.col("line.id").alias("line_id"),
            F.col("line.name").alias("line_name"),
            F.col("line.modeName").alias("mode"),
            F.col("s.statusSeverity").cast("int").alias("status_severity"),
            F.col("s.statusSeverityDescription").alias("status_description"),
            F.col("s.reason").alias("reason"),
            F.col("s.disruption.category").alias("disruption_category"),
        )
        .withColumn("delay_level", DELAY_LEVEL)
        .withColumn("is_planned", F.coalesce(F.col("disruption_category") == "PlannedWork", F.lit(False)))
    )


@dp.materialized_view(
    name="tfl.silver.weather",
    comment="London weather readings from Open-Meteo, deduplicated on observation time.",
)
@dp.expect_or_drop("has_observed_at", "observed_at IS NOT NULL")
def weather():
    return (
        spark.read.table("tfl.bronze.weather_raw")
        .select(
            F.col("payload.current.time").cast("timestamp").alias("observed_at"),
            F.col("payload.current.temperature_2m").cast("double").alias("temp_c"),
            F.col("payload.current.precipitation").cast("double").alias("precip_mm"),
            F.col("payload.current.rain").cast("double").alias("rain_mm"),
            F.col("payload.current.wind_speed_10m").cast("double").alias("wind_kmh"),
            F.col("payload.current.weather_code").cast("int").alias("weather_code"),
        )
        .dropDuplicates(["observed_at"])
    )
