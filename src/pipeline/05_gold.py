"""
Gold layer - model-ready features.

tfl.gold.line_features : one row per line per 15-minute bucket.
  - current state, recent history, time-of-day (London local), weather
  - target: is the line disrupted 1 hour from now?

Rows with a NULL target are the most recent hour - those are the rows we score.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="tfl.gold.line_features",
    comment="Per-line, per-15-min features with weather and a 1-hour-ahead disruption target.",
)
def line_features():
    # 1. One row per line per bucket (several polls can land in one bucket).
    #    Worst unplanned status in the bucket wins.
    base = (
        spark.read.table("tfl.silver.line_status")
        .groupBy("line_id", "line_name", "mode", "time_bucket")
        .agg(
            F.max("delay_level").alias("delay_level"),
            F.max(F.col("is_planned").cast("int")).alias("has_planned_work"),
        )
        .withColumn("is_disrupted", (F.col("delay_level") >= 1).cast("int"))
    )

    # 2. Recent history per line (range window in seconds, so gaps in polling are handled).
    ts = F.unix_timestamp("time_bucket")
    w_line = Window.partitionBy("line_id").orderBy("time_bucket")
    w_2h = Window.partitionBy("line_id").orderBy(ts).rangeBetween(-7200, 0)

    base = (
        base.withColumn("prev_delay_level", F.lag("delay_level").over(w_line))
        .withColumn("disrupted_buckets_2h", F.sum("is_disrupted").over(w_2h))
        .withColumn("max_delay_level_2h", F.max("delay_level").over(w_2h))
    )

    # 3. Time features in London local time (handles BST/GMT switch).
    local = F.from_utc_timestamp("time_bucket", "Europe/London")
    dow = F.dayofweek(local)  # 1 = Sunday, 7 = Saturday
    hour = F.hour(local)
    base = (
        base.withColumn("hour_local", hour)
        .withColumn("day_of_week", dow)
        .withColumn("is_weekend", dow.isin(1, 7).cast("int"))
        .withColumn(
            "is_peak",
            ((~dow.isin(1, 7)) & (hour.between(7, 9) | hour.between(16, 19))).cast("int"),
        )
    )

    # 4. Weather at the same 15-min slot (Open-Meteo readings are already 15-min aligned).
    weather = spark.read.table("tfl.silver.weather").withColumnRenamed("observed_at", "time_bucket")
    base = base.join(weather, on="time_bucket", how="left")

    # 5. Target: delay state of the same line 1 hour later.
    future = base.select(
        "line_id",
        (F.col("time_bucket") - F.expr("INTERVAL 1 HOUR")).alias("time_bucket"),
        F.col("delay_level").alias("delay_level_next_1h"),
    )
    return base.join(future, on=["line_id", "time_bucket"], how="left").withColumn(
        "is_disrupted_next_1h", (F.col("delay_level_next_1h") >= 1).cast("int")
    )
