"""
Clickstream Session Analysis with PySpark Window Functions
==========================================================
Problem:
  Given clickstream events (user_id, ts, url), assign session IDs
  (new session starts after 30 minutes of inactivity per user), then
  compute sessions per day and average session length.

Approach:
  1. Compute lag(ts) per user ordered by ts → gap_seconds
  2. Flag a new session when gap_seconds > 1800 (30 min) or NULL (first event)
  3. Cumulative sum of the flag → session_index per user
  4. Compose a unique session_id = user_id + date + session_index
  5. Aggregate: sessions per day, avg session length (last_ts - first_ts)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Spark session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ClickstreamSessionAnalysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. Load sample data ────────────────────────────────────────────────────────
raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/clickstream.csv")
)

events = raw.withColumn("ts", F.to_timestamp("ts", "yyyy-MM-dd HH:mm:ss"))

print("=== Raw Events (first 10) ===")
events.orderBy("user_id", "ts").show(10, truncate=False)

# ── 2. Session ID assignment via window functions ──────────────────────────────
# Window: per user, ordered by event timestamp
user_ts_w = Window.partitionBy("user_id").orderBy("ts")

# Time gap from the previous event (seconds)
events = events.withColumn(
    "prev_ts",
    F.lag("ts").over(user_ts_w)
).withColumn(
    "gap_seconds",
    F.when(
        F.col("prev_ts").isNull(), F.lit(None)   # first event of this user
    ).otherwise(
        F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")
    )
)

# New-session flag: 1 when gap > 30 min OR it's the user's first event
SESSION_GAP = 30 * 60   # 1800 seconds

events = events.withColumn(
    "new_session_flag",
    F.when(
        F.col("gap_seconds").isNull() | (F.col("gap_seconds") > SESSION_GAP),
        F.lit(1)
    ).otherwise(F.lit(0))
)

# Cumulative sum of the flag gives a monotonically increasing session index
events = events.withColumn(
    "session_index",
    F.sum("new_session_flag").over(user_ts_w.rowsBetween(Window.unboundedPreceding, 0))
)

# Globally unique session ID
events = events.withColumn(
    "session_id",
    F.concat_ws(
        "_",
        F.col("user_id"),
        F.date_format("ts", "yyyyMMdd"),
        F.col("session_index").cast("string")
    )
)

print("=== Events with Session IDs ===")
events.select("user_id", "ts", "url", "gap_seconds", "new_session_flag", "session_id") \
      .orderBy("user_id", "ts") \
      .show(40, truncate=False)

# ── 3. Sessions per day ────────────────────────────────────────────────────────
sessions_per_day = (
    events
    .withColumn("date", F.to_date("ts"))
    .groupBy("date")
    .agg(F.countDistinct("session_id").alias("sessions_count"))
    .orderBy("date")
)

print("=== Sessions Per Day ===")
sessions_per_day.show(truncate=False)

# ── 4. Average session length ──────────────────────────────────────────────────
# Session length = last_event_ts - first_event_ts within the session (seconds)
session_stats = (
    events
    .groupBy("session_id", "user_id")
    .agg(
        F.min("ts").alias("session_start"),
        F.max("ts").alias("session_end"),
        F.count("*").alias("event_count")
    )
    .withColumn(
        "session_length_seconds",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
    )
    .withColumn(
        "session_length_minutes",
        F.round(F.col("session_length_seconds") / 60, 2)
    )
)

print("=== Per-Session Stats ===")
session_stats.orderBy("user_id", "session_start").show(truncate=False)

avg_session_length = session_stats.agg(
    F.round(F.avg("session_length_seconds"), 2).alias("avg_length_seconds"),
    F.round(F.avg("session_length_minutes"), 2).alias("avg_length_minutes"),
    F.round(F.avg("event_count"), 2).alias("avg_events_per_session"),
    F.count("*").alias("total_sessions")
)

print("=== Average Session Length ===")
avg_session_length.show(truncate=False)

# ── 5. Save outputs ───────────────────────────────────────────────────────────
(
    events
    .select("user_id", "ts", "url", "session_id", "gap_seconds")
    .orderBy("user_id", "ts")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv("output/events_with_sessions")
)

sessions_per_day.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("output/sessions_per_day")

session_stats.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv("output/session_stats")

print("\n✅ Outputs written to output/")
spark.stop()
