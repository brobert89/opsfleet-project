from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import STAGING_SCHEMA, INTERMEDIATE_SCHEMA

def create_schemas(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {INTERMEDIATE_SCHEMA}")

def build_session_events(spark):
    events = spark.table(f"{STAGING_SCHEMA}.events")
    users  = spark.table(f"{STAGING_SCHEMA}.users")
    videos = spark.table(f"{STAGING_SCHEMA}.videos")
    int_session_events = (
        events
        .join(
            users,
            on = "user_id",
            how = "left"
        )
        .join(
            videos,
            on = "video_id",
            how = "left"
        )
    )
    (
        int_session_events
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{INTERMEDIATE_SCHEMA}.session_events")
    
    )

def build_user_sessions(spark):
    session_events = spark.table(f"{INTERMEDIATE_SCHEMA}.session_events")

    session_agg = (
        session_events.groupBy("user_id", "session_id")
          .agg(
              F.min(F.when(F.col("event_name") == "session_start", F.col("timestamp"))).alias("session_start_ts"),
              F.max(F.when(F.col("event_name") == "session_end",   F.col("timestamp"))).alias("session_end_ts"),
              F.sum(F.when(F.col("event_name") == "watch_time", F.col("value")).otherwise(0)).alias("total_watch_time_s"),
              F.max(F.when(F.col("event_name") == "heart", F.lit(1)).otherwise(0)).alias("has_heart"),
              F.max(F.when(F.col("event_name") == "like",  F.lit(1)).otherwise(0)).alias("has_like"),
              F.first("account_id", ignorenulls=True).alias("account_id"),
              F.first("device").alias("device"),
              F.first("device_os").alias("device_os"),
              F.first("app_version").alias("app_version"),
              F.first("network_type").alias("network_type"),
              F.first("country").alias("country"),
              F.first("video_id").alias("first_video_id"),
              F.first("genre").alias("first_genre")
          )
    )
    w_sessions = Window.partitionBy("user_id").orderBy("session_start_ts")
    user_sessions = (
        session_agg
        .withColumn("session_number", F.row_number().over(w_sessions))
    )
    (
        user_sessions
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{INTERMEDIATE_SCHEMA}.user_sessions")
    )


def run_intermediate(spark):
    create_schemas(spark)
    build_session_events(spark)
    build_user_sessions(spark)