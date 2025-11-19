from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import STAGING_SCHEMA, INTERMEDIATE_SCHEMA

def run_intermediate(spark):
    """
    Build intermediate.user_sessions aggregated at (user_id, session_id).
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {INTERMEDIATE_SCHEMA}")

    df = (
        spark.table(f"{STAGING_SCHEMA}.events")
        .join(
            spark.table(f"{STAGING_SCHEMA}.users"),
            on = "user_id",
            how = "left"
        )
        .join(
            spark.table(f"{STAGING_SCHEMA}.videos"),
            on = "video_id",
            how = "left"
        )
        .groupBy("user_id", "session_id")
        .agg(
            F.min(
                F.when(
                    F.col("event_name") == "session_start",
                    F.col("timestamp")
                )
            ).alias("session_start_ts"),
            F.max(
                F.when(
                    F.col("event_name") == "session_end",
                    F.col("timestamp")
                )
            ).alias("session_end_ts"),
            F.sum(
                F.when(
                    F.col("event_name") == "watch_time",
                    F.col("value")
                ).otherwise(0)
            ).alias("total_watch_time_s"),
            F.max(F.when(F.col("event_name") == "heart", 1).otherwise(0)).alias("has_heart"),
            F.max(F.when(F.col("event_name") == "like", 1).otherwise(0)).alias("has_like"),
            F.first("device").alias("device"),
            F.first("device_os").alias("device_os"),
            F.first("app_version").alias("app_version"),
            F.first("country").alias("country"),
            F.first("signup_date").alias("signup_date"),
            F.first("subscription_tier").alias("subscription_tier"),
            F.first("age_group").alias("age_group"),
            F.first("gender").alias("gender"),
            F.first("video_id").alias("first_video_id"),
            F.first("title").alias("first_video_title"),
            F.first("genre").alias("first_genre"),
            F.first("duration_seconds").alias("first_video_duration_s"),
            F.first("account_id").alias("account_id")
        )
        .withColumn(
            "session_number",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy("session_start_ts")
            )
        )
    )

    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{INTERMEDIATE_SCHEMA}.user_sessions"
    )