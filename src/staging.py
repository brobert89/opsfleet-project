from pyspark.sql import functions as F
from config import BASE_DATA_PATH, STAGING_SCHEMA


def create_schemas(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")


def load_events(spark):
    (
        spark.read
        .json(f"{BASE_DATA_PATH}/events.jsonl")
        .select(
            F.to_timestamp("timestamp").alias("timestamp"),
            "account_id",
            "user_id",
            "video_id",
            "session_id",
            "event_name",
            F.col("value").cast("int").alias("value"),
            "device",
            "device_os",
            "app_version",
            "country"
        )
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{STAGING_SCHEMA}.events")
    )

def load_users(spark):
    (
        spark.read
        .option("header", "true")
        .csv(f"{BASE_DATA_PATH}/users.csv")
        .select(
            "user_id",
            F.to_date("signup_date").alias("signup_date"),
            "subscription_tier",
            "age_group",
            "gender"
        )
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{STAGING_SCHEMA}.users")
    )


def load_videos(spark):
    (
        spark.read
        .option("header", "true")
        .csv(f"{BASE_DATA_PATH}/videos.csv")
        .select(
            "video_id",
            "title",
            "genre",
            "duration_seconds",
        )
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{STAGING_SCHEMA}.videos")
    )


def run_staging(spark):
    create_schemas(spark)
    load_events(spark)
    load_users(spark)
    load_videos(spark)