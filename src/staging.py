from pyspark.sql import functions as F
from config import BASE_DATA_PATH, STAGING_SCHEMA

def create_schemas(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")

def load_events(spark):
    events_path = f"{BASE_DATA_PATH}/events.jsonl"
    events_raw_df = (
        spark.read
        .json(events_path)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )
    stg_events = (
        events_raw_df
        .select(
            "timestamp",
            "account_id",
            "user_id",
            "video_id",
            "session_id",
            "event_name",
            F.col("value").cast("int").alias("value"),
            "device",
            "device_os",
            "app_version",
            "network_type",
            "ip",
            "country"
        )
    )
    stg_events.write.format("delta").mode("overwrite").saveAsTable(f"{STAGING_SCHEMA}.events")

def load_users(spark):
    users_path = f"{BASE_DATA_PATH}/users.csv"
    stg_users = (
        spark.read
        .option("header", "true")
        .csv(users_path)
    )
    stg_users.write.format("delta").mode("overwrite").saveAsTable(f"{STAGING_SCHEMA}.users")

def load_videos(spark):
    videos_path = f"{BASE_DATA_PATH}/videos.csv"
    stg_videos = (
        spark.read
        .option("header", "true")
        .csv(videos_path)
    )
    stg_videos.write.format("delta").mode("overwrite").saveAsTable(f"{STAGING_SCHEMA}.videos")

def load_devices(spark):
    devices_path = f"{BASE_DATA_PATH}/devices.csv"
    stg_devices = (
        spark.read
        .option("header", "true")
        .csv(devices_path)
    )
    stg_devices.write.format("delta").mode("overwrite").saveAsTable(f"{STAGING_SCHEMA}.devices")

def run_staging(spark):
    create_schemas(spark)
    load_events(spark)
    load_users(spark)
    load_videos(spark)
    load_devices(spark)