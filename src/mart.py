# src/mart.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import INTERMEDIATE_SCHEMA, MART_SCHEMA

def create_schemas(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MART_SCHEMA}")


def build_user_session_facts(spark):
    us = spark.table(f"{INTERMEDIATE_SCHEMA}.user_sessions")

    w_user = Window.partitionBy("user_id").orderBy("session_start_ts")

    mart = (
        us
        .withColumn(
            "reached_30s",
            F.when(F.col("total_watch_time_s") >= 30, F.lit(1)).otherwise(0)
        )
        .withColumn(
            "dropoff_under_10s",
            F.when(F.col("total_watch_time_s") < 10, F.lit(1)).otherwise(0)
        )
        .withColumn("next_session_start_ts", F.lead("session_start_ts").over(w_user))
        .withColumn("next_session_number",    F.lead("session_number").over(w_user))
        .withColumn(
            "retained_next_session_within_3d",
            F.when(
                (F.col("session_number") == 1) &
                F.col("next_session_start_ts").isNotNull() &
                (F.datediff(F.col("next_session_start_ts"), F.col("session_start_ts")) <= 3),
                F.lit(1)
            ).otherwise(0)
        )
    )

    mart.write.format("delta").mode("overwrite").saveAsTable(f"{MART_SCHEMA}.user_session_facts")


def run_mart(spark):
    create_schemas(spark)
    build_user_session_facts(spark)