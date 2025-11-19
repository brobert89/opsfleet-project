# src/run_pipeline.py

from pyspark.sql import SparkSession

from staging import run_staging
from intermediate import run_intermediate
from mart import run_mart

def main():
    spark = SparkSession.builder.getOrCreate()

    # 1. STAGING
    run_staging(spark)

    # 2. INTERMEDIATE
    run_intermediate(spark)

    # 3. MART
    run_mart(spark)

    # Test mic: arată câteva rânduri din mart
    spark.sql("SELECT * FROM mart.user_session_facts LIMIT 10").show()

if __name__ == "__main__":
    main()