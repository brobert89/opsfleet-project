# src/run_pipeline.py

from pyspark.sql import SparkSession

from staging import run_staging
from intermediate import run_intermediate
from mart import run_mart

def main():
    spark = SparkSession.builder.getOrCreate()

    run_staging(spark)
    run_intermediate(spark)
    run_mart(spark)

if __name__ == "__main__":
    main()