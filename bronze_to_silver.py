from prefect import task, flow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
import os

@task
def clean_and_save_bronze_to_silver(bronze_path="bronze", silver_path="silver"):
    spark = SparkSession.builder.appName("BronzeToSilver").master("local[*]").getOrCreate()
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        df = spark.read.parquet(f"{bronze_path}/{table}")
        text_columns = [col_name for col_name, dtype in df.dtypes if dtype == "string"]
        for column in text_columns:
            df = df.withColumn(column, trim(lower(col(column))))
        df = df.dropDuplicates()
        os.makedirs(silver_path, exist_ok=True)
        output_path = f"{silver_path}/{table}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Saved {table} to: {output_path}")
    spark.stop()

@flow
def bronze_to_silver_flow():
    clean_and_save_bronze_to_silver()

if __name__ == "__main__":
    bronze_to_silver_flow()
