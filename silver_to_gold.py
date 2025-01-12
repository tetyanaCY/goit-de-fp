from prefect import task, flow
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

@task
def silver_to_gold(silver_path="silver", gold_path="gold"):
    spark = SparkSession.builder.appName("SilverToGold").master("local[*]").getOrCreate()
    athlete_bio_df = spark.read.parquet(f"{silver_path}/athlete_bio")
    athlete_event_results_df = spark.read.parquet(f"{silver_path}/athlete_event_results")

    joined_df = athlete_bio_df.join(
        athlete_event_results_df,
        athlete_bio_df.athlete_id == athlete_event_results_df.athlete_id,
        "inner"
    ).select(
        athlete_event_results_df.sport,
        athlete_event_results_df.medal,
        athlete_bio_df.sex,
        athlete_bio_df.country_noc,
        athlete_bio_df.height,
        athlete_bio_df.weight
    )

    gold_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

    os.makedirs(gold_path, exist_ok=True)
    output_path = f"{gold_path}/avg_stats"
    gold_df.write.mode("overwrite").parquet(output_path)
    print(f"Saved data to: {output_path}")
    spark.stop()

@flow
def silver_to_gold_flow():
    silver_to_gold()

if __name__ == "__main__":
    silver_to_gold_flow()
