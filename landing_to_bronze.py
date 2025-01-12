import os
from pyspark.sql import SparkSession
import requests
from prefect import task, flow

# FTP base URL and tables to process
ftp_base_url = "https://ftp.goit.study/neoversity/"
tables = ["athlete_bio", "athlete_event_results"]

# Define Bronze base path for saving Parquet files
bronze_base_path = "bronze"

@task
def download_data(file):
    """
    Downloads a CSV file from the specified FTP server.
    """
    downloading_url = f"{ftp_base_url}{file}.csv"
    print(f"Attempting to download from: {downloading_url}")

    try:
        response = requests.get(downloading_url)
        response.raise_for_status()  # Raise an error for bad HTTP status codes
        with open(f"{file}.csv", "wb") as file_handle:
            file_handle.write(response.content)
        print(f"File '{file}.csv' downloaded successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {file}.csv. Error: {e}")
        raise  # Raise the exception for Prefect to handle

@task
def process_table(table):
    """
    Reads CSV data into Spark, converts it to Parquet, and verifies the saved data.
    """
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Step 1: Read CSV file into a DataFrame
        local_path = f"{table}.csv"
        print(f"Step 1: Reading CSV data for '{table}' into a Spark DataFrame...")
        df = spark.read.csv(local_path, header=True, inferSchema=True)
        print(f"DataFrame schema for '{table}':")
        df.printSchema()  # Print the schema of the DataFrame

        # Step 2: Save the DataFrame to Parquet in the Bronze zone
        output_path = f"{bronze_base_path}/{table}"
        os.makedirs(bronze_base_path, exist_ok=True)  # Ensure the bronze directory exists
        print(f"Step 2: Saving '{table}' data to Parquet in the Bronze zone at '{output_path}'...")
        df.write.mode("overwrite").parquet(output_path)
        print(f"Data for '{table}' successfully saved to '{output_path}'.")

        # Step 3: Verify the saved data by reading it back
        print(f"Step 3: Verifying the saved data for '{table}' by reading it back...")
        bronze_df = spark.read.parquet(output_path)
        print(f"Successfully read back '{table}' data from Parquet:")
        bronze_df.show(5, truncate=False)  # Show the first 5 rows of the DataFrame

    except Exception as e:
        print(f"Error processing table '{table}': {e}")
        raise  # Raise the exception for Prefect to handle
    finally:
        spark.stop()

@flow
def landing_to_bronze_flow():
    """
    Prefect flow for processing all tables.
    """
    for table in tables:
        print(f"\nProcessing table: {table}")
        download_data(table)  # Task to download data
        process_table(table)  # Task to process table

if __name__ == "__main__":
    landing_to_bronze_flow()
