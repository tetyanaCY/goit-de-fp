from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import os

# Kafka and MySQL configurations
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "sasl_plain_username": "admin",
    "sasl_plain_password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_jaas_config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ),
}

jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
    "dbtable": "ginger_athlete_enriched_avg",
}

# Initialize Spark Session
spark = (
    SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .appName("JDBCToKafka")
    .master("local[*]")
    .getOrCreate()
)

# Kafka message schema
schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Read data from Kafka
kafka_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", "ginger_athlete_event_results")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Read athlete bio data from MySQL
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable="athlete_bio",
        user=jdbc_config["user"],
        password=jdbc_config["password"],
    )
    .load()
)

# Filter valid bio data
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull())
    & (col("weight").isNotNull())
    & (col("height").cast("double").isNotNull())
    & (col("weight").cast("double").isNotNull())
)

# Join Kafka stream with bio data
joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

# Aggregate results
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

# Define the batch processing function
def foreach_batch_function(df, epoch_id):
    # Write to Kafka
    df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]) \
        .option("topic", "ginger_athlete_enriched_avg") \
        .save()

    # Write to MySQL
    df.write.format("jdbc").options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable=jdbc_config["dbtable"],
        user=jdbc_config["user"],
        password=jdbc_config["password"],
    ).mode("append").save()

# Start the streaming query
query = (
    aggregated_df.writeStream.outputMode("complete")
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", "/path/to/checkpoint/dir")
    .start()
)

query.awaitTermination()
