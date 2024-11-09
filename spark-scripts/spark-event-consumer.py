from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("PurchaseStreamProcessor") \
    .getOrCreate()

# Define schema for purchase events
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

def process_stream():
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "dataeng-kafka:9092") \
        .option("subscribe", "purchase_events") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON from Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert timestamp string to timestamp type
    parsed_df = parsed_df.withColumn(
        "event_timestamp", 
        to_timestamp("timestamp")
    )

    # Calculate running totals with minute window
    window_agg = parsed_df \
        .withWatermark("event_timestamp", "1 minute") \
        .groupBy(
            window("event_timestamp", "1 minute")
        ) \
        .agg(
            sum("price").alias("total_amount"),
            count("*").alias("number_of_transactions"),
            round(avg("price"), 2).alias("average_price")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_amount"),
            col("number_of_transactions"),
            col("average_price")
        )

    # Write to console
    query = window_agg \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    print("Starting Spark Streaming Consumer...")
    process_stream()