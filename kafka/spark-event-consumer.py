from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# Define the schema
schema = StructType([
    StructField('event_id', IntegerType(), True),
    StructField('value', DoubleType(), True),
    StructField('timestamp', DoubleType(), True)
])

spark = SparkSession \
    .builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "random_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Perform calculation (e.g., average value)
avg_df = json_df.groupBy().agg(avg("value").alias("average_value"))

# Write output to console
query = avg_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
