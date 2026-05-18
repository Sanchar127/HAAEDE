from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# CONFIG
# -----------------------------
KAFKA = "recovery-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
TOPIC = "fintech.debt.raw"

# -----------------------------
# SCHEMA
# -----------------------------
schema = StructType() \
    .add("source", StringType()) \
    .add("title", StringType()) \
    .add("timestamp", StringType())

# -----------------------------
# KAFKA STREAM
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------
# PARSE JSON SAFELY
# -----------------------------
parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("source").isNotNull()) \
    .withColumn("ingested_at", current_timestamp())

# -----------------------------
# ICEBERG TABLE (OK)
# -----------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS local.bronze_events (
    source STRING,
    title STRING,
    timestamp STRING,
    ingested_at TIMESTAMP
)
USING iceberg
""")

# -----------------------------
# STREAM WRITE (FIXED)
# -----------------------------
query = parsed.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/bronze") \
    .trigger(processingTime="10 seconds") \
    .toTable("local.bronze_events")

query.awaitTermination()