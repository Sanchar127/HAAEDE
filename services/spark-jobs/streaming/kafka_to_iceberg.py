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
# READ KAFKA
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

# -----------------------------
# ICEBERG TABLE
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
# WRITE STREAM → ICEBERG
# -----------------------------
query = parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/bronze") \
    .toTable("local.bronze_events")

query.awaitTermination()