from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType

# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
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
# READ KAFKA STREAM
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
    .filter(col("source").isNotNull()) \
    .withColumn("ingested_at", current_timestamp())

# -----------------------------
# STREAM WRITE
# Iceberg creates the table automatically on first write
# when using .toTable() with a non-existent table.
# No DDL needed - avoids HadoopCatalog + S3A version-hint bug.
# -----------------------------
query = parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/bronze") \
    .option("fanout-enabled", "true") \
    .toTable("local.bronze_events")

query.awaitTermination()