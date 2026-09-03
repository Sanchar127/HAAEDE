from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType

# -----------------------------
# SPARK SESSION (FIXED ICEBERG CONFIG)
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# -----------------------------
# CONFIG
# -----------------------------
KAFKA = "recovery-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
TOPIC = "fintech.debt.raw"

# -----------------------------
# FLEXIBLE SCHEMA (covers GitHub + HackerNews)
# -----------------------------
schema = StructType() \
    .add("source", StringType()) \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("repo", StringType()) \
    .add("actor", StringType()) \
    .add("title", StringType()) \
    .add("score", StringType()) \
    .add("comments", StringType()) \
    .add("url", StringType()) \
    .add("author", StringType()) \
    .add("created_at", StringType()) \
    .add("fetched_at", StringType())

# -----------------------------
# CREATE ICEBERG TABLE
# -----------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS local.bronze_events (
    source STRING,
    event_id STRING,
    event_type STRING,
    repo STRING,
    actor STRING,
    title STRING,
    score STRING,
    comments STRING,
    url STRING,
    author STRING,
    created_at STRING,
    fetched_at STRING,
    ingested_at TIMESTAMP
)
USING iceberg
""")

# -----------------------------
# READ KAFKA STREAM (FIXED OFFSET FOR TESTING)
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
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
# OPTIONAL DEBUG (UNCOMMENT IF NEEDED)
# -----------------------------
parsed.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

# -----------------------------
# WRITE TO ICEBERG (STREAMING)
# -----------------------------
query = parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/bronze") \
    .trigger(processingTime="10 seconds") \
    .toTable("local.bronze_events")

query.awaitTermination()