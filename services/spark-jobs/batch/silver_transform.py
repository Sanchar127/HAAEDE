from common.spark_session import create_spark

spark = create_spark("SilverTransform")

# ------------------------
# READ BRONZE TABLE
# ------------------------
bronze = spark.table("local.bronze_events")

# ------------------------
# CLEAN + TRANSFORM
# ------------------------
silver = bronze \
    .dropDuplicates(["title", "timestamp"]) \
    .filter("title IS NOT NULL") \
    .withColumnRenamed("title", "event_title")

# ------------------------
# CREATE SILVER TABLE
# ------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver_events (
    source STRING,
    event_title STRING,
    timestamp STRING,
    ingested_at TIMESTAMP
)
USING iceberg
""")

# ------------------------
# WRITE SILVER TABLE (BATCH)
# ------------------------
silver.writeTo("local.silver_events").append()