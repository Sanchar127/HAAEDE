from pyspark.sql.functions import (
    col,
    trim,
    when,
    to_timestamp,
)
from common.spark_session import create_spark


spark = create_spark("SilverTransform")


# --------------------------------------------------
# READ BRONZE
# --------------------------------------------------

bronze = spark.table("local.bronze_events")


# --------------------------------------------------
# CLEAN STRINGS
# --------------------------------------------------

cleaned = (
    bronze
    .withColumn("source", trim(col("source")))
    .withColumn("event_id", trim(col("event_id")))
    .withColumn("title", trim(col("title")))
    .withColumn("repo", trim(col("repo")))
    .withColumn("actor", trim(col("actor")))
    .withColumn("url", trim(col("url")))
    .withColumn("author", trim(col("author")))
)


# --------------------------------------------------
# VALIDATE REQUIRED FIELDS
# --------------------------------------------------

cleaned = cleaned.filter(
    col("event_id").isNotNull()
    & (col("event_id") != "")
    & col("source").isNotNull()
    & (col("source") != "")
    & col("title").isNotNull()
    & (col("title") != "")
)


# --------------------------------------------------
# CONVERT TYPES
# --------------------------------------------------

cleaned = (
    cleaned
    .withColumn(
        "score",
        when(
            col("score").cast("long").isNotNull(),
            col("score").cast("long"),
        ).otherwise(None),
    )
    .withColumn(
        "comments",
        when(
            col("comments").cast("long").isNotNull(),
            col("comments").cast("long"),
        ).otherwise(None),
    )
    .withColumn(
        "created_at",
        to_timestamp(col("created_at")),
    )
    .withColumn(
        "fetched_at",
        to_timestamp(col("fetched_at")),
    )
)


# --------------------------------------------------
# DEDUPLICATE
# --------------------------------------------------

silver = cleaned.dropDuplicates(["event_id"])


# --------------------------------------------------
# FINAL SILVER SCHEMA
# --------------------------------------------------

silver = silver.select(
    "source",
    "event_id",
    "event_type",
    "repo",
    "actor",
    col("title").alias("event_title"),
    "score",
    "comments",
    "url",
    "author",
    "created_at",
    "fetched_at",
    "ingested_at",
)


# --------------------------------------------------
# CREATE SILVER TABLE
# --------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver_events (
    source STRING,
    event_id STRING,
    event_type STRING,
    repo STRING,
    actor STRING,
    event_title STRING,
    score BIGINT,
    comments BIGINT,
    url STRING,
    author STRING,
    created_at TIMESTAMP,
    fetched_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING iceberg
""")


# --------------------------------------------------
# WRITE SILVER
# --------------------------------------------------

silver.writeTo("local.silver_events").append()


spark.stop()