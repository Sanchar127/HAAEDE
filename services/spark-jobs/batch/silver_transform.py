from pyspark.sql.functions import (
    col,
    trim,
    when,
    to_timestamp,
    row_number,
    lit,
)
from pyspark.sql.window import Window

from common.spark_session import create_spark


spark = create_spark("SilverTransform")


# ============================================================
# 1. Read Bronze
# ============================================================

bronze = spark.table("local.bronze_events")


# ============================================================
# 2. Clean Bronze data
# ============================================================

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


# ============================================================
# 3. Remove invalid events
# ============================================================

cleaned = cleaned.filter(
    col("event_id").isNotNull()
    & (col("event_id") != "")
    & col("source").isNotNull()
    & (col("source") != "")
    & col("title").isNotNull()
    & (col("title") != "")
)


# ============================================================
# 4. Normalize data types
# ============================================================

cleaned = (
    cleaned
    .withColumn(
        "score",
        when(
            col("score").cast("long").isNotNull(),
            col("score").cast("long"),
        ).otherwise(lit(None).cast("long")),
    )
    .withColumn(
        "comments",
        when(
            col("comments").cast("long").isNotNull(),
            col("comments").cast("long"),
        ).otherwise(lit(None).cast("long")),
    )
    .withColumn(
        "created_at",
        to_timestamp(col("created_at")),
    )
    .withColumn(
        "fetched_at",
        to_timestamp(col("fetched_at")),
    )
    .withColumn(
        "ingested_at",
        to_timestamp(col("ingested_at")),
    )
)


# ============================================================
# 5. Deduplicate Bronze before MERGE
#
# Bronze is append-only and can contain multiple copies
# of the same event_id.
#
# Keep the latest version based on ingested_at.
# ============================================================

dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(col("ingested_at").desc_nulls_last())
)

deduplicated = (
    cleaned
    .withColumn(
        "_row_number",
        row_number().over(dedup_window),
    )
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)


# ============================================================
# 6. Select canonical Silver schema
# ============================================================

silver = deduplicated.select(
    "source",
    "event_id",
    "event_type",
    "repo",
    "actor",
    "title",
    "score",
    "comments",
    "url",
    "author",
    "created_at",
    "fetched_at",
    "ingested_at",
)


# ============================================================
# 7. Create Silver table if it does not exist
# ============================================================

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS local.silver_events (
        source STRING,
        event_id STRING,
        event_type STRING,
        repo STRING,
        actor STRING,
        title STRING,
        score BIGINT,
        comments BIGINT,
        url STRING,
        author STRING,
        created_at TIMESTAMP,
        fetched_at TIMESTAMP,
        ingested_at TIMESTAMP
    )
    USING iceberg
    """
)


# ============================================================
# 8. Create temporary source view
# ============================================================

silver.createOrReplaceTempView("silver_source")


# ============================================================
# 9. Idempotent MERGE into Silver
#
# event_id is the business key.
#
# Existing event_id:
#     UPDATE the canonical Silver row.
#
# New event_id:
#     INSERT a new Silver row.
#
# This makes the operation safe to run repeatedly.
# ============================================================

spark.sql(
    """
    MERGE INTO local.silver_events AS target
    USING silver_source AS source
    ON target.event_id = source.event_id

    WHEN MATCHED THEN UPDATE SET
        target.source = source.source,
        target.event_type = source.event_type,
        target.repo = source.repo,
        target.actor = source.actor,
        target.title = source.title,
        target.score = source.score,
        target.comments = source.comments,
        target.url = source.url,
        target.author = source.author,
        target.created_at = source.created_at,
        target.fetched_at = source.fetched_at,
        target.ingested_at = source.ingested_at

    WHEN NOT MATCHED THEN INSERT (
        source,
        event_id,
        event_type,
        repo,
        actor,
        title,
        score,
        comments,
        url,
        author,
        created_at,
        fetched_at,
        ingested_at
    )
    VALUES (
        source.source,
        source.event_id,
        source.event_type,
        source.repo,
        source.actor,
        source.title,
        source.score,
        source.comments,
        source.url,
        source.author,
        source.created_at,
        source.fetched_at,
        source.ingested_at
    )
    """
)


# ============================================================
# 10. Stop Spark
# ============================================================

spark.stop()

