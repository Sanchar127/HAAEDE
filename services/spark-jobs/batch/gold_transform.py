from pyspark.sql.functions import (
    avg,
    col,
    count,
    coalesce,
    current_timestamp,
    lit,
    sum,
    to_date,
)
from pyspark.sql.window import Window

from common.spark_session import create_spark


# ============================================================
# 1. Create Spark session
# ============================================================

spark = create_spark("GoldTransform")


try:
    # ========================================================
    # 2. Read Silver
    # ========================================================

    silver = spark.table("local.silver_events")


    # ========================================================
    # 3. Validate required Silver columns
    # ========================================================

    required_columns = {
        "event_id",
        "source",
        "score",
        "comments",
        "created_at",
        "ingested_at",
    }

    missing_columns = required_columns - set(silver.columns)

    if missing_columns:
        raise ValueError(
            f"Silver table is missing required columns: "
            f"{sorted(missing_columns)}"
        )


    # ========================================================
    # 4. Determine business event date
    #
    # Prefer the actual event timestamp.
    # Fall back to ingestion timestamp when created_at is NULL.
    # ========================================================

    silver_with_date = silver.withColumn(
        "event_date",
        coalesce(
            to_date(col("created_at")),
            to_date(col("ingested_at")),
        ),
    )


    # ========================================================
    # 5. Validate event_date
    #
    # A valid Silver record should have at least one usable
    # timestamp. Do not silently create a NULL date aggregate.
    # ========================================================

    invalid_date_count = (
        silver_with_date
        .filter(col("event_date").isNull())
        .limit(1)
        .count()
    )

    if invalid_date_count > 0:
        raise ValueError(
            "Silver contains records with both created_at and "
            "ingested_at NULL. Gold aggregation cannot continue."
        )


    # ========================================================
    # 6. Build daily Gold aggregation
    # ========================================================

    gold = (
        silver_with_date
        .groupBy(
            "event_date",
            "source",
        )
        .agg(
            count("*").cast("long").alias("event_count"),
            sum("score").cast("long").alias("total_score"),
            avg("score").cast("double").alias("avg_score"),
            sum("comments").cast("long").alias("total_comments"),
        )
        .withColumn(
            "updated_at",
            current_timestamp(),
        )
    )


    # ========================================================
    # 7. Validate Gold aggregation
    # ========================================================

    invalid_gold_count = (
        gold
        .filter(
            col("event_date").isNull()
            | col("source").isNull()
            | (col("source") == "")
        )
        .limit(1)
        .count()
    )

    if invalid_gold_count > 0:
        raise ValueError(
            "Gold aggregation contains invalid business keys."
        )


    # ========================================================
    # 8. Create Gold table if it does not exist
    # ========================================================

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.gold_daily_event_summary (
            event_date DATE,
            source STRING,
            event_count BIGINT,
            total_score BIGINT,
            avg_score DOUBLE,
            total_comments BIGINT,
            updated_at TIMESTAMP
        )
        USING iceberg
        """
    )


    # ========================================================
    # 9. Create temporary source view
    # ========================================================

    gold.createOrReplaceTempView("gold_source")


    # ========================================================
    # 10. Atomic/idempotent MERGE
    #
    # Business key:
    #     event_date + source
    #
    # Existing aggregate:
    #     UPDATE
    #
    # New aggregate:
    #     INSERT
    # ========================================================

    spark.sql(
        """
        MERGE INTO local.gold_daily_event_summary AS target
        USING gold_source AS source
        ON target.event_date = source.event_date
           AND target.source = source.source

        WHEN MATCHED THEN UPDATE SET
            target.event_count = source.event_count,
            target.total_score = source.total_score,
            target.avg_score = source.avg_score,
            target.total_comments = source.total_comments,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            event_date,
            source,
            event_count,
            total_score,
            avg_score,
            total_comments,
            updated_at
        )
        VALUES (
            source.event_date,
            source.source,
            source.event_count,
            source.total_score,
            source.avg_score,
            source.total_comments,
            source.updated_at
        )
        """
    )


    # ========================================================
    # 11. Post-write validation
    # ========================================================

    gold_count = (
        spark.table("local.gold_daily_event_summary")
        .count()
    )

    if gold_count == 0:
        raise ValueError(
            "Gold table contains zero rows after successful MERGE."
        )


    print(
        f"Gold transformation completed successfully. "
        f"Gold aggregate rows: {gold_count}"
    )


finally:
    # ========================================================
    # 12. Always stop Spark
    # ========================================================

    spark.stop()

