"""
Feature Engineering Pipeline

Reads raw events from Iceberg
Builds ML features
Writes features back to Iceberg

Input Table:
    local.bronze_events

Output Table:
    local.github_features
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    when,
    avg,
    max,
    current_timestamp,
)

from services.ingestion.app.log.logger import get_logger

logger = get_logger("feature-builder")


class FeatureBuilder:
    def __init__(self):
        logger.info("Initializing Feature Builder...")

        self.spark = (
            SparkSession.builder
            .appName("FeatureBuilder")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                "spark.sql.catalog.local.type",
                "hadoop",
            )
            .config(
                "spark.sql.catalog.local.warehouse",
                "s3a://warehouse/",
            )
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")

        self.input_table = "local.bronze_events"
        self.output_table = "local.github_features"

        logger.info("Spark Session initialized successfully.")

    def read_events(self):
        """
        Read events from Iceberg.
        """
        logger.info(f"Reading Iceberg table: {self.input_table}")

        try:
            df = (
                self.spark.read
                .format("iceberg")
                .load(self.input_table)
            )

            logger.info("Successfully loaded input table.")
            return df

        except Exception as e:
            logger.exception(f"Failed to read table {self.input_table}: {e}")
            raise

    def build_features(self, df):
        """
        Build repository-level ML features.
        """

        logger.info("Building repository features...")

        try:
            df = (
                df.withColumn("score", col("score").cast("double"))
                  .withColumn("comments", col("comments").cast("double"))
            )

            features = (
                df.groupBy("repo")
                .agg(
                    count("*").alias("total_events"),

                    countDistinct("actor").alias(
                        "unique_contributors"
                    ),

                    count(
                        when(
                            col("event_type") == "PushEvent",
                            True,
                        )
                    ).alias("push_events"),

                    count(
                        when(
                            col("event_type") == "WatchEvent",
                            True,
                        )
                    ).alias("watch_events"),

                    count(
                        when(
                            col("event_type") == "IssuesEvent",
                            True,
                        )
                    ).alias("issue_events"),

                    count(
                        when(
                            col("event_type") == "PullRequestEvent",
                            True,
                        )
                    ).alias("pull_request_events"),

                    avg("score").alias("avg_score"),

                    avg("comments").alias("avg_comments"),

                    max("created_at").alias("latest_event"),
                )
                .fillna(0)
                .withColumn(
                    "feature_created_at",
                    current_timestamp(),
                )
            )

            logger.info("Feature engineering completed.")

            return features

        except Exception as e:
            logger.exception(f"Feature engineering failed: {e}")
            raise

    def create_feature_table(self):
        """
        Create Iceberg feature table.
        """

        logger.info(
            f"Creating feature table if not exists: {self.output_table}"
        )

        try:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.output_table}
                (
                    repo STRING,
                    total_events BIGINT,
                    unique_contributors BIGINT,
                    push_events BIGINT,
                    watch_events BIGINT,
                    issue_events BIGINT,
                    pull_request_events BIGINT,
                    avg_score DOUBLE,
                    avg_comments DOUBLE,
                    latest_event STRING,
                    feature_created_at TIMESTAMP
                )
                USING iceberg
                """
            )

            logger.info("Feature table is ready.")

        except Exception as e:
            logger.exception(f"Failed creating feature table: {e}")
            raise

    def save_features(self, features):
        """
        Save engineered features.
        """

        logger.info(
            f"Writing engineered features to {self.output_table}"
        )

        try:
            (
                features.write
                .format("iceberg")
                .mode("overwrite")
                .save(self.output_table)
            )

            logger.info("Features successfully written.")

        except Exception as e:
            logger.exception(f"Failed writing features: {e}")
            raise

    def run(self):
        """
        Execute Feature Engineering Pipeline.
        """

        logger.info("=" * 60)
        logger.info("FEATURE ENGINEERING PIPELINE STARTED")
        logger.info("=" * 60)

        try:
            df = self.read_events()

            input_count = df.count()
            logger.info(f"Input records: {input_count}")

            features = self.build_features(df)

            feature_count = features.count()
            logger.info(f"Generated feature rows: {feature_count}")

            self.create_feature_table()

            self.save_features(features)

            logger.info("=" * 60)
            logger.info("FEATURE ENGINEERING PIPELINE COMPLETED")
            logger.info("=" * 60)

        except Exception as e:
            logger.exception(
                f"Feature Engineering Pipeline failed: {e}"
            )
            raise

        finally:
            logger.info("Stopping Spark Session...")
            self.spark.stop()
            logger.info("Spark Session stopped.")


if __name__ == "__main__":
    FeatureBuilder().run()