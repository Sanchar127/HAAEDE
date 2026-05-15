from pyspark.sql import SparkSession
from .config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE

def create_spark(app_name: str):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
                ICEBERG_WAREHOUSE) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark