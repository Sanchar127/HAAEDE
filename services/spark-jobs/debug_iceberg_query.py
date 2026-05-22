from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergQueryDebug") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# QUERY ICEBERG TABLE
# -----------------------------
df = spark.sql("""
SELECT *
FROM local.bronze_events
LIMIT 20
""")

df.show(truncate=False)

print("✅ Query completed successfully")