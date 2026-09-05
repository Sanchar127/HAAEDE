from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergQueryDebug") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n========== ICEBERG DEBUG ==========")

print("\n1. Effective catalog configuration:")

for key in [
    "spark.sql.catalog.local",
    "spark.sql.catalog.local.type",
    "spark.sql.catalog.local.warehouse",
    "spark.sql.catalog.local.io-impl",
]:
    try:
        print(f"{key} = {spark.conf.get(key)}")
    except Exception as e:
        print(f"{key} = <NOT SET> ({e})")

print("\n2. Hadoop filesystem configuration:")

for key in [
    "spark.hadoop.fs.s3a.endpoint",
    "spark.hadoop.fs.s3a.access.key",
    "spark.hadoop.fs.s3a.secret.key",
    "spark.hadoop.fs.s3a.impl",
]:
    try:
        value = spark.conf.get(key)

        # Don't print the MinIO secret.
        if "secret" in key:
            value = "***REDACTED***"

        print(f"{key} = {value}")
    except Exception as e:
        print(f"{key} = <NOT SET> ({e})")

print("\n3. Catalogs:")
spark.sql("SHOW CATALOGS").show(truncate=False)

print("\n4. Local namespaces:")
spark.sql("SHOW NAMESPACES IN local").show(truncate=False)

print("\n5. Tables in local:")
spark.sql("SHOW TABLES IN local").show(truncate=False)

print("\n6. Querying bronze_events:")
spark.sql("""
    SELECT *
    FROM local.bronze_events
    LIMIT 20
""").show(truncate=False)

print("\n✅ Query completed successfully")

spark.stop()