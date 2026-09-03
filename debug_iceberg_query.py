from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("debug").getOrCreate()

df = spark.sql("SELECT * FROM local.bronze_events LIMIT 20")
df.show(truncate=False)
