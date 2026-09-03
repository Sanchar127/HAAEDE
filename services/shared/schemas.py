from pyspark.sql.types import StructType, StringType

EVENT_SCHEMA = StructType() \
    .add("source", StringType()) \
    .add("title", StringType()) \
    .add("timestamp", StringType())