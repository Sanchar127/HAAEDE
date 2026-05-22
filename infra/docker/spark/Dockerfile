FROM apache/spark:3.5.0

USER root

# ----------------------------
# Create app directory
# ----------------------------
RUN mkdir -p /opt/spark/jobs

# ----------------------------
# Copy Spark jobs into image
# ----------------------------
COPY services/spark-jobs/ /opt/spark/jobs/

# ----------------------------
# Create jars directory
# ----------------------------
RUN mkdir -p /opt/spark/custom-jars

# ----------------------------
# Kafka + Spark SQL connector
# ----------------------------
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    /opt/spark/custom-jars/

ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    /opt/spark/custom-jars/

# ----------------------------
# Iceberg
# ----------------------------
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    /opt/spark/custom-jars/

# ----------------------------
# Hadoop S3A (MinIO support)
# ----------------------------
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/custom-jars/

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/custom-jars/

# ----------------------------
# Classpath setup
# ----------------------------
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/custom-jars/*

# ----------------------------
# Permissions
# ----------------------------
RUN chmod -R 777 /opt/spark/jobs

USER spark