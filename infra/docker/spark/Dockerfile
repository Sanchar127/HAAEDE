FROM apache/spark:3.5.0

USER root

# ----------------------------
# Create directories
# ----------------------------
RUN mkdir -p /opt/spark/jobs \
    /opt/spark/custom-jars \
    /jmx

# ----------------------------
# Copy Spark jobs
# ----------------------------

COPY services/spark-jobs/ /opt/spark/jobs/
COPY services/shared/ /opt/spark/jobs/common/
# ----------------------------
# Kafka + Spark SQL (Structured Streaming)
# REQUIRED FOR spark.readStream.format("kafka")
# ----------------------------
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    /opt/spark/custom-jars/

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    /opt/spark/custom-jars/

ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    /opt/spark/custom-jars/

ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    /opt/spark/custom-jars/

# ----------------------------
# Iceberg runtime (Spark 3.5)
# ----------------------------
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    /opt/spark/custom-jars/

# ----------------------------
# Hadoop AWS (S3A / MinIO support)
# ----------------------------
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/custom-jars/

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/custom-jars/

# ----------------------------
# Prometheus JMX Exporter
# ----------------------------
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
    /jmx/jmx_prometheus_javaagent.jar

# ----------------------------
# JMX Config (IMPORTANT FIXED PATH)
# ----------------------------
COPY infra/k8s/lakehouse/spark/spark-jmx-config.yaml \
    /jmx/jmx-config.yaml

# ----------------------------
# Spark classpath
# ----------------------------
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/custom-jars/*

# ----------------------------
# Fix permissions (important for Spark operator)
# ----------------------------
RUN chmod -R 777 /opt/spark/jobs \
    /opt/spark/custom-jars \
    /jmx

USER spark