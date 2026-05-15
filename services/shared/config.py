KAFKA_BOOTSTRAP_SERVERS = "recovery-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
KAFKA_TOPIC = "fintech.debt.raw"

ICEBERG_CATALOG = "local"
ICEBERG_WAREHOUSE = "s3a://warehouse/"
CHECKPOINT_PATH = "/tmp/checkpoints"