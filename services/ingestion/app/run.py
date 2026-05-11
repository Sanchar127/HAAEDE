import os
# from loader import load_data
from producer import stream_to_kafka

TOPIC_NAME = os.getenv("TOPIC_NAME", "fintech.debt.raw")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def start_ingestion():
    print("Starting Layer 1 Ingestion...")

    # chunks = load_data()

    # if chunks:
    #     for i, chunk in enumerate(chunks):
    #         print(f"Processing chunk {i+1}...")
    #         stream_to_kafka(TOPIC_NAME, chunk)

    # print("Ingestion complete.")


if __name__ == "__main__":
    start_ingestion()