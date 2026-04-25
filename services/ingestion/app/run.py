from modules.ingestion.app.loader import load_dataset
from modules.ingestion.app.mapper import map_to_event
from modules.ingestion.app.producer import EventProducer

DATA_PATH = "app/data/lending_club.csv"
TOPIC = "recovery-events"

def main():
    print("📊 Loading dataset...")
    df = load_dataset(DATA_PATH)

    producer = EventProducer()

    print("🚀 Streaming events to Kafka...")

    for _, row in df.iterrows():
        event = map_to_event(row.to_dict())

        producer.send(TOPIC, event)

        print(f"📤 Sent: {event['event_type']} | {event['customer_id']}")

        time.sleep(0.2)  # simulate real-time stream

if __name__ == "__main__":
    main()