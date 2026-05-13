from collections import defaultdict

trend_scores = defaultdict(int)

def process_event(event: dict):
    """
    Process incoming Kafka event
    """

    source = event.get("source", "unknown")

    trend_scores[source] += 1

    return {
        "source": source,
        "trend_score": trend_scores[source]
    }