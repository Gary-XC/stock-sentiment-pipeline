"""
text_sentiment_producer.py

Generates and publishes simulated financial news headlines to a Kafka topic.
Simulates a real-time text stream for sentiment analysis processing.
"""

import json
import time
import os
import random
import signal
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

shutdown_flag = False

# Simulated financial headlines mapped to tickers to ensure realistic sentiment distribution
SIMULATED_HEADLINES = [
    "Apple announces record-breaking iPhone sales in Q4.",
    "Analysts downgrade Apple stock citing supply chain concerns.",
    "Apple's new Vision Pro headset receives mixed early reviews.",
    "Strong services revenue boosts Apple's profit margins.",
    "Apple faces new antitrust scrutiny in the European Union.",
]


def delivery_report(err, msg):
    """Callback for Kafka delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def signal_handler(sig, frame):
    """Handles graceful shutdown."""
    global shutdown_flag
    logger.info("Shutdown signal received. Flushing producer...")
    shutdown_flag = True


def create_producer() -> Producer:
    """Initializes the Kafka producer."""
    conf = {
        "bootstrap.servers": f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}",
        "client.id": "text_sentiment_producer",
        "enable.idempotence": True,
        "acks": "all",
    }
    return Producer(conf)


def generate_and_produce(producer: Producer, topic: str):
    """
    Generates a simulated headline and produces it to Kafka.

    Args:
        producer (Producer): Kafka producer instance.
        topic (str): Target Kafka topic.
    """
    try:
        # ticker = random.choice(list(SIMULATED_HEADLINES.keys())) # type: ignore
        headline = random.choice(SIMULATED_HEADLINES)  # type: ignore

        payload = {
            "ticker": "AAPL",
            "headline": headline,
            "source": "simulated_news_api",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        producer.produce(
            topic,
            key="AAPL".encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

        logger.info(f"Produced Text: [{'AAPL'}] {headline}")

    except Exception as e:
        logger.error(f"Error producing text data: {e}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    TOPIC = "stock_texts"
    INTERVAL_SECONDS = 3  # Generate text slightly faster than prices

    logger.info(f"Starting Text Sentiment Producer to topic {TOPIC}...")
    producer = create_producer()

    try:
        while not shutdown_flag:
            generate_and_produce(producer, TOPIC)
            time.sleep(INTERVAL_SECONDS)
    finally:
        producer.flush(timeout=10)
        logger.info("Text Producer shut down gracefully.")
