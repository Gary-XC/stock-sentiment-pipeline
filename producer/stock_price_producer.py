"""
stock_price_producer.py

Continuously fetches real-time stock price data using yfinance and publishes
it to a Kafka topic. Designed for high reliability with idempotent producer
configurations and graceful shutdown handling.
"""

import json
import time
import signal
import sys
import os
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer
import yfinance as yf

# Configure logging for production-grade observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_flag = False

def delivery_report(err, msg):
    """
    Callback function for Kafka message delivery reports.
    
    Args:
        err (KafkaError): Error object if delivery failed, else None.
        msg (Message): The message object that was delivered or failed.
    """
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

def signal_handler(sig, frame):
    """
    Handles SIGINT/SIGTERM for graceful shutdown.
    """
    global shutdown_flag
    logger.info("Shutdown signal received. Flushing producer and exiting...")
    shutdown_flag = True

def create_producer() -> Producer:
    """
    Initializes and returns a Confluent Kafka Producer with production-grade configs.
    
    Returns:
        Producer: Configured Kafka producer instance.
    """
    conf = {
        'bootstrap.servers': f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}",
        'client.id': 'stock_price_producer',
        # Idempotence ensures exactly-once delivery to the broker
        'enable.idempotence': True, 
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 300
    }
    return Producer(conf)

def fetch_and_produce(producer: Producer, ticker_symbol: str, topic: str):
    """
    Fetches the latest stock price and produces it to Kafka.
    
    Args:
        producer (Producer): The Kafka producer instance.
        ticker_symbol (str): The stock ticker to fetch (e.g., 'AAPL').
        topic (str): The Kafka topic to publish to.
    """
    try:
        # Using fast_info for low-latency price fetching without downloading full history
        ticker = yf.Ticker(ticker_symbol)
        price = ticker.fast_info['last_price']
        
        payload = {
            "ticker": ticker_symbol,
            "price": round(price, 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Serialize to JSON and produce
        producer.produce(
            topic, 
            key=ticker_symbol.encode('utf-8'), 
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        
        # Trigger delivery reports (non-blocking)
        producer.poll(0)
        
        logger.info(f"Produced: {payload}")
        
    except Exception as e:
        logger.error(f"Error fetching/producing data for {ticker_symbol}: {e}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    TICKER = "AAPL"
    TOPIC = "stock_prices"
    INTERVAL_SECONDS = 5  # Fetch every 5 seconds

    logger.info(f"Starting Stock Price Producer for {TICKER} to topic {TOPIC}...")
    producer = create_producer()

    try:
        while not shutdown_flag:
            fetch_and_produce(producer, TICKER, TOPIC)
            time.sleep(INTERVAL_SECONDS)
    finally:
        # Flush ensures all buffered messages are sent before exiting
        logger.info("Flushing remaining messages...")
        producer.flush(timeout=10)
        logger.info("Producer shut down gracefully.")