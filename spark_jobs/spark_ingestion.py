"""
spark_ingestion.py

Phase 3 of the Real-Time Stock Sentiment Pipeline.
Initializes PySpark Structured Streaming to ingest raw JSON messages from 
Kafka topics ('stock_prices' and 'stock_texts'), applies strict schemas, 
and parses them into structured DataFrames. 

This script acts as the foundational ingestion layer, verifying data flow 
by writing the parsed streams to the console.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp

# Suppress verbose Spark INFO logs to keep the console output clean
logging.getLogger("py4j").setLevel(logging.WARN)

def create_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Kafka ingestion.
    
    Returns:
        SparkSession: Configured Spark session with necessary Kafka packages.
    """
    return (
        SparkSession.builder
        .appName("StockSentimentIngestion") # type: ignore
        .master("local[*]")
        # Download the Kafka SQL package matching our PySpark version (Scala 2.12)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        # Reduce shuffle partitions for local testing to avoid overhead
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def main():
    """
    Main execution block for the ingestion job.
    Reads from Kafka, applies schemas, and writes to console.
    """
    spark = create_spark_session()
    # Set Spark context log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN") 

    # ---------------------------------------------------------
    # 1. Define Strict Schemas
    # ---------------------------------------------------------
    # We use StringType for timestamps initially to safely parse the raw JSON,
    # then explicitly cast to TimestampType to ensure timezone awareness.
    price_schema = StructType() \
        .add("ticker", StringType()) \
        .add("price", DoubleType()) \
        .add("timestamp", StringType())
        
    text_schema = StructType() \
        .add("ticker", StringType()) \
        .add("headline", StringType()) \
        .add("source", StringType()) \
        .add("timestamp", StringType())

    # ---------------------------------------------------------
    # 2. Configure Kafka Source Options
    # ---------------------------------------------------------
    # startingOffsets="latest" ensures we only process new data arriving 
    # after the job starts, preventing reprocessing of old data on restart.
    kafka_options_prices = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "stock_prices",
        "startingOffsets": "latest",
        "failOnDataLoss": "false" # Prevents job crash if a topic/partition is temporarily lost
    }
    
    kafka_options_texts = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "stock_texts",
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
    }

    # ---------------------------------------------------------
    # 3. Read Streams from Kafka
    # ---------------------------------------------------------
    df_prices_raw = spark.readStream.format("kafka").options(**kafka_options_prices).load()
    df_texts_raw = spark.readStream.format("kafka").options(**kafka_options_texts).load()

    # ---------------------------------------------------------
    # 4. Parse JSON and Cast Types
    # ---------------------------------------------------------
    # Kafka values are binary. We cast to string, parse JSON using the schema,
    # and then select the nested fields.
    df_prices = df_prices_raw.select(
        from_json(col("value").cast("string"), price_schema).alias("data")
    ).select(
        col("data.ticker"),
        col("data.price"),
        # Explicitly cast string to timestamp for proper time-series operations later
        to_timestamp(col("data.timestamp")).alias("timestamp") 
    )
    
    df_texts = df_texts_raw.select(
        from_json(col("value").cast("string"), text_schema).alias("data")
    ).select(
        col("data.ticker"),
        col("data.headline"),
        col("data.source"),
        to_timestamp(col("data.timestamp")).alias("timestamp")
    )

    # ---------------------------------------------------------
    # 5. Write to Console (Verification Sink)
    # ---------------------------------------------------------
    print("\n--- Starting Price Stream ---")
    query_prices = df_prices.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .queryName("prices_stream") \
        .start()
        
    print("--- Starting Text Stream ---")
    query_texts = df_texts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .queryName("texts_stream") \
        .start()

    # Keep the application running until manually stopped (Ctrl+C)
    query_prices.awaitTermination()
    query_texts.awaitTermination()

if __name__ == "__main__":
    main()