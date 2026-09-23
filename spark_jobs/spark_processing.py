"""
spark_processing.py

Phase 4 of the Real-Time Stock Sentiment Pipeline.
Extends the ingestion layer by applying text cleaning and sentiment analysis 
to the incoming text stream using a high-performance Pandas UDF (Vectorized UDF).
"""

import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp, lower, regexp_replace
from pyspark.sql.pandas.functions import pandas_udf

# Suppress verbose Spark INFO logs to keep the console output clean
logging.getLogger("py4j").setLevel(logging.WARN)

def create_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Kafka ingestion.
    
    Returns:
        SparkSession: Configured Spark session with necessary Kafka packages.
    """
    return (SparkSession.builder
        .appName("StockSentimentProcessing") # type: ignore
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

@pandas_udf(DoubleType()) # type: ignore
def get_sentiment_score(headlines: pd.Series) -> pd.Series:
    """
    Calculates the VADER compound sentiment score for a batch of headlines.
    
    This is a Vectorized (Pandas) UDF, which processes data in columnar batches 
    via Apache Arrow, drastically reducing serialization overhead compared to 
    standard row-by-row Python UDFs.
    
    Args:
        headlines (pd.Series): A pandas Series containing text headlines.
        
    Returns:
        pd.Series: A pandas Series of float values representing the compound 
                   sentiment score (ranging from -1.0 for highly negative to 1.0 for highly positive).
    """
    # Lazy import ensures the analyzer is initialized on the executor side 
    # when this code is eventually deployed to a distributed cluster.
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    
    analyzer = SentimentIntensityAnalyzer()
    
    # Apply the sentiment analysis to each string in the batch
    # We cast to string to safely handle any unexpected nulls or non-string types
    return headlines.apply(lambda text: analyzer.polarity_scores(str(text))['compound'])

def main():
    """
    Main execution block. Reads from Kafka, applies schemas, computes sentiment, 
    and writes the enriched streams to the console for verification.
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------------
    # 1. Define Strict Schemas
    # ---------------------------------------------------------
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
    kafka_options_prices = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "stock_prices",
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
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
    df_prices = df_prices_raw.select(
        from_json(col("value").cast("string"), price_schema).alias("data")
    ).select(
        col("data.ticker"),
        col("data.price"),
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
    # 5. Text Cleaning & Sentiment Analysis (Phase 4 Addition)
    # ---------------------------------------------------------
    # Clean text: lowercase and remove multiple spaces for consistent scoring
    df_texts_cleaned = df_texts.withColumn(
        "cleaned_headline", 
        regexp_replace(lower(col("headline")), "\\s+", " ")
    )
    
    # Apply the Vectorized UDF to generate the sentiment score
    df_texts_scored = df_texts_cleaned.withColumn(
        "sentiment_score", 
        get_sentiment_score(col("cleaned_headline")) # type: ignore
    )

    # ---------------------------------------------------------
    # 6. Write to Console (Verification Sink)
    # ---------------------------------------------------------
    print("\n--- Starting Price Stream ---")
    query_prices = df_prices.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "3") \
        .queryName("prices_stream") \
        .start()
        
    print("--- Starting Text Stream (with Sentiment) ---")
    query_texts = df_texts_scored.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "3") \
        .queryName("texts_stream") \
        .start()

    # Keep the application running until manually stopped (Ctrl+C)
    query_prices.awaitTermination()
    query_texts.awaitTermination()

if __name__ == "__main__":
    main()