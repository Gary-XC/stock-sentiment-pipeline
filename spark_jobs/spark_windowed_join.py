"""
spark_windowed_join.py

Phase 5 of the Real-Time Stock Sentiment Pipeline.
Implements watermarking, time-windowing, and stream-stream joins to align
aggregated sentiment scores with aggregated stock prices over fixed time intervals.
"""

import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp, lower, regexp_replace, window, avg, count, min, max

from pyspark.sql.pandas.functions import pandas_udf

# Suppress verbose Spark INFO logs
logging.getLogger("py4j").setLevel(logging.WARN)

def create_spark_session() -> SparkSession:
    """Initializes SparkSession with Kafka and Arrow optimizations."""
    return ( SparkSession.builder
        .appName("StockSentimentWindowedJoin")  # type: ignore
        .master("local[*]") 
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") 
        .config("spark.sql.shuffle.partitions", "2") 
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") 
        .getOrCreate()
    )

@pandas_udf(DoubleType()) # type: ignore
def get_sentiment_score(headlines: pd.Series) -> pd.Series:
    """Vectorized UDF to calculate VADER compound sentiment scores."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    return headlines.apply(lambda text: analyzer.polarity_scores(str(text))['compound'])

def main():
    """Main execution block for windowing and joining streams."""
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
    kafka_options = {
        "kafka.bootstrap.servers": "localhost:9092",
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
    }

    # ---------------------------------------------------------
    # 3. Read and Parse Streams
    # ---------------------------------------------------------
    df_prices_raw = spark.readStream.format("kafka").options(**kafka_options, subscribe="stock_prices").load()
    df_texts_raw = spark.readStream.format("kafka").options(**kafka_options, subscribe="stock_texts").load()

    df_prices = df_prices_raw.select(from_json(col("value").cast("string"), price_schema).alias("data")).select(
        col("data.ticker"), col("data.price"), to_timestamp(col("data.timestamp")).alias("timestamp")
    )
    
    df_texts = df_texts_raw.select(from_json(col("value").cast("string"), text_schema).alias("data")).select(
        col("data.ticker"), col("data.headline"), col("data.source"), to_timestamp(col("data.timestamp")).alias("timestamp")
    )

    # ---------------------------------------------------------
    # 4. Text Cleaning & Sentiment Scoring
    # ---------------------------------------------------------
    df_texts_scored = df_texts \
        .withColumn("cleaned_headline", regexp_replace(lower(col("headline")), "\\s+", " ")) \
        .withColumn("sentiment_score", get_sentiment_score(col("cleaned_headline"))) # type: ignore

    # ---------------------------------------------------------
    # 5. Watermarking & Windowing
    # ---------------------------------------------------------
    # Watermark of 1 minutes allows for late-arriving data up to 1 mins old.
    # Window of 1 minute aggregates data into 1-minute tumbling buckets.
    
    df_prices_windowed = df_prices \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            col("ticker"),
            window(col("timestamp"), "1 minute").alias("time_window")
        ) \
        .agg(
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            count("price").alias("price_count")
        )

    df_sentiment_windowed = df_texts_scored \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            col("ticker"),
            window(col("timestamp"), "1 minute").alias("time_window")
        ) \
        .agg(
            avg("sentiment_score").alias("avg_sentiment"),
            count("headline").alias("headline_count")
        )

    # ---------------------------------------------------------
    # 6. Stream-Stream Join
    # ---------------------------------------------------------
    # Joining on BOTH 'ticker' and 'time_window' ensures state is bounded 
    # and the join operation remains highly efficient and memory-safe.
    df_joined = df_prices_windowed.alias("p").join(
        df_sentiment_windowed.alias("s"),
        (col("p.ticker") == col("s.ticker")) & 
        (col("p.time_window.start") == col("s.time_window.start")) &
        (col("p.time_window.end") == col("s.time_window.end")),
        how="inner"
    ).select(
        col("p.ticker"),
        col("p.time_window"),
        col("p.avg_price"),
        col("p.min_price"),
        col("p.max_price"),
        col("p.price_count"),
        col("s.avg_sentiment"),
        col("s.headline_count")
    )

    # ---------------------------------------------------------
    # 7. Console Output (Verification Sink)
    # ---------------------------------------------------------
    print("\n--- Starting Windowed Join Stream ---")
    print("(Note: Output will appear after the first 1-minute window closes)\n")
    
    query_joined = df_joined.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .start()

    query_joined.awaitTermination()

if __name__ == "__main__":
    main()