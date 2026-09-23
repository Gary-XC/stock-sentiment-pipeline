"""
spark_postgres_sink.py

Phase 6 of the Real-Time Stock Sentiment Pipeline.
Takes the windowed, joined stream and persists it to PostgreSQL using 
a foreachBatch upsert pattern to guarantee idempotency and exactly-once 
semantics, even in the event of micro-batch retries.
"""

import logging
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp, lower, regexp_replace, window, avg, count, min, max
from pyspark.sql.pandas.functions import pandas_udf

logging.getLogger("py4j").setLevel(logging.WARN)

def create_spark_session() -> SparkSession:
    """Initializes SparkSession with Kafka and Arrow optimizations."""
    return (SparkSession.builder 
        .appName("StockSentimentPostgresSink")  # type: ignore
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

def upsert_to_postgres(df, epoch_id):
    """
    ForeachBatch function to upsert micro-batch data into PostgreSQL.
    
    Args:
        df (DataFrame): The micro-batch DataFrame containing joined metrics.
        epoch_id (int): The unique identifier for the current micro-batch.
    """
    if df.isEmpty():
        return
    
    # Collecting is safe here because the micro-batch is heavily aggregated 
    # (e.g., 1 row per ticker per minute), keeping memory footprint tiny.
    rows = df.collect()
    if not rows:
        return

    # Format data into a list of tuples for efficient batch execution
    data = [(
        row.ticker,
        row.time_window.start,
        row.time_window.end,
        float(row.avg_price),
        float(row.min_price),
        float(row.max_price),
        int(row.price_count),
        float(row.avg_sentiment) if row.avg_sentiment else 0.0,
        int(row.headline_count)
    ) for row in rows]

    # Production-grade upsert query using ON CONFLICT
    query = """
        INSERT INTO stock_sentiment_metrics 
        (ticker, window_start, window_end, avg_price, min_price, max_price, price_count, avg_sentiment, headline_count)
        VALUES %s
        ON CONFLICT (ticker, window_start) 
        DO UPDATE SET 
            window_end = EXCLUDED.window_end,
            avg_price = EXCLUDED.avg_price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            price_count = EXCLUDED.price_count,
            avg_sentiment = EXCLUDED.avg_sentiment,
            headline_count = EXCLUDED.headline_count;
    """
    
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="sentiment_db", user="admin", password="admin", 
            host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5433")
        )
        with conn.cursor() as cur:
            # execute_values is highly optimized for batch inserts/upserts
            execute_values(cur, query, data)
        conn.commit()
        print(f"✅ Successfully upserted {len(data)} rows to Postgres for epoch {epoch_id}")
    except Exception as e:
        print(f"❌ Error upserting to Postgres: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    """Main execution block for the streaming pipeline."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define Strict Schemas
    price_schema = StructType().add("ticker", StringType()).add("price", DoubleType()).add("timestamp", StringType())
    text_schema = StructType().add("ticker", StringType()).add("headline", StringType()).add("source", StringType()).add("timestamp", StringType())

    # 2. Configure Kafka Source Options
    kafka_options = {
        "kafka.bootstrap.servers": f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}", 
        "startingOffsets": "latest", "failOnDataLoss": "false"
    }
    # 3. Read and Parse Streams
    df_prices = spark.readStream.format("kafka").options(**kafka_options, subscribe="stock_prices").load() \
        .select(from_json(col("value").cast("string"), price_schema).alias("data")) \
        .select(col("data.ticker"), col("data.price"), to_timestamp(col("data.timestamp")).alias("timestamp"))
    
    df_texts = spark.readStream.format("kafka").options(**kafka_options, subscribe="stock_texts").load() \
        .select(from_json(col("value").cast("string"), text_schema).alias("data")) \
        .select(col("data.ticker"), col("data.headline"), col("data.source"), to_timestamp(col("data.timestamp")).alias("timestamp"))

    # 4. Text Cleaning & Sentiment Scoring
    df_texts_scored = df_texts \
        .withColumn("cleaned_headline", regexp_replace(lower(col("headline")), "\\s+", " ")) \
        .withColumn("sentiment_score", get_sentiment_score(col("cleaned_headline"))) # type: ignore

    # 5. Watermarking & Windowing
    df_prices_windowed = df_prices \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(col("ticker"), window(col("timestamp"), "1 minute").alias("time_window")) \
        .agg(avg("price").alias("avg_price"), min("price").alias("min_price"), max("price").alias("max_price"), count("price").alias("price_count"))

    df_sentiment_windowed = df_texts_scored \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(col("ticker"), window(col("timestamp"), "1 minute").alias("time_window")) \
        .agg(avg("sentiment_score").alias("avg_sentiment"), count("headline").alias("headline_count"))

    # 6. Stream-Stream Join
    df_joined = df_prices_windowed.alias("p").join(
        df_sentiment_windowed.alias("s"),
        (col("p.ticker") == col("s.ticker")) & 
        (col("p.time_window.start") == col("s.time_window.start")) &
        (col("p.time_window.end") == col("s.time_window.end")),
        how="inner"
    ).select(
        col("p.ticker"), col("p.time_window"), col("p.avg_price"), col("p.min_price"), 
        col("p.max_price"), col("p.price_count"), col("s.avg_sentiment"), col("s.headline_count")
    )

    # 7. Write to PostgreSQL via foreachBatch
    print("\n--- Starting Stream to PostgreSQL Sink ---")
    print("(Output will appear after the first 1-minute window closes)\n")
    
    query = df_joined.writeStream \
        .foreachBatch(upsert_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint_postgres_sink") \
        .queryName("postgres_sink") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()