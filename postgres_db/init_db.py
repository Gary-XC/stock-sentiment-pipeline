"""
init_db.py

Initializes the PostgreSQL database with the target schema for the 
joined stock sentiment metrics. Uses a composite primary key to 
enable idempotent upserts.
"""

import os

import psycopg2

def init_database():
    """Connects to Postgres and creates the metrics table if it doesn't exist."""
    conn = psycopg2.connect(
        dbname="sentiment_db", user="admin", password="admin",
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5433")
        # docker postgres port is 5433, matching the docker container to have the postgres database hosted on the docker container with apache kafka and apache spark
    )

    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_sentiment_metrics (
        ticker VARCHAR(10) NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        avg_price DOUBLE PRECISION,
        min_price DOUBLE PRECISION,
        max_price DOUBLE PRECISION,
        price_count INTEGER,
        avg_sentiment DOUBLE PRECISION,
        headline_count INTEGER,
        PRIMARY KEY (ticker, window_start)
    );
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("Table 'stock_sentiment_metrics' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()