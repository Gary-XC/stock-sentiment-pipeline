"""
stock_pipeline_dag.py

Phase 7: Airflow Orchestration DAG.
Manages the lifecycle of the Real-Time Stock Sentiment Pipeline.
It verifies infrastructure health, initializes the database schema,
and triggers the data producers and Spark streaming job.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
import psycopg2
import logging

# Default arguments for the DAG
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 10),
    "retries": 1,
}


def check_postgres_health():
    """
    Verifies connectivity to the PostgreSQL database.
    Raises an exception if the connection fails, marking the Airflow task as failed.
    """
    try:
        # Note: Airflow containers use the Docker network DNS to reach the 'postgres' container
        conn = psycopg2.connect(
            dbname="sentiment_db",
            user="admin",
            password="admin",
            host="postgres",  # Docker DNS resolution
            port="5432",  # Internal container port
        )
        conn.close()
        logging.info("✅ PostgreSQL health check passed.")
    except Exception as e:
        logging.error(f"❌ PostgreSQL health check failed: {e}")
        raise


with DAG(
    dag_id="stock_pipeline_orchestrator",  # type: ignore
    default_args=default_args,
    description="Orchestrates the Stock Sentiment streaming pipeline",
    schedule_interval=None,  # Triggered manually via UI # type: ignore
    catchup=False,
    tags=["streaming", "kafka", "spark", "postgres"],
) as dag:

    # Task 1: Verify Database Health
    check_db = PythonOperator(
        task_id="check_postgres_health",
        python_callable=check_postgres_health,
    )

    # Task 2: Initialize Database Schema (Updated path to postgres_db folder)
    init_db = BashOperator(
        task_id="initialize_database",
        bash_command="export DB_HOST=postgres DB_PORT=5432 && python /opt/airflow/project/postgres_db/init_db.py",
    )

    # Task 3: Start Price Producer in the background
    start_price_producer = BashOperator(
        task_id="start_price_producer",
        bash_command="export KAFKA_HOST=kafka KAFKA_PORT=29092 && nohup python /opt/airflow/project/producer/text_sentiment_producer.py > /tmp/text_prod.log 2>&1 &",
    )

    # Task 4: Start Text Producer in the background
    start_text_producer = BashOperator(
        task_id="start_text_producer",
        bash_command="export KAFKA_HOST=kafka KAFKA_PORT=29092 && nohup python /opt/airflow/project/producer/stock_price_producer.py > /tmp/price_prod.log 2>&1 &",
    )

    # Task 5: Start Spark Streaming Job in the background
    start_spark_job = BashOperator(
        task_id="start_spark_sink",
        bash_command="export DB_HOST=postgres DB_PORT=5432 KAFKA_HOST=kafka KAFKA_PORT=29092 && nohup python /opt/airflow/project/spark_jobs/spark_postgres_sink.py > /tmp/spark_job.log 2>&1 &",
    )

    # Define Task Dependencies
    check_db >> init_db >> [start_price_producer, start_text_producer, start_spark_job]  # type: ignore
