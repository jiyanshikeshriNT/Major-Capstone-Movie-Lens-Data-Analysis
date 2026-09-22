from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Default configuration for DAG tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

# MovieLens Bronze Ingestion DAG
with DAG(
    dag_id="movielens_bronze_ingestion",
    description="Ingest MovieLens raw CSV files into PostgreSQL Bronze layer using PySpark",
    default_args=default_args,
    start_date=datetime(2026, 9, 9),
    schedule="@daily",
    catchup=False,
    tags=["movielens", "bronze", "pyspark"],
) as dag:

    check_source_files = BashOperator(
        task_id="check_source_files",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.ingest_bronze check_source_files
        """
    )

    ingest_links = BashOperator(
        task_id="ingest_links",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.ingest_bronze ingest_links
        """
    )

    ingest_movies = BashOperator(
        task_id="ingest_movies",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.ingest_bronze ingest_movies
        """
    )

    ingest_tags = BashOperator(
        task_id="ingest_tags",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.ingest_bronze ingest_tags
        """
    )

    ingest_ratings = BashOperator(
        task_id="ingest_ratings",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.ingest_bronze ingest_ratings
        """
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.ingest_bronze validate_bronze
        """
    )

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="movielens_silver_transformation",
        wait_for_completion=False
    )

    (
        check_source_files
        >> ingest_links
        >> ingest_movies
        >> ingest_tags
        >> ingest_ratings
        >> validate_bronze
        >> trigger_silver_dag
    )