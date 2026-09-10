from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="movielens_silver_transformation",
    description="Transform MovieLens Bronze tables into cleaned Silver tables using PySpark",
    default_args=default_args,
    start_date=datetime(2026, 9, 9),
    schedule=None,
    catchup=False,
    tags=["movielens", "silver", "pyspark"],
) as dag:

    transform_links = BashOperator(
        task_id="transform_links",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_silver transform_links
        """
    )

    transform_movies = BashOperator(
        task_id="transform_movies",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_silver transform_movies
        """
    )

    transform_ratings = BashOperator(
        task_id="transform_ratings",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_silver transform_ratings
        """
    )

    transform_tags = BashOperator(
        task_id="transform_tags",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_silver transform_tags
        """
    )

    (
        transform_links
        >> transform_movies
        >> transform_ratings
        >> transform_tags
    )