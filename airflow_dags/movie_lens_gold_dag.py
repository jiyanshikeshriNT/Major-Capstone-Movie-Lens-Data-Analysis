from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="movielens_gold_transformation",
    description="Create Gold insight tables from MovieLens Silver layer",
    default_args=default_args,
    start_date=datetime(2026, 9, 12),
    schedule=None,
    catchup=False,
    tags=["movielens", "gold", "pyspark"],
) as dag:

    transform_movie_insight = BashOperator(
        task_id="transform_movie_insight",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_gold transform_movie_insight
        """
    )

    transform_user_insight = BashOperator(
        task_id="transform_user_insight",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_gold transform_user_insight
        """
    )

    transform_genre_insight = BashOperator(
        task_id="transform_genre_insight",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_gold transform_genre_insight
        """
    )

    transform_yearly_insight = BashOperator(
        task_id="transform_yearly_insight",
        bash_command="""
        cd /opt/movielens &&
        python -m src.pyspark.transform_gold transform_yearly_insight
        """
    )

    [
        transform_movie_insight >> \
        transform_user_insight >> \
        transform_genre_insight >> \
        transform_yearly_insight
    ]