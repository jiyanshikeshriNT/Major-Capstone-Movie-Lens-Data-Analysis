import psycopg2

from pyspark.sql import SparkSession

from src.python.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)

# PostgreSQL Connection
def get_postgres_connection():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

# Spark Session
def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("MovieLens Bronze Ingestion")
        .config(
            "spark.jars",
            "drivers/postgresql-42.7.13.jar"
        )
        .getOrCreate()
    )

    print("Spark session created successfully")

    return spark


# Ingestion logging for bronze.ingestion_log
def write_ingestion_log(
    file_name,
    target_table,
    row_count,
    status,
    error_message=None
):

    try:

        conn = get_postgres_connection()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO bronze.ingestion_log
            (
                file_name,
                target_table,
                row_count,
                status,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_query,
            (
                file_name,
                target_table,
                row_count,
                status,
                error_message
            )
        )

        conn.commit()

        cursor.close()
        conn.close()

        print(
            f"Ingestion log written successfully for {file_name}"
        )

    except Exception as e:

        print(
            f"Failed to write ingestion log for {file_name}: {e}"
        )


# Transformation logging for silver.transformation_log
def write_transformation_log(
    source_table,
    target_table,
    source_row_count,
    target_row_count,
    bad_row_count,
    status,
    error_message=None
):

    try:

        conn = get_postgres_connection()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO silver.transformation_log
            (
                source_table,
                target_table,
                source_row_count,
                target_row_count,
                bad_row_count,
                status,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_query,
            (
                source_table,
                target_table,
                source_row_count,
                target_row_count,
                bad_row_count,
                status,
                error_message
            )
        )

        conn.commit()

        cursor.close()
        conn.close()

        print(
            f"Transformation log written successfully for {target_table}"
        )

    except Exception as e:

        print(
            f"Failed to write transformation log for {target_table}: {e}"
        )