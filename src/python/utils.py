import psycopg2

from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col

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
        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )
        .config(
            "spark.driver.memory",
            "2g"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "64"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
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

# Function to get the table row count for audit purpose
def get_table_row_count(table_name):

    conn = None
    cursor = None

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        query = f"SELECT COUNT(*) FROM {table_name}"

        cursor.execute(query)

        row_count = cursor.fetchone()[0]

        return row_count

    finally:

        if cursor:
            cursor.close()

        if conn:
            conn.close()


def write_gold_transformation_log(
    source_tables,
    target_table,
    status,
    target_row_count=None,
    error_message=None
):
    conn = None
    cursor = None

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        query = """
            INSERT INTO gold.transformation_log (
                source_tables,
                target_table,
                status,
                target_row_count,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        cursor.execute(
            query,
            (
                source_tables,
                target_table,
                status,
                target_row_count,
                error_message
            )
        )

        conn.commit()

        print(
            f"Gold transformation log written successfully "
            f"for {target_table}"
        )

    except Exception as e:
        print(
            f"Failed to write Gold transformation log: {e}"
        )
        raise

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

            
def enforce_schema(df, schema):

    expected_columns = [
        field.name
        for field in schema.fields
    ]

    missing_columns = [
        column
        for column in expected_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    for field in schema.fields:
        df = df.withColumn(
            field.name,
            col(field.name).cast(field.dataType)
        )

    return df
