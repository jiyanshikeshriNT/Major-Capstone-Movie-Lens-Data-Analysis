import sys

from src.python.config import(
    POSTGRES_URL,
    POSTGRES_PROPERTIES,
    DATA_DIR,
    EXPECTED_FILES
)

from src.python.utils import(
    get_postgres_connection,
    create_spark_session,
    write_ingestion_log
)

#Checking source files
def check_source_files():
    print("\nChecking MovieLens source files")
    for file_name in EXPECTED_FILES:
        file_path = DATA_DIR / file_name
        if file_path.exists():
            print(f"FOUND: {file_name}")
        else:
            raise FileNotFoundError(
                f"Missing source file: {file_name}"
            )
    print("\nAll expected source files are available")


#Generic ingestion function for single CSV files
def ingest_csv(
    spark,
    file_name,
    target_table,
    csv_options=None
):

    try:
        file_path = str(
            DATA_DIR / file_name
        )

        reader = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
        )

        if csv_options:
            for option_name, option_value in csv_options.items():
                reader = reader.option(
                    option_name,
                    option_value
                )

        df = reader.csv(file_path)

        row_count = df.count()

        print(
            f"{file_name} row count: {row_count}"
        )

        df.write.jdbc(
            url=POSTGRES_URL,
            table=target_table,
            mode="append",
            properties=POSTGRES_PROPERTIES
        )

        print(
            f"{file_name} loaded successfully "
            f"into {target_table}"
        )

        write_ingestion_log(
            file_name,
            target_table,
            row_count,
            "SUCCESS"
        )

    except Exception as e:

        write_ingestion_log(
            file_name,
            target_table,
            0,
            "FAILED",
            str(e)
        )

        raise


#Truncating large bronzee tables which have huge amount of data before every dag trigger
def truncate_bronze_table(table_name):

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:

        # Checking whether the table already exists
        cursor.execute(
            "SELECT to_regclass(%s)",
            (f"bronze.{table_name}",)
        )

        table_exists = cursor.fetchone()[0]

        if table_exists:

            print(
                f"\nTruncating bronze.{table_name}"
            )

            cursor.execute(
                f'TRUNCATE TABLE bronze."{table_name}"'
            )

            conn.commit()

            print(
                f"bronze.{table_name} truncated successfully"
            )

        else:

            print(
                f"\nbronze.{table_name} does not exist yet. "
                "Skipping truncate."
            )

    except Exception:

        conn.rollback()
        raise

    finally:

        cursor.close()
        conn.close()


#Ingesting links.csv table
def ingest_links(spark):

    ingest_csv(
        spark=spark,
        file_name="links.csv",
        target_table="bronze.links"
    )


#Ingesting movies.csv table
def ingest_movies(spark):

    ingest_csv(
        spark=spark,
        file_name="movies.csv",
        target_table="bronze.movies",
        csv_options={
            "quote": '"',
            "escape": '"'
        }
    )


#Ingesting tags.csv
def ingest_tags(spark):

    #Tags contains around 2M rows, therefore clearing existing data before reloading to avoid unecessary growth
    truncate_bronze_table("tags")

    ingest_csv(
        spark=spark,
        file_name="tags.csv",
        target_table="bronze.tags",
        csv_options={
            "quote": '"',
            "escape": '"'
        }
    )


#Ingesting ratings.csv increamentally
def ingest_ratings(spark):

    #Ratings conatins around 32M rows, clearing existing data before loading all rating parts.
    truncate_bronze_table("ratings")

    ratings_files = sorted([
        file_name
        for file_name in EXPECTED_FILES
        if file_name.startswith("ratings_part")
        and file_name.endswith(".csv")
    ])

    for file_name in ratings_files:

        print(
            f"\nProcessing {file_name}"
        )

        # Each ratings part is loaded sequentially
        ingest_csv(
            spark=spark,
            file_name=file_name,
            target_table="bronze.ratings"
        )


#Valdating bronze tables
def validate_bronze():

    conn = get_postgres_connection()
    cursor = conn.cursor()

    tables = [
        "links",
        "movies",
        "tags",
        "ratings"
    ]

    print("\nValidating Bronze tables")
    try:
        for table_name in tables:
            cursor.execute(
                f'SELECT COUNT(*) FROM bronze."{table_name}"'
            )

            row_count = cursor.fetchone()[0]
            print(
                f"bronze.{table_name}: "
                f"{row_count} rows"
            )

            if row_count == 0:
                raise ValueError(
                    f"Validation failed: "
                    f"bronze.{table_name} contains 0 rows"
                )

    finally:
        cursor.close()
        conn.close()

    print(
        "\nBronze validation completed successfully."
    )


if __name__ == "__main__":

    if len(sys.argv) == 1:
        # Run complete Bronze pipeline manually
        check_source_files()
        spark = create_spark_session()
        try:
            ingest_links(spark)
            ingest_movies(spark)
            ingest_tags(spark)
            ingest_ratings(spark)

        finally:
            spark.stop()


        validate_bronze()

    else:
        task_name = sys.argv[1]

        if task_name == "check_source_files":
            check_source_files()

        elif task_name == "validate_bronze":
            validate_bronze()

        elif task_name in [
            "ingest_links",
            "ingest_movies",
            "ingest_tags",
            "ingest_ratings"
        ]:

            spark = create_spark_session()

            try:

                if task_name == "ingest_links":
                    ingest_links(spark)


                elif task_name == "ingest_movies":
                    ingest_movies(spark)


                elif task_name == "ingest_tags":
                    ingest_tags(spark)


                elif task_name == "ingest_ratings":
                    ingest_ratings(spark)

            finally:
                spark.stop()

        else:
            raise ValueError(f"Unknown task: {task_name}")  