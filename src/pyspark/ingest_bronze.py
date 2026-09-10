import sys

from pyspark.sql.types import(
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType, 
    DoubleType
)

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


#Ingesting links.csv table
def ingest_links():

    spark = create_spark_session()

    try:
        links_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("imdbId", StringType(), True),
            StructField("tmdbId", StringType(), True)
        ])

        links_path = str(DATA_DIR / "links.csv")

        links_df = (
            spark.read
            .option("header", True)
            .schema(links_schema)
            .csv(links_path)
        )

        links_row_count = links_df.count()

        print(
            f"links.csv row count: {links_row_count}"
        )

        links_df.write.jdbc(
            url=POSTGRES_URL,
            table="bronze.links",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "links.csv loaded successfully into bronze.links"
        )

        write_ingestion_log(
            "links.csv",
            "bronze.links",
            links_row_count,
            "SUCCESS"
        )

    except Exception as e:
        write_ingestion_log(
            "links.csv",
            "bronze.links",
            0,
            "FAILED",
            str(e)
        )
        raise
    finally:
        spark.stop()


#Ingesting movies.csv table
def ingest_movies():

    spark = create_spark_session()
    try:
        movies_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True)
        ])
        movies_path = str(DATA_DIR / "movies.csv")
        movies_df = (
            spark.read
            .option("header", True)
            .schema(movies_schema)
            .csv(movies_path)
        )

        movies_row_count = movies_df.count()

        print(
            f"movies.csv row count: {movies_row_count}"
        )

        movies_df.write.jdbc(
            url=POSTGRES_URL,
            table="bronze.movies",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "movies.csv loaded successfully into bronze.movies"
        )

        write_ingestion_log(
            "movies.csv",
            "bronze.movies",
            movies_row_count,
            "SUCCESS"
        )

    except Exception as e:
        write_ingestion_log(
            "movies.csv",
            "bronze.movies",
            0,
            "FAILED",
            str(e)
        )
        raise
    finally:
        spark.stop()


#Ingesting tags.csv
def ingest_tags():

    spark = create_spark_session()

    try:
        tags_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("tag", StringType(), True),
            StructField("timestamp", LongType(), True)
        ])

        tags_path = str(DATA_DIR / "tags.csv")

        tags_df = (
            spark.read
            .option("header", True)
            .schema(tags_schema)
            .csv(tags_path)
        )

        tags_row_count = tags_df.count()

        print(
            f"tags.csv row count: {tags_row_count}"
        )

        tags_df.write.jdbc(
            url=POSTGRES_URL,
            table="bronze.tags",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "tags.csv loaded successfully into bronze.tags"
        )

        write_ingestion_log(
            "tags.csv",
            "bronze.tags",
            tags_row_count,
            "SUCCESS"
        )

    except Exception as e:
        write_ingestion_log(
            "tags.csv",
            "bronze.tags",
            0,
            "FAILED",
            str(e)
        )
        raise
    finally:
        spark.stop()



#Checking if ratings file hasalready been loaded
def is_file_already_loaded(file_name):

    conn = get_postgres_connection()
    cursor = conn.cursor()

    query = """
        SELECT COUNT(*)
        FROM bronze.ingestion_log
        WHERE file_name = %s
          AND target_table = 'bronze.ratings'
          AND status = 'SUCCESS'
    """

    cursor.execute(
        query,
        (file_name,)
    )

    count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return count > 0


#Ingesting ratings.csv increamentally
def ingest_ratings():

    spark = create_spark_session()

    ratings_schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    ratings_files = [
        "ratings_part1.csv",
        "ratings_part2.csv",
        "ratings_part3.csv",
        "ratings_part4.csv",
        "ratings_part5.csv"
    ]

    try:
        for file_name in ratings_files:
            print(f"\nProcessing {file_name}")
            if is_file_already_loaded(file_name):

                print(
                    f"{file_name} was already successfully loaded"
                )

                print(
                    "Skipping file to prevent duplicate records"
                )

                continue

            ratings_path = str(
                DATA_DIR / file_name
            )

            ratings_df = (
                spark.read
                .option("header", True)
                .schema(ratings_schema)
                .csv(ratings_path)
            )

            ratings_row_count = ratings_df.count()

            print(
                f"{file_name} row count: "
                f"{ratings_row_count}"
            )

            ratings_df.write.jdbc(
                url=POSTGRES_URL,
                table="bronze.ratings",
                mode="append",
                properties=POSTGRES_PROPERTIES
            )

            print(
                f"{file_name} loaded successfully "
                "into bronze.ratings"
            )

            write_ingestion_log(
                file_name,
                "bronze.ratings",
                ratings_row_count,
                "SUCCESS"
            )

    except Exception as e:
        print(
            f"Ratings ingestion failed: {e}"
        )
        raise
    finally:
        spark.stop()


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
            cursor.close()
            conn.close()
            raise ValueError(
                f"Validation failed: "
                f"bronze.{table_name} contains 0 rows"
            )

    cursor.close()
    conn.close()

    print(
        "\nBronze validation completed successfully."
    )


if __name__ == "__main__":

    if len(sys.argv) == 1:
        # Run complete Bronze pipeline manually
        check_source_files()
        ingest_links()
        ingest_movies()
        ingest_tags()
        ingest_ratings()
        validate_bronze()

    else:
        task_name = sys.argv[1]

        if task_name == "check_source_files":
            check_source_files()

        elif task_name == "ingest_links":
            ingest_links()

        elif task_name == "ingest_movies":
            ingest_movies()

        elif task_name == "ingest_tags":
            ingest_tags()

        elif task_name == "ingest_ratings":
            ingest_ratings()

        elif task_name == "validate_bronze":
            validate_bronze()

        else:
            raise ValueError(f"Unknown task: {task_name}")  