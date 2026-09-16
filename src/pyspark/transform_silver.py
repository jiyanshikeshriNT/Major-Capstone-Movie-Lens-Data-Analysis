import sys

from pyspark.sql.types import(
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType, 
    DoubleType
)

from src.python.config import (
    POSTGRES_URL,
    POSTGRES_PROPERTIES,
)

from src.python.utils import (
    create_spark_session,
    write_transformation_log,
    get_table_row_count,
    enforce_schema
)

from pyspark.sql.functions import (
    current_timestamp,
    regexp_extract,
    regexp_replace,
    trim,
    col,
    concat_ws,
    collect_set,
    from_unixtime
)

# Expected schemas for Silver layer
links_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("imdbId", IntegerType(), True),
    StructField("tmdbId", IntegerType(), True)
])

movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

tags_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), True)
])


def read_bronze_table(spark, table_name):
    """
    Read a Bronze PostgreSQL table into a PySpark DataFrame
    """

    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        properties=POSTGRES_PROPERTIES
    )

    return df


def transform_links():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        links_df = read_bronze_table(
            spark,
            "bronze.links"
        )

        links_df = enforce_schema(
            links_df,
            links_schema
        )

        print("bronze.links loaded successfully")
        print(f"Bronze links count: {links_df.count()}")

        links_silver_df = (
            links_df
            .dropDuplicates()
            .withColumnRenamed("movieId", "MovieId")
            .withColumnRenamed("imdbId", "ImdbId")
            .withColumnRenamed("tmdbId", "TmdbId")
            .withColumn("CreateDtTm", current_timestamp())
            .withColumn("UpdateDtTm", current_timestamp())
        )

        links_silver_df.printSchema()
        links_silver_df.show(5, truncate=False)

        source_row_count = links_df.count()
        target_row_count = links_silver_df.count()
        bad_row_count = source_row_count - target_row_count

        print(f"Silver links count: {target_row_count}")

        links_silver_df.write.jdbc(
            url=POSTGRES_URL,
            table="silver.links",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.links written successfully to PostgreSQL")

        write_transformation_log(
            source_table="bronze.links",
            target_table="silver.links",
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            bad_row_count=bad_row_count,
            status="SUCCESS"
        )

    except Exception as e:

        write_transformation_log(
            source_table="bronze.links",
            target_table="silver.links",
            source_row_count=0,
            target_row_count=0,
            bad_row_count=0,
            status="FAILED",
            error_message=str(e)
        )

        raise

    finally:
        spark.stop()


def transform_movies():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:

        movies_df = read_bronze_table(
            spark,
            "bronze.movies"
        )

        movies_df = enforce_schema(
            movies_df,
            movies_schema
        )

        print("bronze.movies loaded successfully")
        print(f"Bronze movies count: {movies_df.count()}")

        movies_silver_df = (
        movies_df
            .dropDuplicates()
            .withColumnRenamed("movieId", "MovieId")
            .withColumnRenamed("title", "Title")
            .withColumnRenamed("genres", "Genres")
            .withColumn("CreateDtTm", current_timestamp())
            .withColumn("UpdateDtTm", current_timestamp())
        )

        movies_silver_df.printSchema()
        movies_silver_df.show(5, truncate=False)

        source_row_count = movies_df.count()
        target_row_count = movies_silver_df.count()
        bad_row_count = source_row_count - target_row_count

        print(f"Silver movies count: {target_row_count}")

        movies_silver_df.write.jdbc(
             url=POSTGRES_URL,
            table="silver.movies",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.movies written successfully to PostgreSQL")

        write_transformation_log(
            source_table="bronze.movies",
            target_table="silver.movies",
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            bad_row_count=bad_row_count,
            status="SUCCESS"
        )

    except Exception as e:

        write_transformation_log(
            source_table="bronze.movies",
            target_table="silver.movies",
            source_row_count=0,
            target_row_count=0,
            bad_row_count=0,
            status="FAILED",
            error_message=str(e)
        )

        raise

    finally:
        spark.stop()


def transform_ratings():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        #Reading ratings file differently than other files, in chunks as it is huge
        ratings_df = (
            spark.read
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", "bronze.ratings")
            .option("user", POSTGRES_PROPERTIES["user"])
            .option("password", POSTGRES_PROPERTIES["password"])
            .option("driver", POSTGRES_PROPERTIES["driver"])
            .option("partitionColumn", "userId")
            .option("lowerBound", "1")
            .option("upperBound", "200948")
            .option("numPartitions", "32")
            .option("fetchsize", "10000")
            .load()
        )

        ratings_df = enforce_schema(
            ratings_df,
            ratings_schema
        )

        print("bronze.ratings loaded successfully")
        print(f"Number of Spark partitions:" f"{ratings_df.rdd.getNumPartitions()}")

        source_row_count = ratings_df.count()

        print(f"Bronze ratings count: {source_row_count}")

        ratings_silver_df = (
            ratings_df
            .withColumnRenamed("userId", "UserId")
            .withColumnRenamed("movieId", "MovieId")
            .withColumnRenamed("rating", "Rating")
            .withColumn(
                "timestamp",
                from_unixtime(col("timestamp")).cast("timestamp")
            )
            .withColumnRenamed("timestamp", "Timestamp")
            .withColumn("CreateDtTm", current_timestamp())
            .withColumn("UpdateDtTm", current_timestamp())
        )

        ratings_silver_df.printSchema()
        ratings_silver_df.show(5, truncate=False)

        target_row_count = ratings_silver_df.count()
        bad_row_count = source_row_count - target_row_count

        print(f"Silver ratings count: {target_row_count}")

        ratings_silver_df.write.jdbc(
            url=POSTGRES_URL,
            table="silver.ratings",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.ratings written successfully to PostgreSQL")

        write_transformation_log(
            source_table="bronze.ratings",
            target_table="silver.ratings",
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            bad_row_count=bad_row_count,
            status="SUCCESS"
        )

    except Exception as e:

        write_transformation_log(
            source_table="bronze.ratings",
            target_table="silver.ratings",
            source_row_count=0,
            target_row_count=0,
            bad_row_count=0,
            status="FAILED",
            error_message=str(e)
        )

        raise

    finally:

        spark.stop()


def transform_tags():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:

        tags_df = read_bronze_table(
            spark,
            "bronze.tags"
        )

        tags_df = enforce_schema(
            tags_df,
            tags_schema
        )

        print("bronze.tags loaded successfully")

        source_row_count = tags_df.count()

        print(f"Bronze tags count: {source_row_count}")

        tags_silver_df = (
            tags_df
            .dropDuplicates()
            .withColumnRenamed("userId", "UserId")
            .withColumnRenamed("movieId", "MovieId")
            .withColumnRenamed("tag", "Tag")
            .withColumn(
                "timestamp",
                from_unixtime(col("timestamp")).cast("timestamp")
            )
            .withColumnRenamed("timestamp", "Timestamp")
            .withColumn("CreateDtTm", current_timestamp())
            .withColumn("UpdateDtTm", current_timestamp())
        )

        tags_silver_df.printSchema()
        tags_silver_df.show(5, truncate=False)

        target_row_count = tags_silver_df.count()
        bad_row_count = source_row_count - target_row_count

        print(f"Silver tags count: {target_row_count}")

        tags_silver_df.write.jdbc(
            url=POSTGRES_URL,
            table="silver.tags",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.tags written successfully to PostgreSQL")

        write_transformation_log(
            source_table="bronze.tags",
            target_table="silver.tags",
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            bad_row_count=bad_row_count,
            status="SUCCESS"
        )

    except Exception as e:

        write_transformation_log(
            source_table="bronze.tags",
            target_table="silver.tags",
            source_row_count=0,
            target_row_count=0,
            bad_row_count=0,
            status="FAILED",
            error_message=str(e)
        )

        raise

    finally:

        spark.stop()


def transform_movie_metadata():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        # Reading existing Silver tables
        movies_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="silver.movies",
            properties=POSTGRES_PROPERTIES
        )

        links_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="silver.links",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.movies loaded successfully")
        print("silver.links loaded successfully")

        movies_row_count = movies_df.count()
        links_row_count = links_df.count()

        print(f"Silver movies count: {movies_row_count}")
        print(f"Silver links count: {links_row_count}")

        # Selecting only required columns
        movies_selected_df = movies_df.select(
            "MovieId",
            "Title",
            "Genres"
        )

        links_selected_df = links_df.select(
            "MovieId",
            "ImdbId",
            "TmdbId"
        )

        # Performing joins, LEFT JOIN preserves all movies even if an identifier is missing
        movie_metadata_df = (
            movies_selected_df
            .join(
                links_selected_df,
                on="MovieId",
                how="left"
            )
            .withColumn(
                "ReleaseYear",
                regexp_extract(
                    col("Title"),
                    r"\((\d{4})\)\s*$",
                    1
                )
            )
        )

        # regexp_extract returns an empty string when year is unavailable then convert the extracted year to integer
        movie_metadata_df = movie_metadata_df.withColumn(
            "ReleaseYear",
            col("ReleaseYear").cast("integer")
        )

        movie_metadata_df = movie_metadata_df.withColumn(
            "Title",
            trim(
                regexp_replace(
                    col("Title"),
                    r"\s*\(\d{4}\)\s*$",
                    ""
                )
            )
        )

        # Arranging columns according to the required movie_metadata structure and add new audit timestamps
        movie_metadata_df = (
            movie_metadata_df
            .select(
                "MovieId",
                "Title",
                "ReleaseYear",
                "Genres",
                "ImdbId",
                "TmdbId"
            )
            .withColumn(
                "CreateDtTm",
                current_timestamp()
            )
            .withColumn(
                "UpdateDtTm",
                current_timestamp()
            )
        )

        movie_metadata_df.printSchema()
        movie_metadata_df.show(5, truncate=False)

        target_row_count = movie_metadata_df.count()

        print(
            f"silver.movie_metadata count: "
            f"{target_row_count}"
        )

        # Writing derived Silver asset to PostgreSQL
        movie_metadata_df.write.jdbc(
            url=POSTGRES_URL,
            table="silver.movie_metadata",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "silver.movie_metadata written successfully to PostgreSQL"
        )

        write_transformation_log(
            source_table="silver.movies + silver.links",
            target_table="silver.movie_metadata",
            source_row_count=movies_row_count,
            target_row_count=target_row_count,
            bad_row_count=movies_row_count - target_row_count,
            status="SUCCESS"
        )

    except Exception as e:

        write_transformation_log(
            source_table="silver.movies + silver.links",
            target_table="silver.movie_metadata",
            source_row_count=0,
            target_row_count=0,
            bad_row_count=0,
            status="FAILED",
            error_message=str(e)
        )

        raise

    finally:

        spark.stop()


def transform_user_ratings_master():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:

        # Reading smaller Silver tables once
        tags_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="silver.tags",
            properties=POSTGRES_PROPERTIES
        )

        movie_metadata_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="silver.movie_metadata",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.tags loaded successfully")
        print("silver.movie_metadata loaded successfully")

        # Aggregating multiple tags for same UserId + MovieId
        tags_aggregated_df = (
            tags_df
            .groupBy(
                "UserId",
                "MovieId"
            )
            .agg(
                concat_ws(
                    ", ",
                    collect_set("Tag")
                ).alias("Tags")
            )
        )

        print(
            "Tags aggregated successfully for UserId + MovieId"
        )

        # Selecting required metadata columns
        metadata_selected_df = (
            movie_metadata_df
            .select(
                "MovieId",
                "Title",
                "ReleaseYear",
                "Genres",
                "ImdbId",
                "TmdbId"
            )
        )

        # Processing ratings in UserId batches
        min_user_id = 1
        max_user_id = 200948
        batch_size = 25000

        first_batch = True

        for start_user_id in range(
            min_user_id,
            max_user_id + 1,
            batch_size
        ):

            end_user_id = min(
                start_user_id + batch_size - 1,
                max_user_id
            )

            print(
                f"Processing UserId range: "
                f"{start_user_id} - {end_user_id}"
            )

            # Reading only current ratings batch
            ratings_batch_df = (
                spark.read
                .format("jdbc")
                .option("url", POSTGRES_URL)
                .option(
                    "dbtable",
                    f"""
                    (
                        SELECT *
                        FROM silver.ratings
                        WHERE "UserId"
                        BETWEEN {start_user_id}
                        AND {end_user_id}
                    ) AS ratings_batch
                    """
                )
                .option("user", POSTGRES_PROPERTIES["user"])
                .option("password", POSTGRES_PROPERTIES["password"])
                .option("driver", POSTGRES_PROPERTIES["driver"])
                .option("fetchsize","10000")
                .load()
            )

            print(
                f"Ratings batch loaded for "
                f"{start_user_id} - {end_user_id}"
            )

            # Filtering tags to same UserId range
            tags_batch_df = (
                tags_aggregated_df
                .filter(
                    (col("UserId") >= start_user_id)
                    &
                    (col("UserId") <= end_user_id)
                )
            )

            # Joining ratings with movie metadata
            user_ratings_master_batch_df = (
                ratings_batch_df
                .join(
                    metadata_selected_df,
                    on="MovieId",
                    how="left"
                )
            )

            # Joining aggregated tags
            user_ratings_master_batch_df = (
                user_ratings_master_batch_df
                .join(
                    tags_batch_df,
                    on=[
                        "UserId",
                        "MovieId"
                    ],
                    how="left"
                )
            )

            # Renaming rating timestamp
            user_ratings_master_batch_df = (
                user_ratings_master_batch_df
                .withColumnRenamed(
                    "Timestamp",
                    "RatingTstmp"
                )
            )

            # Selecting required columns
            user_ratings_master_batch_df = (
                user_ratings_master_batch_df
                .select(
                    "UserId",
                    "MovieId",
                    "Title",
                    "ReleaseYear",
                    "Genres",
                    "Tags",
                    "Rating",
                    "RatingTstmp",
                    "ImdbId",
                    "TmdbId"
                )
                .withColumn(
                    "CreateDtTm",
                    current_timestamp()
                )
                .withColumn(
                    "UpdateDtTm",
                    current_timestamp()
                )
            )

            # First batch overwrites old table Remaining batches append
            write_mode = (
                "overwrite"
                if first_batch
                else "append"
            )

            user_ratings_master_batch_df.write.jdbc(
                url=POSTGRES_URL,
                table="silver.user_ratings_master",
                mode=write_mode,
                properties=POSTGRES_PROPERTIES
            )

            print(
                f"Batch {start_user_id} - "
                f"{end_user_id} written successfully"
            )

            first_batch = False

        print(
            "silver.user_ratings_master written successfully to PostgreSQL"
        )

        source_row_count = get_table_row_count(
            "silver.ratings"
        )

        target_row_count = get_table_row_count(
            "silver.user_ratings_master"
        )

        print(
            f"Source ratings count: "
            f"{source_row_count}"
        )

        print(
            f"Target user_ratings_master count: "
            f"{target_row_count}"
        )

        write_transformation_log(
            source_table=(
                "silver.ratings + "
                "silver.tags + "
                "silver.movie_metadata"
            ),
            target_table="silver.user_ratings_master",
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            bad_row_count=(
                source_row_count
                - target_row_count
            ),
            status="SUCCESS"
        )

    except Exception as e:

        write_transformation_log(
            source_table=(
                "silver.ratings + "
                "silver.tags + "
                "silver.movie_metadata"
            ),
            target_table="silver.user_ratings_master",
            source_row_count=0,
            target_row_count=0,
            bad_row_count=0,
            status="FAILED",
            error_message=str(e)
        )

        raise

    finally:

        spark.stop()

if __name__ == "__main__":

    if len(sys.argv) == 1:

        # Run complete Silver pipeline manually
        transform_links()
        transform_movies()
        transform_ratings()
        transform_tags()
        transform_movie_metadata()
        transform_user_ratings_master()


    else:

        task_name = sys.argv[1]

        if task_name == "transform_links":
            transform_links()

        elif task_name == "transform_movies":
            transform_movies()

        elif task_name == "transform_ratings":
            transform_ratings()

        elif task_name == "transform_tags":
            transform_tags()

        elif task_name == "transform_movie_metadata":
            transform_movie_metadata()

        elif task_name == "transform_user_ratings_master":
            transform_user_ratings_master()

        else:
            raise ValueError(
                f"Unknown task: {task_name}"
            )