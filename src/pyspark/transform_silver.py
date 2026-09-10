import sys

from src.python.config import (
    POSTGRES_URL,
    POSTGRES_PROPERTIES,
)

from src.python.utils import (
    create_spark_session,
    write_transformation_log
)

from pyspark.sql.functions import current_timestamp

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

        print("bronze.ratings loaded successfully")
        print(f"Number of Spark partitions:" f"{ratings_df.rdd.getNumPartitions()}")

        source_row_count = ratings_df.count()

        print(f"Bronze ratings count: {source_row_count}")

        ratings_silver_df = (
            ratings_df
            .withColumnRenamed("userId", "UserId")
            .withColumnRenamed("movieId", "MovieId")
            .withColumnRenamed("rating", "Rating")
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

        print("bronze.tags loaded successfully")

        source_row_count = tags_df.count()

        print(f"Bronze tags count: {source_row_count}")

        tags_silver_df = (
            tags_df
            .dropDuplicates()
            .withColumnRenamed("userId", "UserId")
            .withColumnRenamed("movieId", "MovieId")
            .withColumnRenamed("tag", "Tag")
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


if __name__ == "__main__":

    if len(sys.argv) == 1:

        # Run complete Silver pipeline manually
        transform_links()
        transform_movies()
        transform_ratings()
        transform_tags()

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

        else:
            raise ValueError(
                f"Unknown task: {task_name}"
            )