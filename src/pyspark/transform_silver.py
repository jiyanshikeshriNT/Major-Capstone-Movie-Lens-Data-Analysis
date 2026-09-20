import sys
from psycopg2 import sql

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
    enforce_schema,
    get_postgres_connection
)

from pyspark.sql.functions import (
    current_timestamp,
    regexp_extract,
    regexp_replace,
    trim,
    col,
    concat_ws,
    collect_set,
    sort_array,
    from_unixtime,
    row_number
)

from pyspark.sql.window import Window

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


def rename_columns_to_pascal_case(df):

    for column_name in df.columns:

        pascal_case_name = "".join(
            part[:1].upper() + part[1:]
            for part in column_name.split("_")
        )

        df = df.withColumnRenamed(
            column_name,
            pascal_case_name
        )

    return df


def deduplicate_data(
    df,
    key_cols=None,
    order_col=None
):

    # Exact duplicate removal
    if key_cols is None:
        return df.dropDuplicates()

    # Business-key deduplication when no ordering column is required
    if order_col is None:
        return df.dropDuplicates(key_cols)

    # Business-key deduplication:keeping latest record based on order_col
    window_spec = (
        Window
        .partitionBy(*key_cols)
        .orderBy(
            col(order_col).desc()
        )
    )

    return (
        df
        .withColumn(
            "_row_num",
            row_number().over(window_spec)
        )
        .filter(
            col("_row_num") == 1
        )
        .drop("_row_num")
    )


def ensure_unique_index(
    target_table,
    key_columns
):

    schema_name, table_name = target_table.split(".")

    index_name = (
        f"uq_{schema_name}_{table_name}_"
        f"{'_'.join(key_columns)}"
    ).lower()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:

        # Checking whether target table exists
        cursor.execute(
            "SELECT to_regclass(%s)",
            (target_table,)
        )

        table_exists = (
            cursor.fetchone()[0]
            is not None
        )

        if not table_exists:

            print(
                f"{target_table} does not exist yet. "
                "Unique index will be created after initial load."
            )

            return

        index_query = sql.SQL("""
            CREATE UNIQUE INDEX IF NOT EXISTS {}
            ON {}.{} ({})
        """).format(
            sql.Identifier(index_name),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.Identifier(column_name)
                for column_name in key_columns
            )
        )

        cursor.execute(index_query)

        conn.commit()

        print(
            f"Unique index ensured on "
            f"{target_table} for {key_columns}"
        )

    except Exception:

        conn.rollback()
        raise

    finally:

        cursor.close()
        conn.close()


def upsert_to_postgres(
    df,
    target_table,
    key_columns
):

    schema_name, table_name = target_table.split(".")

    staging_table_name = (
        f"{table_name}_staging"
    )

    staging_table = (
        f"{schema_name}.{staging_table_name}"
    )

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:

        # Check whether final Silver table already exists
        cursor.execute(
            "SELECT to_regclass(%s)",
            (target_table,)
        )

        table_exists = (
            cursor.fetchone()[0]
            is not None
        )

        # First load
        if not table_exists:

            print(
                f"{target_table} does not exist. "
                "Creating initial table."
            )

            df.write.jdbc(
                url=POSTGRES_URL,
                table=target_table,
                mode="append",
                properties=POSTGRES_PROPERTIES
            )

            ensure_unique_index(
                target_table,
                key_columns
            )

            print(
                f"{target_table} created successfully"
            )

        # Subsequent loads
        else:

            print(
                f"Preparing upsert for {target_table}"
            )

            ensure_unique_index(
                target_table,
                key_columns
            )

            # Temporary staging table can safely be overwritten
            df.write.jdbc(
                url=POSTGRES_URL,
                table=staging_table,
                mode="overwrite",
                properties=POSTGRES_PROPERTIES
            )

            print(
                f"Staging table {staging_table} "
                "created successfully"
            )

            all_columns = df.columns

            # Creating a list of columns that can be updated
            # CreateDtTm must remain unchanged for an already-existing record
            update_columns = [
                column_name
                for column_name in all_columns
                if column_name not in key_columns
                and column_name != "CreateDtTm"
            ]

            # Columns that is used to determine whether the business data has actually changed
            comparison_columns = [
                column_name
                for column_name in update_columns
                if column_name != "UpdateDtTm"
            ]

            insert_columns_sql = sql.SQL(", ").join(
                sql.Identifier(column_name)
                for column_name in all_columns
            )

            select_columns_sql = sql.SQL(", ").join(
                sql.Identifier(column_name)
                for column_name in all_columns
            )

            conflict_columns_sql = sql.SQL(", ").join(
                sql.Identifier(column_name)
                for column_name in key_columns
            )

            update_sql = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column_name),
                    sql.Identifier(column_name)
                )
                for column_name in update_columns
            )

            # Only update an existing row when its actual business values changed
            change_conditions = [
                sql.SQL(
                    "target.{} IS DISTINCT FROM "
                    "EXCLUDED.{}"
                ).format(
                    sql.Identifier(column_name),
                    sql.Identifier(column_name)
                )
                for column_name in comparison_columns
            ]

            if change_conditions:

                change_condition_sql = (
                    sql.SQL(" OR ").join(
                        change_conditions
                    )
                )

                merge_query = sql.SQL("""
                    INSERT INTO {}.{} AS target ({})
                    SELECT {}
                    FROM {}.{}
                    ON CONFLICT ({})
                    DO UPDATE
                    SET {}
                    WHERE {}
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    insert_columns_sql,
                    select_columns_sql,
                    sql.Identifier(schema_name),
                    sql.Identifier(staging_table_name),
                    conflict_columns_sql,
                    update_sql,
                    change_condition_sql
                )

            else:

                merge_query = sql.SQL("""
                    INSERT INTO {}.{} ({})
                    SELECT {}
                    FROM {}.{}
                    ON CONFLICT ({})
                    DO NOTHING
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    insert_columns_sql,
                    select_columns_sql,
                    sql.Identifier(schema_name),
                    sql.Identifier(staging_table_name),
                    conflict_columns_sql
                )

            cursor.execute(
                merge_query
            )

            conn.commit()

            print(
                f"{target_table} upsert completed "
                "successfully"
            )

            # Removing temporary staging table
            cursor.execute(
                sql.SQL(
                    "DROP TABLE IF EXISTS {}.{}"
                ).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(
                        staging_table_name
                    )
                )
            )

            conn.commit()

            print(
                f"Staging table {staging_table} removed"
            )

    except Exception:

        conn.rollback()
        raise

    finally:

        cursor.close()
        conn.close()


def transform_links(spark):

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
        source_row_count = links_df.count()

        print(
            f"Bronze links count: "
            f"{source_row_count}"
        )

        # Removing Bronze duplicates using MovieId as the business key
        links_deduplicated_df = deduplicate_data(
            links_df,
            key_cols=["movieId"]
        )

        deduplicated_row_count = (
            links_deduplicated_df.count()
        )

        print(
            f"Deduplicated links count: "
            f"{deduplicated_row_count}"
        )

        links_silver_df = rename_columns_to_pascal_case(
            links_deduplicated_df
        )

        links_silver_df = (
            links_silver_df
            .withColumn("CreateDtTm", current_timestamp())
            .withColumn("UpdateDtTm", current_timestamp())
        )

        links_silver_df.printSchema()

        # Number of duplicate rows removed from Bronze
        bad_row_count = (
            source_row_count
            - deduplicated_row_count
        )

        print(
            f"Duplicate links removed: "
            f"{bad_row_count}"
        )

        # Upsert cleaned data into Silver
        upsert_to_postgres(
            df=links_silver_df,
            target_table="silver.links",
            key_columns=["MovieId"]
        )

        # Get actual final Silver table count
        target_row_count = get_table_row_count(
            "silver.links"
        )

        print(
            f"Silver links count after upsert: "
            f"{target_row_count}"
        )

        print(
            "silver.links upsert completed successfully"
        )

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


def transform_movies(spark):

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
        source_row_count = movies_df.count()

        print(
            f"Bronze movies count: "
            f"{source_row_count}"
        )

        # Remove Bronze duplicates using MovieId
        # as the business key
        movies_deduplicated_df = deduplicate_data(
            movies_df,
            key_cols=["movieId"]
        )

        deduplicated_row_count = (
            movies_deduplicated_df.count()
        )

        print(
            f"Deduplicated movies count: "
            f"{deduplicated_row_count}"
        )

        movies_silver_df = rename_columns_to_pascal_case(
            movies_deduplicated_df
        )

        movies_silver_df = (
            movies_silver_df
            .withColumn("CreateDtTm", current_timestamp())
            .withColumn("UpdateDtTm", current_timestamp())
        )

        movies_silver_df.printSchema()

        # Number of duplicate rows removed from Bronze
        bad_row_count = (
            source_row_count
            - deduplicated_row_count
        )

        print(
            f"Duplicate movies removed: "
            f"{bad_row_count}"
        )

        # Upsert cleaned data into Silver
        upsert_to_postgres(
            df=movies_silver_df,
            target_table="silver.movies",
            key_columns=["MovieId"]
        )

        # Get actual final Silver row count
        target_row_count = get_table_row_count(
            "silver.movies"
        )

        print(
            f"Silver movies count after upsert: "
            f"{target_row_count}"
        )

        print(
            "silver.movies upsert completed successfully"
        )

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


def transform_ratings(spark):

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

        ## Keep only the latest rating for each UserId + MovieId combination
        ratings_deduplicated_df = deduplicate_data(
            ratings_df,
            key_cols=[
                "userId",
                "movieId"
            ],
            order_col="timestamp"
        )

        deduplicated_row_count = (
            ratings_deduplicated_df.count()
        )

        print(
            f"Deduplicated ratings count: "
            f"{deduplicated_row_count}"
        )

        ratings_silver_df = rename_columns_to_pascal_case(
            ratings_deduplicated_df
        )

        ratings_silver_df = (
            ratings_silver_df
            .withColumn(
                "Timestamp",
                from_unixtime(
                    col("Timestamp")
                ).cast("timestamp")
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

        ratings_silver_df.printSchema()

        # Rows removed during deduplication
        bad_row_count = (
            source_row_count
            - deduplicated_row_count
        )

        print(
            f"Duplicate ratings removed: "
            f"{bad_row_count}"
        )

        # Upsert cleaned ratings into Silver = UserId + MovieId identifies one user's current rating for a movie
        upsert_to_postgres(
            df=ratings_silver_df,
            target_table="silver.ratings",
            key_columns=[
                "UserId",
                "MovieId"
            ]
        )

        # Get actual final count after upsert
        target_row_count = get_table_row_count(
            "silver.ratings"
        )

        print(
            f"Silver ratings count after upsert: "
            f"{target_row_count}"
        )

        print(
            "silver.ratings upsert completed successfully"
        )

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


def transform_tags(spark):

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

        # Removing exact duplicate tag events
        tags_deduplicated_df = deduplicate_data(tags_df)

        deduplicated_row_count = (
            tags_deduplicated_df.count()
        )

        print(
            f"Deduplicated tags count: "
            f"{deduplicated_row_count}"
        )

        tags_silver_df = rename_columns_to_pascal_case(
            tags_deduplicated_df
        )

        tags_silver_df = (
            tags_silver_df
            .withColumn(
                "Timestamp",
                from_unixtime(
                    col("Timestamp")
                ).cast("timestamp")
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

        tags_silver_df.printSchema()

        # Number of duplicate rows removed
        bad_row_count = (
            source_row_count
            - deduplicated_row_count
        )

        print(
            f"Duplicate tags removed: "
            f"{bad_row_count}"
        )

        # Upsert cleaned tag events into Silver
        upsert_to_postgres(
            df=tags_silver_df,
            target_table="silver.tags",
            key_columns=[
                "UserId",
                "MovieId",
                "Tag",
                "Timestamp"
            ]
        )

        # Get actual final Silver table count
        target_row_count = get_table_row_count(
            "silver.tags"
        )

        print(
            f"Silver tags count after upsert: "
            f"{target_row_count}"
        )

        print(
            "silver.tags upsert completed successfully"
        )

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


def transform_movie_metadata(spark):

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

        source_row_count = movies_row_count

        transformed_row_count = (
            movie_metadata_df.count()
        )

        print(
            f"Prepared movie_metadata count: "
            f"{transformed_row_count}"
        )

        # Since MovieId is unique in silver.movies,
        # one movie_metadata row is expected per movie.
        bad_row_count = (
            source_row_count
            - transformed_row_count
        )

        # Upsert derived metadata into Silver
        upsert_to_postgres(
            df=movie_metadata_df,
            target_table="silver.movie_metadata",
            key_columns=[
                "MovieId"
            ]
        )

        # Get actual final table count after upsert
        target_row_count = get_table_row_count(
            "silver.movie_metadata"
        )

        print(
            f"Silver movie_metadata count "
            f"after upsert: {target_row_count}"
        )

        print(
            "silver.movie_metadata "
            "upsert completed successfully"
        )

        write_transformation_log(
            source_table="silver.movies + silver.links",
            target_table="silver.movie_metadata",
            source_row_count=movies_row_count,
            target_row_count=target_row_count,
            bad_row_count=bad_row_count,
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


def transform_user_ratings_master(spark):

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
                    sort_array(
                            collect_set("Tag")
                    )
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

            # Upsert each batch into the final Silver table
            # instead of overwrite for first batch
            # and append for remaining batches.
            upsert_to_postgres(
                df=user_ratings_master_batch_df,
                target_table=(
                    "silver.user_ratings_master"
                ),
                key_columns=[
                    "UserId",
                    "MovieId"
                ]
            )

            print(
                f"Batch {start_user_id} - "
                f"{end_user_id} "
                "upserted successfully"
            )

        print(
            "silver.user_ratings_master upsert completed successfully"
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

        # Since LEFT JOINs are used, every Silver rating is expected to remain in the master table
        bad_row_count = max(
            source_row_count
            - target_row_count,
            0
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
            bad_row_count=bad_row_count,
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

if __name__ == "__main__":

    spark = create_spark_session()
    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC"
    )

    try:

        if len(sys.argv) == 1:
            # Run complete Silver pipeline manually
            transform_links(spark)
            transform_movies(spark)
            transform_ratings(spark)
            transform_tags(spark)
            transform_movie_metadata(spark)
            transform_user_ratings_master(spark)

        else:

            task_name = sys.argv[1]

            if task_name == "transform_links":
                transform_links(spark)

            elif task_name == "transform_movies":
                transform_movies(spark)

            elif task_name == "transform_ratings":
                transform_ratings(spark)

            elif task_name == "transform_tags":
                transform_tags(spark)

            elif task_name == "transform_movie_metadata":
                transform_movie_metadata(spark)

            elif task_name == "transform_user_ratings_master":
                transform_user_ratings_master(spark)

            else:
                raise ValueError(
                    f"Unknown task: {task_name}"
                )
        
    finally:
        spark.stop()