import sys
import builtins
import pyspark.sql.functions as F

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    max,
    min,
    count,
    countDistinct,
    current_timestamp,
    col,
    row_number,
    when,
    from_unixtime,
    year,
    split,
    explode,
    desc,
    trim,
    sum as spark_sum
)

from src.python.config import (
    POSTGRES_URL,
    POSTGRES_PROPERTIES
)

from src.python.utils import (
    create_spark_session,
    get_table_row_count,
    write_gold_transformation_log
)


def transform_movie_insight():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    source_tables = (
        "silver.movie_metadata, "
        "silver.ratings, "
        "silver.tags"
    )

    try:

        # Reading silver.movie_metadata table
        movie_metadata_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="silver.movie_metadata",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.movie_metadata loaded successfully")

        # Reading silver.ratings, Ratings table is huge, so reading using JDBC partitions
        ratings_df = (
            spark.read
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", "silver.ratings")
            .option("user", POSTGRES_PROPERTIES["user"])
            .option("password", POSTGRES_PROPERTIES["password"])
            .option("driver", POSTGRES_PROPERTIES["driver"])
            .option("partitionColumn", "UserId")
            .option("lowerBound", "1")
            .option("upperBound", "200948")
            .option("numPartitions", "16")
            .option("fetchsize", "10000")
            .load()
        )

        print("silver.ratings loaded successfully")


        # Reading silver.tags table
        tags_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="silver.tags",
            properties=POSTGRES_PROPERTIES
        )

        print("silver.tags loaded successfully")

        # Movie-level rating aggregation, One row per MovieId
        rating_stats_df = (
            ratings_df
            .groupBy("MovieId")
            .agg(
                avg("Rating").alias("AverageRating"),
                max("Rating").alias("HighestRating"),
                min("Rating").alias("LowestRating"),
                count("*").alias("TotalRatings")
            )
        )

        print("Movie-level rating statistics calculated successfully")

        # Movie-level tag aggregation
        tag_stats_df = (
            tags_df
            .groupBy("MovieId")
            .agg(
                countDistinct("UserId").alias(
                    "TotalPeopleTagged"
                ),
                count("Tag").alias(
                    "TotalTags"
                ),
                countDistinct("Tag").alias(
                    "DistinctTags"
                )
            )
        )

        print("Movie-level tag statistics calculated successfully")

        # Finding PopularTag for each movie
        tag_frequency_df = (
            tags_df
            .filter(
                col("Tag").isNotNull() &
                (col("Tag") != "")
            )
            .groupBy(
                "MovieId",
                "Tag"
            )
            .agg(
                count("*").alias("TagFrequency")
            )
        )

        # Within each movie, highest frequency tag gets rank 1. If two tags have same frequency, alphabetical tag is used as tie-breaker
        popular_tag_window = (
            Window
            .partitionBy("MovieId")
            .orderBy(
                col("TagFrequency").desc(),
                col("Tag").asc()
            )
        )


        popular_tag_df = (
            tag_frequency_df
            .withColumn(
                "TagRank",
                row_number().over(
                    popular_tag_window
                )
            )
            .filter(
                col("TagRank") == 1
            )
            .select(
                "MovieId",
                col("Tag").alias("PopularTag")
            )
        )

        print("Popular tag calculated successfully")


        # Combining general tag statistics + popular tag
        tag_insight_df = (
            tag_stats_df
            .join(
                popular_tag_df,
                on="MovieId",
                how="left"
            )
        )

        # Selecting required columns from movie metadata
        movie_selected_df = (
            movie_metadata_df
            .select(
                "MovieId",
                "ReleaseYear",
                col("Title").alias("MovieTitle")
            )
        )

        # Joining movie metadata with rating statistics
        movie_insight_df = (
            movie_selected_df
            .join(
                rating_stats_df,
                on="MovieId",
                how="left"
            )
        )

        # Joining tag statistics
        movie_insight_df = (
            movie_insight_df
            .join(
                tag_insight_df,
                on="MovieId",
                how="left"
            )
        )

        # Filling tag counts with 0. Some movies may never have been tagged, their PopularTag can remain NULL
        movie_insight_df = (
            movie_insight_df
            .fillna(
                {
                    "TotalPeopleTagged": 0,
                    "TotalTags": 0,
                    "DistinctTags": 0
                }
            )
        )


        # Adding Gold audit columns
        movie_insight_df = (
            movie_insight_df
            .withColumn(
                "LoadTs",
                current_timestamp()
            )
            .withColumn(
                "UpdateTs",
                current_timestamp()
            )
        )

        # Final column order
        movie_insight_df = (
            movie_insight_df
            .select(
                "MovieId",
                "ReleaseYear",
                "MovieTitle",
                "AverageRating",
                "HighestRating",
                "LowestRating",
                "TotalRatings",
                "TotalPeopleTagged",
                "TotalTags",
                "DistinctTags",
                "PopularTag",
                "LoadTs",
                "UpdateTs"
            )
        )

        movie_insight_df.printSchema()

        # Writing Gold table
        movie_insight_df.write.jdbc(
            url=POSTGRES_URL,
            table="gold.movie_insight",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "gold.movie_insight written successfully to PostgreSQL"
        )

        target_row_count = get_table_row_count(
            "gold.movie_insight"
        )

        print(
            f"gold.movie_insight row count: "
            f"{target_row_count}"
        )


        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.movie_insight",
            status="SUCCESS",
            target_row_count=target_row_count,
            error_message=None
        )


    except Exception as e:

        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.movie_insight",
            status="FAILED",
            target_row_count=None,
            error_message=str(e)
        )

        raise


    finally:

        spark.stop()


def transform_user_insight():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.conf.set(
        "spark.sql.shuffle.partitions",
        "64"
    )

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "true"
    )

    source_tables = (
        "silver.ratings, "
        "silver.tags, "
        "silver.movie_metadata"
    )

    try:

        movie_metadata_df = (
            spark.read.jdbc(
                url=POSTGRES_URL,
                table="silver.movie_metadata",
                properties=POSTGRES_PROPERTIES
            )
            .select(
                "MovieId",
                "Title",
                "Genres"
            )
        )

        print(
            "silver.movie_metadata loaded successfully"
        )


        # Prepareing MovieId for Genre mapping 
        movie_genre_df = (
            movie_metadata_df
            .select(
                "MovieId",
                split(
                    col("Genres"),
                    "\\|"
                ).alias("GenreArray")
            )
            .withColumn(
                "Genre",
                explode("GenreArray")
            )
            .select(
                "MovieId",
                "Genre"
            )
            .filter(
                col("Genre").isNotNull()
                & (col("Genre") != "")
                & (
                    col("Genre")
                    != "(no genres listed)"
                )
            )
        )


        # Processing users in batches
        min_user_id = 1
        max_user_id = 200948

        batch_size = 25000

        first_batch = True

        for start_user_id in range(
            min_user_id,
            max_user_id + 1,
            batch_size
        ):

            end_user_id = builtins.min(
                start_user_id + batch_size - 1,
                max_user_id
            )

            print(
                f"\nProcessing UserId batch: "
                f"{start_user_id} - {end_user_id}"
            )

            # Reading only ratings for current users
            ratings_query = f"""
            (
                SELECT
                    "UserId",
                    "MovieId",
                    "Rating",
                    "Timestamp"
                FROM silver.ratings
                WHERE "UserId"
                BETWEEN {start_user_id}
                AND {end_user_id}
            ) AS ratings_batch
            """

            ratings_df = (
                spark.read
                .format("jdbc")
                .option(
                    "url",
                    POSTGRES_URL
                )
                .option(
                    "dbtable",
                    ratings_query
                )
                .option(
                    "user",
                    POSTGRES_PROPERTIES["user"]
                )
                .option(
                    "password",
                    POSTGRES_PROPERTIES["password"]
                )
                .option(
                    "driver",
                    POSTGRES_PROPERTIES["driver"]
                )
                .option(
                    "fetchsize",
                    "5000"
                )
                .load()
            )

            # Reading only tags for current users
            tags_query = f"""
            (
                SELECT
                    "UserId",
                    "MovieId",
                    "Tag",
                    "Timestamp"
                FROM silver.tags
                WHERE "UserId"
                BETWEEN {start_user_id}
                AND {end_user_id}
            ) AS tags_batch
            """

            tags_df = (
                spark.read
                .format("jdbc")
                .option(
                    "url",
                    POSTGRES_URL
                )
                .option(
                    "dbtable",
                    tags_query
                )
                .option(
                    "user",
                    POSTGRES_PROPERTIES["user"]
                )
                .option(
                    "password",
                    POSTGRES_PROPERTIES["password"]
                )
                .option(
                    "driver",
                    POSTGRES_PROPERTIES["driver"]
                )
                .option(
                    "fetchsize",
                    "5000"
                )
                .load()
            )


            # User rating statistics
            user_rating_stats_df = (
                ratings_df
                .groupBy(
                    "UserId"
                )
                .agg(

                    countDistinct(
                        "MovieId"
                    ).alias(
                        "NumberOfMoviesRated"
                    ),

                    avg(
                        "Rating"
                    ).alias(
                        "AverageLifetimeRating"
                    ),

                    min(
                        "Rating"
                    ).alias(
                        "LeastRatingGiven"
                    ),

                    max(
                        "Rating"
                    ).alias(
                        "HighestRatingGiven"
                    ),

                    countDistinct(
                        when(
                            col("Rating") == 5.0,
                            col("MovieId")
                        )
                    ).alias(
                        "FiveStarMovieCount"
                    )
                )
            )


            # Average ratings per year
            ratings_with_year_df = (
                ratings_df
                .select(
                    "UserId",
                    "Timestamp"
                )
                .withColumn(
                    "RatingDateTime",
                    from_unixtime(
                        col(
                            "Timestamp"
                        ).cast("long")
                    ).cast("timestamp")
                )
                .withColumn(
                    "RatingYear",
                    year(
                        "RatingDateTime"
                    )
                )
            )

            ratings_per_year_df = (
                ratings_with_year_df
                .groupBy(
                    "UserId",
                    "RatingYear"
                )
                .agg(
                    count("*").alias(
                        "RatingsInYear"
                    )
                )
            )

            average_ratings_per_year_df = (
                ratings_per_year_df
                .groupBy(
                    "UserId"
                )
                .agg(
                    avg(
                        "RatingsInYear"
                    ).alias(
                        "AverageRatingsPerYear"
                    )
                )
            )


            # Total tags used
            total_tags_df = (
                tags_df
                .groupBy(
                    "UserId"
                )
                .agg(
                    count(
                        "Tag"
                    ).alias(
                        "TotalTagsUsed"
                    )
                )
            )


            # Most used tag
            user_tag_frequency_df = (
                tags_df
                .filter(
                    col("Tag").isNotNull()
                    & (col("Tag") != "")
                )
                .groupBy(
                    "UserId",
                    "Tag"
                )
                .agg(
                    count("*").alias(
                        "TagFrequency"
                    )
                )
            )

            most_used_tag_window = (
                Window
                .partitionBy(
                    "UserId"
                )
                .orderBy(
                    col(
                        "TagFrequency"
                    ).desc(),
                    col(
                        "Tag"
                    ).asc()
                )
            )

            most_used_tag_df = (
                user_tag_frequency_df
                .withColumn(
                    "TagRank",
                    row_number().over(
                        most_used_tag_window
                    )
                )
                .filter(
                    col("TagRank") == 1
                )
                .select(
                    "UserId",
                    col(
                        "Tag"
                    ).alias(
                        "MostUsedTag"
                    )
                )
            )

            # Joing Ratings and Genres
            user_genre_ratings_df = (
                ratings_df
                .select(
                    "UserId",
                    "MovieId",
                    "Rating"
                )
                .join(
                    movie_genre_df,
                    on="MovieId",
                    how="inner"
                )
            )

            user_genre_stats_df = (
                user_genre_ratings_df
                .groupBy(
                    "UserId",
                    "Genre"
                )
                .agg(

                    countDistinct(
                        "MovieId"
                    ).alias(
                        "MoviesRatedInGenre"
                    ),

                    avg(
                        "Rating"
                    ).alias(
                        "GenreAverageRating"
                    )
                )
            )


            # Top genre
            top_genre_window = (
                Window
                .partitionBy(
                    "UserId"
                )
                .orderBy(
                    col(
                        "MoviesRatedInGenre"
                    ).desc(),
                    col(
                        "Genre"
                    ).asc()
                )
            )

            top_genre_df = (
                user_genre_stats_df
                .withColumn(
                    "GenreRank",
                    row_number().over(
                        top_genre_window
                    )
                )
                .filter(
                    col("GenreRank") == 1
                )
                .select(
                    "UserId",
                    col(
                        "Genre"
                    ).alias(
                        "TopGenre"
                    )
                )
            )


            # Best rated genre
            best_genre_window = (
                Window
                .partitionBy(
                    "UserId"
                )
                .orderBy(
                    col(
                        "GenreAverageRating"
                    ).desc(),
                    col(
                        "MoviesRatedInGenre"
                    ).desc(),
                    col(
                        "Genre"
                    ).asc()
                )
            )

            best_rated_genre_df = (
                user_genre_stats_df
                .withColumn(
                    "GenreRank",
                    row_number().over(
                        best_genre_window
                    )
                )
                .filter(
                    col("GenreRank") == 1
                )
                .select(
                    "UserId",
                    col(
                        "Genre"
                    ).alias(
                        "BestRatedGenre"
                    )
                )
            )


            # Most recently rated movie
            ratings_with_movie_df = (
                ratings_df
                .select(
                    "UserId",
                    "MovieId",
                    "Timestamp"
                )
                .join(
                    movie_metadata_df
                    .select(
                        "MovieId",
                        "Title"
                    ),
                    on="MovieId",
                    how="left"
                )
            )

            recent_rating_window = (
                Window
                .partitionBy(
                    "UserId"
                )
                .orderBy(
                    col(
                        "Timestamp"
                    ).cast(
                        "long"
                    ).desc(),
                    col(
                        "MovieId"
                    ).desc()
                )
            )

            recent_rated_movie_df = (
                ratings_with_movie_df
                .withColumn(
                    "RecentRatingRank",
                    row_number().over(
                        recent_rating_window
                    )
                )
                .filter(
                    col(
                        "RecentRatingRank"
                    ) == 1
                )
                .select(
                    "UserId",
                    col(
                        "Title"
                    ).alias(
                        "MostRecentlyRatedMovie"
                    )
                )
            )


            # Most recently tagged movie
            tags_with_movie_df = (
                tags_df
                .select(
                    "UserId",
                    "MovieId",
                    "Timestamp"
                )
                .join(
                    movie_metadata_df
                    .select(
                        "MovieId",
                        "Title"
                    ),
                    on="MovieId",
                    how="left"
                )
            )

            recent_tag_window = (
                Window
                .partitionBy(
                    "UserId"
                )
                .orderBy(
                    col(
                        "Timestamp"
                    ).cast(
                        "long"
                    ).desc(),
                    col(
                        "MovieId"
                    ).desc()
                )
            )

            recent_tagged_movie_df = (
                tags_with_movie_df
                .withColumn(
                    "RecentTagRank",
                    row_number().over(
                        recent_tag_window
                    )
                )
                .filter(
                    col(
                        "RecentTagRank"
                    ) == 1
                )
                .select(
                    "UserId",
                    col(
                        "Title"
                    ).alias(
                        "MostRecentlyTaggedMovie"
                    )
                )
            )


            # Joining all user-level insights
            user_insight_df = (
                user_rating_stats_df

                .join(
                    average_ratings_per_year_df,
                    on="UserId",
                    how="left"
                )

                .join(
                    total_tags_df,
                    on="UserId",
                    how="left"
                )

                .join(
                    most_used_tag_df,
                    on="UserId",
                    how="left"
                )

                .join(
                    top_genre_df,
                    on="UserId",
                    how="left"
                )

                .join(
                    best_rated_genre_df,
                    on="UserId",
                    how="left"
                )

                .join(
                    recent_rated_movie_df,
                    on="UserId",
                    how="left"
                )

                .join(
                    recent_tagged_movie_df,
                    on="UserId",
                    how="left"
                )
            )


            # Handling null tag count
            user_insight_df = (
                user_insight_df
                .fillna(
                    {
                        "TotalTagsUsed": 0
                    }
                )
            )


            # Adding audit columns
            user_insight_df = (
                user_insight_df
                .withColumn(
                    "LoadTs",
                    current_timestamp()
                )
                .withColumn(
                    "UpdateTs",
                    current_timestamp()
                )
            )


            # Final column order
            user_insight_df = (
                user_insight_df
                .select(
                    "UserId",
                    "NumberOfMoviesRated",
                    "AverageLifetimeRating",
                    "AverageRatingsPerYear",
                    "LeastRatingGiven",
                    "HighestRatingGiven",
                    "FiveStarMovieCount",
                    "TotalTagsUsed",
                    "MostUsedTag",
                    "TopGenre",
                    "BestRatedGenre",
                    "MostRecentlyRatedMovie",
                    "MostRecentlyTaggedMovie",
                    "LoadTs",
                    "UpdateTs"
                )
            )


            write_mode = (
                "overwrite"
                if first_batch
                else "append"
            )

            user_insight_df.write.jdbc(
                url=POSTGRES_URL,
                table="gold.user_insight",
                mode=write_mode,
                properties=POSTGRES_PROPERTIES
            )

            first_batch = False

            print(
                f"UserId batch "
                f"{start_user_id} - {end_user_id} "
                f"written successfully"
            )

        target_row_count = (
            get_table_row_count(
                "gold.user_insight"
            )
        )

        print(
            f"\ngold.user_insight row count: "
            f"{target_row_count}"
        )


        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.user_insight",
            status="SUCCESS",
            target_row_count=target_row_count,
            error_message=None
        )

        print(
            "gold.user_insight transformation "
            "completed successfully"
        )


    except Exception as e:

        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.user_insight",
            status="FAILED",
            target_row_count=None,
            error_message=str(e)
        )

        raise


    finally:

        spark.stop()


def transform_genre_insight():

    spark = create_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Reduce memory pressure during shuffle operations
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true"
    )

    source_tables = (
        "silver.movie_metadata, "
        "silver.ratings, "
        "silver.tags"
    )

    try:

        movie_metadata_df = (
            spark.read.jdbc(
                url=POSTGRES_URL,
                table="silver.movie_metadata",
                properties=POSTGRES_PROPERTIES
            )
            .select(
                "MovieId",
                "Title",
                "ReleaseYear",
                "Genres"
            )
        )

        print("silver.movie_metadata loaded successfully")


        # Preparing movie-genre mapping
        movie_genre_df = (
            movie_metadata_df
            .select(
                "MovieId",
                "Title",
                "ReleaseYear",
                F.split(
                    F.col("Genres"),
                    "\\|"
                ).alias("GenreArray")
            )
            .withColumn(
                "Genre",
                F.explode("GenreArray")
            )
            .select(
                "MovieId",
                "Title",
                "ReleaseYear",
                "Genre"
            )
            .filter(
                F.col("Genre").isNotNull()
                & (F.col("Genre") != "")
                & (F.col("Genre") != "(no genres listed)")
            )
            .cache()
        )

        print("Movie genre mapping prepared successfully")


        # Counting movies per genre
        genre_movie_count_df = (
            movie_genre_df
            .groupBy("Genre")
            .agg(
                F.countDistinct("MovieId").alias(
                    "MovieCount"
                )
            )
        )

        print("Movie count per genre calculated successfully")

        min_movie_id = 1
        max_movie_id = 292757
        batch_size = 25000

        rating_batch_results = []

        for start_movie_id in range(
            min_movie_id,
            max_movie_id + 1,
            batch_size
        ):

            end_movie_id = builtins.min(
                start_movie_id + batch_size - 1,
                max_movie_id
            )

            print(
                f"Processing MovieId batch: "
                f"{start_movie_id} - {end_movie_id}"
            )

            ratings_batch_df = (
                spark.read
                .format("jdbc")
                .option("url", POSTGRES_URL)
                .option(
                    "dbtable",
                    f"""
                    (
                        SELECT
                            "MovieId",
                            "Rating"
                        FROM silver.ratings
                        WHERE "MovieId" >= {start_movie_id}
                          AND "MovieId" <= {end_movie_id}
                    ) AS ratings_batch
                    """
                )
                .option(
                    "user",
                    POSTGRES_PROPERTIES["user"]
                )
                .option(
                    "password",
                    POSTGRES_PROPERTIES["password"]
                )
                .option(
                    "driver",
                    POSTGRES_PROPERTIES["driver"]
                )
                .option(
                    "fetchsize",
                    "10000"
                )
                .load()
            )

            genre_batch_df = (
                movie_genre_df
                .filter(
                    (F.col("MovieId") >= start_movie_id)
                    & (F.col("MovieId") <= end_movie_id)
                )
                .select(
                    "MovieId",
                    "Genre"
                )
            )

            joined_batch_df = (
                ratings_batch_df
                .join(
                    F.broadcast(genre_batch_df),
                    on="MovieId",
                    how="inner"
                )
            )

            # Genre rating statistics for current batch
            batch_stats_df = (
                joined_batch_df
                .groupBy("Genre")
                .agg(

                    F.count("*").alias(
                        "TotalRatings"
                    ),

                    F.sum("Rating").alias(
                        "RatingSum"
                    ),

                    F.sum(
                        F.when(
                            F.col("Rating") == 0.0,
                            1
                        ).otherwise(0)
                    ).alias("Rated0Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 0.5,
                            1
                        ).otherwise(0)
                    ).alias("Rated05Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 1.0,
                            1
                        ).otherwise(0)
                    ).alias("Rated1Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 1.5,
                            1
                        ).otherwise(0)
                    ).alias("Rated15Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 2.0,
                            1
                        ).otherwise(0)
                    ).alias("Rated2Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 2.5,
                            1
                        ).otherwise(0)
                    ).alias("Rated25Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 3.0,
                            1
                        ).otherwise(0)
                    ).alias("Rated3Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 3.5,
                            1
                        ).otherwise(0)
                    ).alias("Rated35Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 4.0,
                            1
                        ).otherwise(0)
                    ).alias("Rated4Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 4.5,
                            1
                        ).otherwise(0)
                    ).alias("Rated45Count"),

                    F.sum(
                        F.when(
                            F.col("Rating") == 5.0,
                            1
                        ).otherwise(0)
                    ).alias("Rated5Count")
                )
            )

            rating_batch_results.append(
                batch_stats_df
            )


        # Combining batch results
        genre_rating_stats_df = rating_batch_results[0]

        for batch_df in rating_batch_results[1:]:

            genre_rating_stats_df = (
                genre_rating_stats_df
                .unionByName(batch_df)
            )


        genre_rating_stats_df = (
            genre_rating_stats_df
            .groupBy("Genre")
            .agg(

                F.sum("TotalRatings").alias(
                    "TotalRatings"
                ),

                F.sum("RatingSum").alias(
                    "RatingSum"
                ),

                F.sum("Rated0Count").alias(
                    "Rated0Count"
                ),

                F.sum("Rated05Count").alias(
                    "Rated05Count"
                ),

                F.sum("Rated1Count").alias(
                    "Rated1Count"
                ),

                F.sum("Rated15Count").alias(
                    "Rated15Count"
                ),

                F.sum("Rated2Count").alias(
                    "Rated2Count"
                ),

                F.sum("Rated25Count").alias(
                    "Rated25Count"
                ),

                F.sum("Rated3Count").alias(
                    "Rated3Count"
                ),

                F.sum("Rated35Count").alias(
                    "Rated35Count"
                ),

                F.sum("Rated4Count").alias(
                    "Rated4Count"
                ),

                F.sum("Rated45Count").alias(
                    "Rated45Count"
                ),

                F.sum("Rated5Count").alias(
                    "Rated5Count"
                )
            )
            .withColumn(
                "AverageRating",
                F.when(
                    F.col("TotalRatings") > 0,
                    F.col("RatingSum")
                    / F.col("TotalRatings")
                )
            )
            .drop("RatingSum")
        )

        print("Genre rating statistics calculated successfully")

        movie_insight_df = (
            spark.read.jdbc(
                url=POSTGRES_URL,
                table="gold.movie_insight",
                properties=POSTGRES_PROPERTIES
            )
            .select(
                "MovieId",
                "MovieTitle",
                "ReleaseYear",
                "AverageRating",
                "TotalRatings"
            )
        )

        movie_genre_stats_df = (
            movie_genre_df
            .select(
                "MovieId",
                "Genre"
            )
            .join(
                movie_insight_df,
                on="MovieId",
                how="left"
            )
        )

        print("Movie-level Gold statistics loaded successfully")

        # Movie with most number of ratings
        most_rated_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("TotalRatings").desc_nulls_last(),
                F.col("MovieId").asc()
            )
        )

        most_rated_movie_df = (
            movie_genre_stats_df
            .filter(
                F.col("TotalRatings").isNotNull()
            )
            .withColumn(
                "Rank",
                F.row_number().over(
                    most_rated_window
                )
            )
            .filter(
                F.col("Rank") == 1
            )
            .select(
                "Genre",
                F.col("MovieTitle").alias(
                    "MostRatedMovie"
                )
            )
        )


        # Movie with least number of ratings
        least_rated_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("TotalRatings").asc_nulls_last(),
                F.col("MovieId").asc()
            )
        )

        least_rated_movie_df = (
            movie_genre_stats_df
            .filter(
                F.col("TotalRatings").isNotNull()
            )
            .withColumn(
                "Rank",
                F.row_number().over(
                    least_rated_window
                )
            )
            .filter(
                F.col("Rank") == 1
            )
            .select(
                "Genre",
                F.col("MovieTitle").alias(
                    "LeastRatedMovie"
                )
            )
        )


        # Highest avg rated movie, Minimum 100 ratings
        highest_avg_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("AverageRating").desc_nulls_last(),
                F.col("TotalRatings").desc_nulls_last(),
                F.col("MovieId").asc()
            )
        )

        highest_avg_movie_df = (
            movie_genre_stats_df
            .filter(
                F.col("TotalRatings") >= 100
            )
            .withColumn(
                "Rank",
                F.row_number().over(
                    highest_avg_window
                )
            )
            .filter(
                F.col("Rank") == 1
            )
            .select(
                "Genre",
                F.col("MovieTitle").alias(
                    "HighestAverageRatedMovie"
                )
            )
        )

        # Worst rated movie, If multiple movies have same average rating, taking the most recent movie.
        worst_rated_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("AverageRating").asc_nulls_last(),
                F.col("ReleaseYear").desc_nulls_last(),
                F.col("MovieId").desc()
            )
        )

        worst_rated_movie_df = (
            movie_genre_stats_df
            .filter(
                F.col("AverageRating").isNotNull()
            )
            .withColumn(
                "Rank",
                F.row_number().over(
                    worst_rated_window
                )
            )
            .filter(
                F.col("Rank") == 1
            )
            .select(
                "Genre",
                F.col("MovieTitle").alias(
                    "WorstRatedMovie"
                )
            )
        )


        # Top 10 popular movies, Popular = highest number of ratings
        popular_movies_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("TotalRatings").desc_nulls_last(),
                F.col("MovieId").asc()
            )
        )

        top_popular_ranked_df = (
            movie_genre_stats_df
            .filter(
                F.col("TotalRatings").isNotNull()
            )
            .withColumn(
                "Rank",
                F.row_number().over(
                    popular_movies_window
                )
            )
            .filter(
                F.col("Rank") <= 10
            )
        )

        top_10_popular_df = (
            top_popular_ranked_df
            .groupBy("Genre")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            "Rank",
                            "MovieTitle"
                        )
                    )
                ).alias("PopularMovieStruct")
            )
            .withColumn(
                "Top10PopularMovies",
                F.expr(
                    "transform("
                    "PopularMovieStruct, "
                    "x -> x.MovieTitle)"
                )
            )
            .drop("PopularMovieStruct")
        )


        # Top 10 highly rated movies
        highly_rated_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("AverageRating").desc_nulls_last(),
                F.col("TotalRatings").desc_nulls_last(),
                F.col("MovieId").asc()
            )
        )

        top_highly_rated_ranked_df = (
            movie_genre_stats_df
            .filter(
                F.col("AverageRating").isNotNull()
            )
            .withColumn(
                "Rank",
                F.row_number().over(
                    highly_rated_window
                )
            )
            .filter(
                F.col("Rank") <= 10
            )
        )

        top_10_highly_rated_df = (
            top_highly_rated_ranked_df
            .groupBy("Genre")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            "Rank",
                            "MovieTitle"
                        )
                    )
                ).alias("RatedMovieStruct")
            )
            .withColumn(
                "Top10HighlyRatedMovies",
                F.expr(
                    "transform("
                    "RatedMovieStruct, "
                    "x -> x.MovieTitle)"
                )
            )
            .drop("RatedMovieStruct")
        )


        # Popular tag
        tags_df = (
            spark.read
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option(
                "dbtable",
                "silver.tags"
            )
            .option(
                "user",
                POSTGRES_PROPERTIES["user"]
            )
            .option(
                "password",
                POSTGRES_PROPERTIES["password"]
            )
            .option(
                "driver",
                POSTGRES_PROPERTIES["driver"]
            )
            .option(
                "partitionColumn",
                "UserId"
            )
            .option(
                "lowerBound",
                "1"
            )
            .option(
                "upperBound",
                "200948"
            )
            .option(
                "numPartitions",
                "16"
            )
            .option(
                "fetchsize",
                "10000"
            )
            .load()
            .select(
                "MovieId",
                "Tag"
            )
            .filter(
                F.col("Tag").isNotNull()
                & (F.col("Tag") != "")
            )
        )


        tag_genre_df = (
            tags_df
            .join(
                F.broadcast(
                    movie_genre_df.select(
                        "MovieId",
                        "Genre"
                    )
                ),
                on="MovieId",
                how="inner"
            )
        )


        genre_tag_frequency_df = (
            tag_genre_df
            .groupBy(
                "Genre",
                "Tag"
            )
            .agg(
                F.count("*").alias(
                    "TagFrequency"
                )
            )
        )


        popular_tag_window = (
            Window
            .partitionBy("Genre")
            .orderBy(
                F.col("TagFrequency").desc(),
                F.col("Tag").asc()
            )
        )


        popular_tag_df = (
            genre_tag_frequency_df
            .withColumn(
                "Rank",
                F.row_number().over(
                    popular_tag_window
                )
            )
            .filter(
                F.col("Rank") == 1
            )
            .select(
                "Genre",
                F.col("Tag").alias(
                    "PopularTag"
                )
            )
        )

        print("Popular tag per genre calculated successfully")


        # Combining all genre insights
        genre_insight_df = (
            genre_movie_count_df

            .join(
                genre_rating_stats_df,
                on="Genre",
                how="left"
            )

            .join(
                popular_tag_df,
                on="Genre",
                how="left"
            )

            .join(
                most_rated_movie_df,
                on="Genre",
                how="left"
            )

            .join(
                least_rated_movie_df,
                on="Genre",
                how="left"
            )

            .join(
                highest_avg_movie_df,
                on="Genre",
                how="left"
            )

            .join(
                top_10_popular_df,
                on="Genre",
                how="left"
            )

            .join(
                top_10_highly_rated_df,
                on="Genre",
                how="left"
            )

            .join(
                worst_rated_movie_df,
                on="Genre",
                how="left"
            )
        )


        # Handling null counts
        genre_insight_df = (
            genre_insight_df
            .fillna(
                {
                    "TotalRatings": 0,
                    "Rated0Count": 0,
                    "Rated05Count": 0,
                    "Rated1Count": 0,
                    "Rated15Count": 0,
                    "Rated2Count": 0,
                    "Rated25Count": 0,
                    "Rated3Count": 0,
                    "Rated35Count": 0,
                    "Rated4Count": 0,
                    "Rated45Count": 0,
                    "Rated5Count": 0
                }
            )
        )

        genre_insight_df = (
            genre_insight_df
            .withColumn(
                "LoadTs",
                F.current_timestamp()
            )
            .withColumn(
                "UpdateTs",
                F.current_timestamp()
            )
        )

        genre_insight_df = (
            genre_insight_df
            .select(
                "Genre",
                "MovieCount",
                "TotalRatings",
                "AverageRating",
                "PopularTag",
                "Rated0Count",
                "Rated05Count",
                "Rated1Count",
                "Rated15Count",
                "Rated2Count",
                "Rated25Count",
                "Rated3Count",
                "Rated35Count",
                "Rated4Count",
                "Rated45Count",
                "Rated5Count",
                "MostRatedMovie",
                "LeastRatedMovie",
                "HighestAverageRatedMovie",
                "Top10PopularMovies",
                "Top10HighlyRatedMovies",
                "WorstRatedMovie",
                "LoadTs",
                "UpdateTs"
            )
        )


        genre_insight_df.printSchema()

        genre_insight_df.write.jdbc(
            url=POSTGRES_URL,
            table="gold.genre_insight",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "gold.genre_insight written successfully to PostgreSQL"
        )


        target_row_count = get_table_row_count(
            "gold.genre_insight"
        )

        print(
            f"gold.genre_insight row count: "
            f"{target_row_count}"
        )


        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.genre_insight",
            status="SUCCESS",
            target_row_count=target_row_count,
            error_message=None
        )

        print(
            "gold.genre_insight transformation "
            "completed successfully"
        )


    except Exception as e:

        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.genre_insight",
            status="FAILED",
            target_row_count=None,
            error_message=str(e)
        )

        raise


    finally:

        movie_genre_df.unpersist()

        spark.stop()


def transform_yearly_insight():

    spark = create_spark_session()
    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC"
    )

    source_tables = (
        "silver.movie_metadata, "
        "silver.ratings, "
        "silver.tags"
    )

    try:

        # Reading movie metadata
        movie_metadata_df = (
            spark.read.jdbc(
                url=POSTGRES_URL,
                table="silver.movie_metadata",
                properties=POSTGRES_PROPERTIES
            )
            .select(
                "MovieId",
                "Title",
                "ReleaseYear",
                "Genres"
            )
            .filter(
                col("ReleaseYear").isNotNull()
            )
            .withColumnRenamed(
                "ReleaseYear",
                "Year"
            )
        )

        print(
            "silver.movie_metadata loaded successfully"
        )

        # Persist because it is reused many times
        movie_metadata_df = (
            movie_metadata_df
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        movie_metadata_df.count()

        # Number of movies released in each year
        movies_released_df = (
            movie_metadata_df
            .groupBy("Year")
            .agg(
                countDistinct("MovieId").alias(
                    "NumberOfMoviesReleased"
                )
            )
        )

        print(
            "Movie count per release year calculated successfully"
        )

        # Find maximum MovieId for batching
        max_movie_id = (
            movie_metadata_df
            .agg(
                max("MovieId").alias(
                    "MaxMovieId"
                )
            )
            .first()["MaxMovieId"]
        )

        max_movie_id = int(max_movie_id)

        print(
            f"Maximum MovieId: {max_movie_id}"
        )

        batch_size = 25000

        movie_rating_parts = []
        year_tag_parts = []

        # Process ratings and tags in MovieId batches
        for start_movie_id in range(
            1,
            max_movie_id + 1,
            batch_size
        ):

            end_movie_id = (
                start_movie_id
                + batch_size
                - 1
            )

            print(
                f"Processing MovieId batch: "
                f"{start_movie_id} - {end_movie_id}"
            )

            batch_metadata_df = (
                movie_metadata_df
                .filter(
                    (col("MovieId") >= start_movie_id)
                    &
                    (col("MovieId") <= end_movie_id)
                )
                .select(
                    "MovieId",
                    "Year",
                    "Title",
                    "Genres"
                )
            )

            # Reading ratings for current MovieId batch
            ratings_query = f"""
            (
                SELECT
                    "MovieId",
                    "Rating"
                FROM silver.ratings
                WHERE "MovieId"
                BETWEEN {start_movie_id}
                AND {end_movie_id}
            ) AS ratings_batch
            """

            ratings_batch_df = (
                spark.read
                .format("jdbc")
                .option(
                    "url",
                    POSTGRES_URL
                )
                .option(
                    "dbtable",
                    ratings_query
                )
                .option(
                    "user",
                    POSTGRES_PROPERTIES["user"]
                )
                .option(
                    "password",
                    POSTGRES_PROPERTIES["password"]
                )
                .option(
                    "driver",
                    POSTGRES_PROPERTIES["driver"]
                )
                .option(
                    "partitionColumn",
                    "MovieId"
                )
                .option(
                    "lowerBound",
                    str(start_movie_id)
                )
                .option(
                    "upperBound",
                    str(end_movie_id)
                )
                .option(
                    "numPartitions",
                    "4"
                )
                .option(
                    "fetchsize",
                    "10000"
                )
                .load()
            )

            # Aggregating at MovieId level
            movie_rating_batch_df = (
                ratings_batch_df
                .groupBy("MovieId")
                .agg(
                    count("*").alias(
                        "TotalRatings"
                    ),
                    avg("Rating").alias(
                        "AverageRating"
                    ),
                    min("Rating").alias(
                        "LowestRating"
                    )
                )
                .join(
                    batch_metadata_df,
                    on="MovieId",
                    how="inner"
                )
                .select(
                    "MovieId",
                    "Year",
                    "Title",
                    "Genres",
                    "TotalRatings",
                    "AverageRating",
                    "LowestRating"
                )
            )

            # Persist only the small aggregated batch
            movie_rating_batch_df = (
                movie_rating_batch_df
                .persist(
                    StorageLevel.DISK_ONLY
                )
            )

            batch_rating_count = (
                movie_rating_batch_df.count()
            )

            print(
                f"Rating batch aggregated successfully: "
                f"{batch_rating_count} movies"
            )

            movie_rating_parts.append(
                movie_rating_batch_df
            )

            # Reading tags for same MovieId batch
            tags_query = f"""
            (
                SELECT
                    "MovieId",
                    "Tag"
                FROM silver.tags
                WHERE "MovieId"
                BETWEEN {start_movie_id}
                AND {end_movie_id}
            ) AS tags_batch
            """

            tags_batch_df = (
                spark.read
                .format("jdbc")
                .option(
                    "url",
                    POSTGRES_URL
                )
                .option(
                    "dbtable",
                    tags_query
                )
                .option(
                    "user",
                    POSTGRES_PROPERTIES["user"]
                )
                .option(
                    "password",
                    POSTGRES_PROPERTIES["password"]
                )
                .option(
                    "driver",
                    POSTGRES_PROPERTIES["driver"]
                )
                .option(
                    "partitionColumn",
                    "MovieId"
                )
                .option(
                    "lowerBound",
                    str(start_movie_id)
                )
                .option(
                    "upperBound",
                    str(end_movie_id)
                )
                .option(
                    "numPartitions",
                    "4"
                )
                .option(
                    "fetchsize",
                    "10000"
                )
                .load()
            )

            # Remove only null and empty tags
            tags_batch_df = (
                tags_batch_df
                .filter(
                    col("Tag").isNotNull()
                    &
                    (
                        trim(
                            col("Tag")
                        ) != ""
                    )
                )
                .withColumn(
                    "Tag",
                    trim(
                        col("Tag")
                    )
                )
            )

            # Joining tags to release year, then aggregating within the current batch.
            year_tag_batch_df = (
                tags_batch_df
                .join(
                    batch_metadata_df.select(
                        "MovieId",
                        "Year"
                    ),
                    on="MovieId",
                    how="inner"
                )
                .groupBy(
                    "Year",
                    "Tag"
                )
                .agg(
                    count("*").alias(
                        "TagFrequency"
                    )
                )
            )

            year_tag_batch_df = (
                year_tag_batch_df
                .persist(
                    StorageLevel.DISK_ONLY
                )
            )

            year_tag_batch_df.count()

            year_tag_parts.append(
                year_tag_batch_df
            )

            print(
                f"Tag batch aggregated successfully for "
                f"{start_movie_id} - {end_movie_id}"
            )

        # Combining all aggregated rating batches
        if not movie_rating_parts:

            raise ValueError(
                "No rating data was found for yearly insight"
            )

        movie_year_stats_df = (
            movie_rating_parts[0]
        )

        for batch_df in movie_rating_parts[1:]:

            movie_year_stats_df = (
                movie_year_stats_df
                .unionByName(
                    batch_df
                )
            )

        movie_year_stats_df = (
            movie_year_stats_df
            .persist(
                StorageLevel.DISK_ONLY
            )
        )

        movie_year_stats_df.count()

        for batch_df in movie_rating_parts:
            batch_df.unpersist()

        print(
            "All movie rating batches combined successfully"
        )

        # Total ratings and yearly numeric statistics
        yearly_rating_stats_df = (
            movie_year_stats_df
            .groupBy("Year")
            .agg(
                spark_sum(
                    "TotalRatings"
                ).alias(
                    "TotalRatings"
                ),

                max(
                    "AverageRating"
                ).alias(
                    "HighestAverageRating"
                ),

                min(
                    "LowestRating"
                ).alias(
                    "LowestRating"
                )
            )
        )

        print(
            "Year-level rating statistics calculated successfully"
        )

        # Most popular movie in each year. Popular means movie having maximum number of ratings
        popular_movie_window = (
            Window
            .partitionBy("Year")
            .orderBy(
                col(
                    "TotalRatings"
                ).desc(),
                col(
                    "AverageRating"
                ).desc(),
                col(
                    "Title"
                ).asc()
            )
        )

        most_popular_movie_df = (
            movie_year_stats_df
            .withColumn(
                "PopularMovieRank",
                row_number().over(
                    popular_movie_window
                )
            )
            .filter(
                col(
                    "PopularMovieRank"
                ) == 1
            )
            .select(
                "Year",
                col(
                    "Title"
                ).alias(
                    "MostPopularMovie"
                )
            )
        )

        print(
            "Most popular movie per year calculated successfully"
        )

        # Highly rated movie in each year, Best movie means highest average rating. Rating count used as tie-breaker
        highly_rated_window = (
            Window
            .partitionBy("Year")
            .orderBy(
                col(
                    "AverageRating"
                ).desc(),
                col(
                    "TotalRatings"
                ).desc(),
                col(
                    "Title"
                ).asc()
            )
        )

        highly_rated_movie_df = (
            movie_year_stats_df
            .withColumn(
                "HighlyRatedRank",
                row_number().over(
                    highly_rated_window
                )
            )
            .filter(
                col(
                    "HighlyRatedRank"
                ) == 1
            )
            .select(
                "Year",
                col(
                    "Title"
                ).alias(
                    "HighlyRatedMovie"
                )
            )
        )

        print(
            "Highly rated movie per year calculated successfully"
        )

        # Worst rated movie in each year
        worst_rated_window = (
            Window
            .partitionBy("Year")
            .orderBy(
                col(
                    "AverageRating"
                ).asc(),
                col(
                    "TotalRatings"
                ).desc(),
                col(
                    "Title"
                ).asc()
            )
        )

        worst_rated_movie_df = (
            movie_year_stats_df
            .withColumn(
                "WorstRatedRank",
                row_number().over(
                    worst_rated_window
                )
            )
            .filter(
                col(
                    "WorstRatedRank"
                ) == 1
            )
            .select(
                "Year",
                col(
                    "Title"
                ).alias(
                    "WorstRatedMovie"
                )
            )
        )

        print(
            "Worst rated movie per year calculated successfully"
        )

        # Prepare movie genres
        movie_year_genre_df = (
            movie_year_stats_df
            .withColumn(
                "Genre",
                explode(
                    split(
                        col("Genres"),
                        "\\|"
                    )
                )
            )
            .filter(
                col("Genre").isNotNull()
                &
                (
                    col("Genre") != ""
                )
                &
                (
                    col("Genre")
                    != "(no genres listed)"
                )
            )
        )

        # Calculating weighted genre average
        yearly_genre_stats_df = (
            movie_year_genre_df
            .groupBy(
                "Year",
                "Genre"
            )
            .agg(
                spark_sum(
                    col("AverageRating")
                    *
                    col("TotalRatings")
                ).alias(
                    "GenreRatingSum"
                ),

                spark_sum(
                    "TotalRatings"
                ).alias(
                    "GenreTotalRatings"
                )
            )
            .withColumn(
                "GenreAverageRating",
                col("GenreRatingSum")
                /
                col("GenreTotalRatings")
            )
        )

        print(
            "Year-level genre statistics calculated successfully"
        )

        # Top rated genre
        top_genre_window = (
            Window
            .partitionBy("Year")
            .orderBy(
                col(
                    "GenreAverageRating"
                ).desc(),
                col(
                    "GenreTotalRatings"
                ).desc(),
                col(
                    "Genre"
                ).asc()
            )
        )

        top_rated_genre_df = (
            yearly_genre_stats_df
            .withColumn(
                "TopGenreRank",
                row_number().over(
                    top_genre_window
                )
            )
            .filter(
                col(
                    "TopGenreRank"
                ) == 1
            )
            .select(
                "Year",
                col(
                    "Genre"
                ).alias(
                    "TopRatedGenre"
                )
            )
        )

        # Least rated genre
        least_genre_window = (
            Window
            .partitionBy("Year")
            .orderBy(
                col(
                    "GenreAverageRating"
                ).asc(),
                col(
                    "GenreTotalRatings"
                ).desc(),
                col(
                    "Genre"
                ).asc()
            )
        )

        least_rated_genre_df = (
            yearly_genre_stats_df
            .withColumn(
                "LeastGenreRank",
                row_number().over(
                    least_genre_window
                )
            )
            .filter(
                col(
                    "LeastGenreRank"
                ) == 1
            )
            .select(
                "Year",
                col(
                    "Genre"
                ).alias(
                    "LeastRatedGenre"
                )
            )
        )

        print(
            "Top and least rated genre calculated successfully"
        )

        # Combining tag batches
        if year_tag_parts:

            year_tag_frequency_df = (
                year_tag_parts[0]
            )

            for batch_df in year_tag_parts[1:]:

                year_tag_frequency_df = (
                    year_tag_frequency_df
                    .unionByName(
                        batch_df
                    )
                )

            year_tag_frequency_df = (
                year_tag_frequency_df
                .groupBy(
                    "Year",
                    "Tag"
                )
                .agg(
                    spark_sum(
                        "TagFrequency"
                    ).alias(
                        "TagFrequency"
                    )
                )
                .persist(
                    StorageLevel.DISK_ONLY
                )
            )

            year_tag_frequency_df.count()

            for batch_df in year_tag_parts:
                batch_df.unpersist()

            # Total number of distinct tags used
            distinct_tags_df = (
                year_tag_frequency_df
                .groupBy("Year")
                .agg(
                    count("*").alias(
                        "TotalDistinctTags"
                    )
                )
            )

            # Most popular tag
            popular_tag_window = (
                Window
                .partitionBy("Year")
                .orderBy(
                    col(
                        "TagFrequency"
                    ).desc(),
                    col(
                        "Tag"
                    ).asc()
                )
            )

            popular_tag_df = (
                year_tag_frequency_df
                .withColumn(
                    "TagRank",
                    row_number().over(
                        popular_tag_window
                    )
                )
                .filter(
                    col(
                        "TagRank"
                    ) == 1
                )
                .select(
                    "Year",
                    col(
                        "Tag"
                    ).alias(
                        "PopularTag"
                    )
                )
            )

        else:

            raise ValueError(
                "No tag data was found for yearly insight"
            )

        print(
            "Year-level tag statistics calculated successfully"
        )

        # Combining all yearly insights
        yearly_insight_df = (
            movies_released_df

            .join(
                yearly_rating_stats_df,
                on="Year",
                how="left"
            )

            .join(
                most_popular_movie_df,
                on="Year",
                how="left"
            )

            .join(
                highly_rated_movie_df,
                on="Year",
                how="left"
            )

            .join(
                worst_rated_movie_df,
                on="Year",
                how="left"
            )

            .join(
                top_rated_genre_df,
                on="Year",
                how="left"
            )

            .join(
                least_rated_genre_df,
                on="Year",
                how="left"
            )

            .join(
                distinct_tags_df,
                on="Year",
                how="left"
            )

            .join(
                popular_tag_df,
                on="Year",
                how="left"
            )
        )

        yearly_insight_df = (
            yearly_insight_df
            .fillna(
                {
                    "TotalRatings": 0,
                    "TotalDistinctTags": 0
                }
            )
        )

        # Adding Gold audit columns
        yearly_insight_df = (
            yearly_insight_df
            .withColumn(
                "LoadTs",
                current_timestamp()
            )
            .withColumn(
                "UpdateTs",
                current_timestamp()
            )
        )

        # Final column order
        yearly_insight_df = (
            yearly_insight_df
            .select(
                "Year",
                "NumberOfMoviesReleased",
                "MostPopularMovie",
                "TotalRatings",
                "HighlyRatedMovie",
                "HighestAverageRating",
                "WorstRatedMovie",
                "LowestRating",
                "TopRatedGenre",
                "LeastRatedGenre",
                "TotalDistinctTags",
                "PopularTag",
                "LoadTs",
                "UpdateTs"
            )
            .orderBy("Year")
        )

        yearly_insight_df.printSchema()

        yearly_insight_df.write.jdbc(
            url=POSTGRES_URL,
            table="gold.yearly_insight",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )

        print(
            "gold.yearly_insight written successfully "
            "to PostgreSQL"
        )

        target_row_count = (
            get_table_row_count(
                "gold.yearly_insight"
            )
        )

        print(
            f"gold.yearly_insight row count: "
            f"{target_row_count}"
        )

        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.yearly_insight",
            status="SUCCESS",
            target_row_count=target_row_count,
            error_message=None
        )

        print(
            "gold.yearly_insight transformation "
            "completed successfully"
        )

    except Exception as e:

        write_gold_transformation_log(
            source_tables=source_tables,
            target_table="gold.yearly_insight",
            status="FAILED",
            target_row_count=None,
            error_message=str(e)
        )

        raise

    finally:

        spark.stop()

if __name__ == "__main__":

    if len(sys.argv) == 1:
        transform_movie_insight()
        transform_user_insight()
        transform_genre_insight()
        transform_yearly_insight()

    else:
        task_name = sys.argv[1]

        if task_name == "transform_movie_insight":
            transform_movie_insight()

        elif task_name == "transform_user_insight":
            transform_user_insight()

        elif task_name == "transform_genre_insight":
            transform_genre_insight()

        elif task_name == "transform_yearly_insight":
            transform_yearly_insight()

        else:
            raise ValueError(
                f"Unknown task: {task_name}"
            )
