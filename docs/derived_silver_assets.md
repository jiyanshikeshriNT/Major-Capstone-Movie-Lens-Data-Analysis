## Derived Silver Assets

### 1. silver.movie_metadata

The `silver.movie_metadata` table is created by combining data from:

- `silver.movies`
- `silver.links`

The transformation extracts the release year from the movie title and keeps the required movie metadata fields.

Target columns:

- MovieId
- Title
- ReleaseYear
- Genres
- ImdbId
- TmdbId
- CreateDtTm
- UpdateDtTm

The table is written to PostgreSQL using overwrite mode.

Data Flow:

```text
silver.movies ─────┐
                   ├──> silver.movie_metadata
silver.links ──────┘
```


### 2. silver.user_ratings_master

The `silver.user_ratings_master` table combines rating information with movie metadata and user tags.

Source tables:

- `silver.ratings`
- `silver.tags`
- `silver.movie_metadata`

Multiple tags belonging to the same UserId and MovieId are aggregated into a single comma-separated Tags field before joining with ratings.

The final table contains:

- UserId
- MovieId
- Title
- ReleaseYear
- Genres
- Tags
- Rating
- RatingTstmp
- ImdbId
- TmdbId
- CreateDtTm
- UpdateDtTm

Since the ratings table contains a large volume of data, the transformation is processed in UserId-based batches to avoid memory issues.

Example batch ranges:

- UserId 1–25000
- UserId 25001–50000
- UserId 50001–75000
- and so on

The first batch writes the target table using overwrite mode, while subsequent batches use append mode.


## Audit Logging

Transformation details are recorded in:

`silver.transformation_log`

The audit information includes:

- source table
- target table
- source row count
- target row count
- bad row count
- status
- error message
- load timestamp

A helper function, `get_table_row_count()`, retrieves row counts directly from PostgreSQL for audit purposes.


## Airflow Dependencies

The Silver transformation pipeline is orchestrated using Airflow.

The derived Silver assets have the following dependencies:

```text
transform_links ─────┐
                     ├──> transform_movie_metadata ─────┐
transform_movies ────┘                                  │
                                                        ├──> transform_user_ratings_master
transform_ratings ──────────────────────────────────────┤
transform_tags ─────────────────────────────────────────┘