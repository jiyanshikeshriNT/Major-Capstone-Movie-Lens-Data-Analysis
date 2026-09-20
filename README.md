# MovieLens Data Analysis — End-to-End Data Engineering Pipeline

## 1. Project Overview

This project implements an end-to-end data engineering and analytics pipeline using the MovieLens 32M dataset.

The solution processes raw MovieLens CSV files through a Medallion-style architecture consisting of:

- Bronze Layer — Raw data ingestion
- Silver Layer — Cleaning, standardization, deduplication, quarantine, and reusable transformed datasets
- Gold Layer — Business-level analytical datasets
- Reporting Layer — PostgreSQL reporting views where additional dashboard-level data was required
- Visualization Layer — Interactive Tableau dashboards

Apache PySpark is used for large-scale data processing, PostgreSQL is used for persistent storage, Apache Airflow is used for workflow orchestration, Docker is used to run Airflow locally, and Tableau is used for reporting and visualization.

The project also implements audit logging, data-quality validation, controlled handling of rejected records, incremental/file-by-file processing, staging-based UPSERT operations, and idempotent Silver processing.

---

## 2. Business Objective

The objective of the project is to analyze a large-scale movie rating dataset and generate useful insights related to:

- Movie popularity
- Movie ratings
- User rating behaviour
- User tagging behaviour
- Genre performance
- Movie release trends
- Rating patterns
- User engagement
- Popular and highly rated movies
- Time-based rating activity

The processed datasets are designed to support analytical queries and interactive dashboards for stakeholder reporting.

---

## 3. Dataset

The project uses the MovieLens 32M dataset.

### Dataset Summary

| Metric | Value |
|---|---:|
| Movies | 87,585 |
| Users | 200,948 |
| Ratings | 32,000,204 |
| Tags | 2,000,072 |

The source dataset contains the following main files:

### links.csv

Columns:

```text
movieId
imdbId
tmdbId
```

This file contains external identifiers that can be used to associate MovieLens movies with IMDb and TMDb.

### movies.csv

Columns:

```text
movieId
title
genres
```

The movie release year is included in the title field.

A movie can belong to multiple genres, with individual genres separated using the `|` delimiter.

### ratings

For this project, the large ratings dataset is divided into five files:

```text
ratings_part1.csv
ratings_part2.csv
ratings_part3.csv
ratings_part4.csv
ratings_part5.csv
```

Columns:

```text
userId
movieId
rating
timestamp
```

Ratings use a 0.5 to 5.0 scale.

The raw `timestamp` is stored as Unix epoch time.

### tags.csv

Columns:

```text
userId
movieId
tag
timestamp
```

Each record represents a tag assigned by a user to a movie.

The raw timestamp is stored as Unix epoch time.

---

## 4. Technology Stack

The project uses the following technologies:

| Technology | Purpose |
|---|---|
| Python | General pipeline development and utility functions |
| PySpark | Large-scale ingestion, cleaning, transformation, aggregation, and analytical processing |
| PostgreSQL | Bronze, Silver, Gold, Quarantine, logging, and reporting storage |
| Apache Airflow | Workflow orchestration and dependency management |
| Docker | Local Airflow execution environment |
| SQL | Validation, database operations, UPSERT processing, reporting views, and analysis |
| Tableau | Interactive dashboard development |
| Git & GitHub | Version control and collaboration |
| VS Code | Development environment |

---

## 5. High-Level Architecture

The complete project follows the following flow:

```text
                    MovieLens CSV Files
                            |
                            v
                   Source Validation
                            |
                            v
                    PySpark Ingestion
                            |
                            v
                  PostgreSQL Bronze
                            |
                            v
                  Bronze Validation
                            |
                            v
              PySpark Silver Transformation
                            |
              +-------------+-------------+
              |                           |
              v                           v
       Valid / Clean Data          Rejected Records
              |                           |
              v                           v
      PostgreSQL Silver          PostgreSQL Quarantine
              |
              v
       Derived Silver Assets
              |
              v
       PySpark Gold Analytics
              |
              v
         PostgreSQL Gold
              |
              +------------------+
              |                  |
              v                  v
      Gold Reporting       PostgreSQL Views
              |                  |
              +---------+--------+
                        |
                        v
                     Tableau
                        |
                        v
              Interactive Dashboards
```

Apache Airflow orchestrates the Bronze, Silver, and Gold processing stages.

---

# BRONZE LAYER

## 6. Bronze Layer Overview

The Bronze layer represents the raw ingestion layer.

The source CSV files are loaded into the PostgreSQL `bronze` schema before major cleaning or business transformations are applied.

The primary Bronze tables are:

```text
bronze.links
bronze.movies
bronze.ratings
bronze.tags
bronze.ingestion_log
```

The Bronze ingestion logic is implemented in:

```text
src/pyspark/ingest_bronze.py
```

---

## 7. Source File Validation

Before ingestion begins, the pipeline verifies that all required source files exist.

The expected files include:

```text
links.csv
movies.csv
tags.csv
ratings_part1.csv
ratings_part2.csv
ratings_part3.csv
ratings_part4.csv
ratings_part5.csv
```

If an expected source file is missing, the pipeline raises an exception and prevents downstream processing from continuing.

---

## 8. Explicit Schema Handling

Explicit PySpark schemas are used while reading the source CSV files.

The schemas use appropriate PySpark data types including:

```text
IntegerType
StringType
LongType
DoubleType
```

Using explicit schemas provides predictable datatype handling and avoids relying completely on automatic schema inference.

---

## 9. Generic Bronze Ingestion

Common CSV ingestion functionality is implemented through a reusable ingestion function.

```text
ingest_csv()
```

The reusable logic handles operations such as:

- Reading source CSV files
- Applying the expected schema
- Counting source rows
- Writing data through PostgreSQL JDBC
- Recording ingestion status
- Recording row counts
- Handling ingestion errors

Source-specific functions use the generic ingestion logic where applicable.

The primary ingestion functions are:

```text
check_source_files()
ingest_links()
ingest_movies()
ingest_tags()
ingest_ratings()
validate_bronze()
```

---

## 10. Bronze Load Strategy

The Bronze ingestion strategy depends on the source dataset.

```text
links    → Append
movies   → Append
tags     → Truncate + Append
ratings  → Truncate + Sequential Append
```

### Links and Movies

`bronze.links` and `bronze.movies` use append-based ingestion.

The existing Bronze tables are not truncated before these files are loaded.

As a result, repeated ingestion runs can preserve repeated raw records.

Duplicate handling is intentionally performed during Silver transformation.

### Tags

The complete tags dataset is reprocessed during the pipeline execution.

Therefore:

```text
TRUNCATE bronze.tags
        |
        v
Read tags.csv
        |
        v
Append
        |
        v
bronze.tags
```

This prevents the complete tags dataset from accumulating repeatedly.

### Ratings

Ratings contain approximately 32 million records and are divided into five files.

Before ratings ingestion begins:

```text
bronze.ratings
```

is truncated once.

The five files are then processed sequentially:

```text
Truncate bronze.ratings
          |
          v
ratings_part1.csv
          |
        Append
          |
          v
ratings_part2.csv
          |
        Append
          |
          v
ratings_part3.csv
          |
        Append
          |
          v
ratings_part4.csv
          |
        Append
          |
          v
ratings_part5.csv
          |
        Append
          |
          v
bronze.ratings
```

This provides file-by-file processing while avoiding repeated accumulation of the complete ratings dataset.

---

## 11. Bronze Ingestion Logging

Bronze ingestion activity is recorded in:

```text
bronze.ingestion_log
```

The log stores information such as:

```text
file_name
target_table
row_count
status
error_message
load_time
```

Successful processing is recorded using:

```text
SUCCESS
```

Failures can be recorded using:

```text
FAILED
```

For ratings, each source part can be logged separately, providing visibility into the processing of the large ratings dataset.

---

## 12. Bronze Validation

After ingestion completes, the pipeline validates:

```text
bronze.links
bronze.movies
bronze.tags
bronze.ratings
```

Row counts are retrieved for each table.

If a required Bronze table contains no data, validation fails and downstream processing does not continue normally.

---

# SILVER LAYER

## 13. Silver Layer Overview

The Silver layer converts raw Bronze data into cleaned, standardized, deduplicated, and reusable datasets.

The main Silver assets are:

```text
silver.links
silver.movies
silver.ratings
silver.tags
silver.movie_metadata
silver.user_ratings_master
```

Silver transformations are implemented in:

```text
src/pyspark/transform_silver.py
```

The main functions are:

```text
transform_links()
transform_movies()
transform_ratings()
transform_tags()
transform_movie_metadata()
transform_user_ratings_master()
```

---

## 14. Silver Transformation Flow

The general Silver processing flow is:

```text
Bronze Table
     |
     v
PySpark JDBC Read
     |
     v
Schema Enforcement
     |
     v
Data Cleaning
     |
     v
Business-Key Deduplication
     |
     +--------------------+
     |                    |
     v                    v
Valid Records       Rejected Records
     |                    |
     v                    v
Standardization       Quarantine
     |
     v
Audit Columns
     |
     v
Staging Table
     |
     v
PostgreSQL UPSERT
     |
     v
Silver Target
```

---

## 15. Silver Naming Standards

Silver columns follow a standardized PascalCase naming convention.

Examples:

```text
movieId   → MovieId
userId    → UserId
rating    → Rating
timestamp → Timestamp
title     → Title
genres    → Genres
tag       → Tag
```

Silver audit columns are:

```text
CreateDtTm
UpdateDtTm
```

Silver table names use snake_case.

---

## 16. UTC Timestamp Handling

The SparkSession explicitly uses UTC:

```text
spark.sql.session.timeZone = UTC
```

This ensures that Unix timestamp conversion remains consistent between execution environments.

The configuration is especially important for timestamp fields that participate in business keys, such as the tag timestamp.

Without a consistent Spark session timezone, the same Unix timestamp could be represented differently between local and Docker/Airflow executions.

---

## 17. Silver Links Transformation

Source:

```text
bronze.links
```

Target:

```text
silver.links
```

Business key:

```text
MovieId
```

The transformation:

- Enforces the expected schema
- Identifies repeated `movieId` records
- Uses deterministic ranking to select one record for each `MovieId`
- Identifies rejected duplicate records
- Standardizes column names
- Adds audit columns
- UPSERTs valid records into Silver
- Logs transformation statistics

Rejected duplicate representations are assigned:

```text
Reason = DUPLICATE_MOVIE_ID
```

and stored in:

```text
quarantine.links
```

A unique index on `MovieId` prevents duplicate business records in the Silver target.

---

## 18. Silver Movies Transformation

Source:

```text
bronze.movies
```

Target:

```text
silver.movies
```

Business key:

```text
MovieId
```

The transformation performs:

- Schema enforcement
- Business-key deduplication
- Deterministic record selection
- Duplicate identification
- Column standardization
- Audit-column generation
- Silver UPSERT processing
- Transformation logging

Rejected duplicate representations use:

```text
Reason = DUPLICATE_MOVIE_ID
```

and are stored in:

```text
quarantine.movies
```

The original MovieLens genre representation is retained.

Example:

```text
Action|Comedy|Drama
```

---

## 19. Silver Ratings Transformation

Source:

```text
bronze.ratings
```

Target:

```text
silver.ratings
```

The ratings dataset contains approximately 32 million records.

### Partitioned JDBC Read

The large ratings table is read using partitioned JDBC processing.

`UserId` is used as the partition column so that records can be distributed across multiple Spark partitions.

### Business Key

The ratings business key is:

```text
UserId + MovieId
```

### Deduplication

Records are ranked within each `(UserId, MovieId)` group according to timestamp.

The latest record is retained for Silver.

An older duplicate rating, if present, is identified using:

```text
Reason = OLDER_DUPLICATE_RATING
```

and can be written to:

```text
quarantine.ratings
```

During the validated dataset execution, no duplicate rating business keys were identified.

The Unix epoch source timestamp is converted into timestamp format during Silver processing.

A unique index on:

```text
(UserId, MovieId)
```

supports idempotent UPSERT processing.

---

## 20. Silver Tags Transformation

Source:

```text
bronze.tags
```

Target:

```text
silver.tags
```

An exact tag record is identified using:

```text
UserId + MovieId + Tag + Timestamp
```

The transformation:

- Enforces the source schema
- Identifies exact duplicate tag records
- Retains one copy for Silver
- Identifies additional copies for quarantine
- Converts Unix epoch timestamps
- Standardizes column names
- Adds audit timestamps
- UPSERTs records into Silver

Rejected exact duplicate records use:

```text
Reason = EXACT_DUPLICATE_TAG
```

and can be stored in:

```text
quarantine.tags
```

During the validated execution, no exact duplicate tag records were identified.

---

# QUARANTINE LAYER

## 21. Quarantine Overview

The PostgreSQL `quarantine` schema is used to preserve rejected records identified during Silver processing rather than silently deleting them.

Possible quarantine tables include:

```text
quarantine.links
quarantine.movies
quarantine.ratings
quarantine.tags
```

A quarantine table is created when rejected records exist for the corresponding dataset.

Therefore, a table may not physically exist when no rejected records were found.

---

## 22. Quarantine Record Structure

A quarantined record contains its original source information along with:

```text
Reason
QuarantinedDtTm
```

Example reasons include:

```text
DUPLICATE_MOVIE_ID
OLDER_DUPLICATE_RATING
EXACT_DUPLICATE_TAG
```

Quarantine writes also use the common UPSERT mechanism.

This prevents the same rejected record representation from being continuously inserted during repeated executions.

`QuarantinedDtTm` is preserved for an already existing quarantined record.

---

## 23. Physical Rejected Rows vs Quarantine Rows

The transformation log and Quarantine layer represent two related but different concepts.

### bad_row_count

`bad_row_count` represents the number of physical source rows excluded from the valid Silver result.

### Quarantine row count

The Quarantine layer stores unique rejected record representations.

Therefore:

```text
bad_row_count
```

can be greater than the number of records stored in a quarantine table when the same rejected source record appears repeatedly.

This allows the transformation log to accurately represent physical rejected rows while preventing unnecessary duplicate records inside Quarantine.

---

# SILVER UPSERT AND IDEMPOTENCY

## 24. Staging-Based UPSERT

Silver data is not recreated using a complete overwrite during every execution.

Instead, a staging-based PostgreSQL UPSERT process is used.

General process:

```text
Transformed PySpark DataFrame
            |
            v
      Staging Table
            |
            v
       Unique Index
            |
            v
 INSERT ... ON CONFLICT
       /           \
      v             v
   INSERT         UPDATE
 New Record     Changed Record
      \             /
       +-----+-----+
             |
             v
        Silver Table
             |
             v
     Drop Staging Table
```

For an existing business key, the relevant record is updated only when business data has changed.

For a new business key, a new record is inserted.

---

## 25. Audit Timestamp Behaviour

Silver records contain:

```text
CreateDtTm
UpdateDtTm
```

`CreateDtTm` is excluded from normal UPSERT updates.

This preserves the original creation timestamp of an existing record.

`UpdateDtTm` can reflect subsequent processing when relevant business data changes.

Similarly, the Quarantine UPSERT process preserves:

```text
QuarantinedDtTm
```

for existing rejected records.

---

## 26. Idempotency

The combination of:

- Business-key deduplication
- Unique indexes
- Staging tables
- PostgreSQL `ON CONFLICT`
- Controlled audit timestamp updates

makes Silver processing idempotent.

Running the same transformation repeatedly does not create duplicate Silver business records.

---

# DERIVED SILVER ASSETS

## 27. silver.movie_metadata

`silver.movie_metadata` combines:

```text
silver.movies
silver.links
```

The transformation:

- Joins movie information with external identifiers
- Extracts `ReleaseYear` from the movie title
- Removes the release-year suffix from `Title`
- Preserves genre information
- Adds audit timestamps

The output contains:

```text
MovieId
Title
ReleaseYear
Genres
ImdbId
TmdbId
CreateDtTm
UpdateDtTm
```

A LEFT JOIN is used so that movies can remain in the output even when an external identifier is unavailable.

Business key:

```text
MovieId
```

The table is maintained using staging-based UPSERT processing.

---

## 28. silver.user_ratings_master

The `silver.user_ratings_master` asset combines:

```text
silver.ratings
silver.tags
silver.movie_metadata
```

Multiple tags belonging to the same:

```text
UserId + MovieId
```

combination are aggregated before joining with ratings.

The final table contains:

```text
UserId
MovieId
Title
ReleaseYear
Genres
Tags
Rating
RatingTstmp
ImdbId
TmdbId
CreateDtTm
UpdateDtTm
```

Because ratings contain approximately 32 million records, processing is performed in `UserId` batches.

Batch size:

```text
25,000 users
```

Example:

```text
1      - 25000
25001  - 50000
50001  - 75000
...
200001 - 200948
```

Each batch is loaded using staging-based UPSERT processing.

Business key:

```text
UserId + MovieId
```

LEFT JOINs are used so that rating records remain available even when optional metadata or tag information is unavailable.

---

# DATA QUALITY AND LOGGING

## 29. Silver Transformation Logging

Silver transformation information is recorded in:

```text
silver.transformation_log
```

The log captures information such as:

```text
source_table
target_table
source_row_count
target_row_count
bad_row_count
status
error_message
load_time
```

Possible statuses include:

```text
SUCCESS
FAILED
```

This provides traceability for Silver processing.

---

## 30. Data Quality Checks

Data-quality validation includes:

- Schema validation
- Datatype validation
- Required-column null checks
- Business-key duplicate checks
- Row-count monitoring
- Audit-column validation
- Rating-range validation
- Rejected-row monitoring
- Quarantine validation

Ratings are validated against the expected range:

```text
0.5 to 5.0
```


Duplicate business-key validation confirmed that the resulting Silver assets contain no duplicate records according to their defined keys.

---

## 31. Silver Business Keys

| Silver Asset | Business Key |
|---|---|
| `silver.links` | `MovieId` |
| `silver.movies` | `MovieId` |
| `silver.ratings` | `UserId`, `MovieId` |
| `silver.tags` | `UserId`, `MovieId`, `Tag`, `Timestamp` |
| `silver.movie_metadata` | `MovieId` |
| `silver.user_ratings_master` | `UserId`, `MovieId` |

Unique indexes on these keys support target-level uniqueness and UPSERT processing.

---

# GOLD LAYER

## 32. Gold Layer Overview

The Gold layer provides business-ready analytical datasets created from cleaned Silver data.

The project contains four main Gold assets:

```text
gold.movie_insight
gold.user_insight
gold.genre_insight
gold.yearly_insight
```

Gold transformations are implemented using PySpark.

Execution information is recorded in:

```text
gold.transformation_log
```

The Gold transformations include large aggregations, joins, ranking operations, and window functions.

---

## 33. gold.movie_insight

`gold.movie_insight` provides one movie-level analytical record.

Primary sources:

```text
silver.movie_metadata
silver.ratings
silver.tags
```

Important metrics include:

```text
MovieId
ReleaseYear
MovieTitle
AverageRating
HighestRating
LowestRating
TotalRatings
TotalPeopleTagged
TotalTags
DistinctTags
PopularTag
LoadTs
UpdateTs
```

### Rating Metrics

Ratings are grouped by movie to calculate:

- Average rating
- Highest rating
- Lowest rating
- Total number of ratings

### Tag Metrics

Tags are used to calculate:

- Number of users who tagged the movie
- Total tags
- Distinct tags
- Popular tag

Tag frequency is used to identify the most popular tag.

Alphabetical ordering provides deterministic tie-breaking when tag frequencies are equal.

LEFT JOINs preserve movies even when rating or tag information is unavailable.

---

## 34. gold.user_insight

`gold.user_insight` provides user-level analytical information.

Sources:

```text
silver.ratings
silver.tags
silver.movie_metadata
```

The asset contains metrics including:

```text
UserId
NumberOfMoviesRated
AverageLifetimeRating
AverageRatingsPerYear
LeastRatingGiven
HighestRatingGiven
FiveStarMovieCount
TotalTagsUsed
MostUsedTag
TopGenre
BestRatedGenre
MostRecentlyRatedMovie
MostRecentlyTaggedMovie
LoadTs
UpdateTs
```

### Top Genre

`TopGenre` represents the genre in which a user rated the greatest number of movies.

### Best Rated Genre

`BestRatedGenre` represents the genre having the highest average rating given by the user.

### Recent Activity

Window functions are used to determine:

- Most recently rated movie
- Most recently tagged movie

### Performance Handling

Users are processed in batches of:

```text
25,000
```

to reduce Spark memory pressure.

---

## 35. gold.genre_insight

`gold.genre_insight` provides one analytical record for each genre.

The `Genres` field from movie metadata is split and exploded to create a movie-to-genre mapping.

Movies containing:

```text
(no genres listed)
```

are excluded from genre-level analysis.

Important metrics include:

```text
Genre
MovieCount
TotalRatings
AverageRating
PopularTag
Rating Distribution
MostRatedMovie
LeastRatedMovie
HighestAverageRatedMovie
Top10PopularMovies
Top10HighlyRatedMovies
WorstRatedMovie
LoadTs
UpdateTs
```

### Rating Distribution

Separate count fields represent individual rating values from 0 to 5.

### Highest Average Rated Movie

Only movies having at least:

```text
100 ratings
```

are considered for this metric.

This prevents movies with very small rating samples from dominating the metric.

### Top Movies

The Gold asset contains arrays representing:

- Top 10 popular movies
- Top 10 highly rated movies

### Performance Handling

Ratings are processed using `MovieId` batches of:

```text
25,000
```

to reduce Spark memory usage.

---

## 36. gold.yearly_insight

`gold.yearly_insight` provides release-year-level analytical information.

Sources:

```text
silver.movie_metadata
silver.ratings
silver.tags
```

Important metrics include:

```text
Year
NumberOfMoviesReleased
MostPopularMovie
TotalRatings
HighlyRatedMovie
HighestAverageRating
WorstRatedMovie
LowestRating
TopRatedGenre
LeastRatedGenre
TotalDistinctTags
MostPopularTag
LoadTs
UpdateTs
```

Only movies having a valid release year are considered for year-level analysis.

Ratings and tags are associated with the release year of their corresponding movie.

Large data processing uses `MovieId` batches of:

```text
25,000
```

to reduce memory pressure.

---

## 37. Gold Performance Strategy

The Gold transformations perform memory-intensive operations including:

- Large joins
- Aggregations
- Sorting
- Window functions
- Genre explosion
- Rating statistics
- Tag-frequency calculations

To support execution in the available local environment:

- Large datasets are processed in batches
- Ratings use UserId or MovieId ranges depending on the transformation
- Gold Airflow tasks execute sequentially
- Large Spark jobs are not executed in parallel

This reduces the risk of Java heap-space and Spark memory errors.

---

# AIRFLOW ORCHESTRATION

## 38. Pipeline Orchestration

Apache Airflow orchestrates the complete pipeline.

The project uses separate DAGs for:

```text
Bronze Ingestion
Silver Transformation
Gold Transformation
```

The overall dependency is:

```text
Bronze DAG
    |
    v
Bronze Validation
    |
    v
Trigger Silver DAG
    |
    v
Silver Transformations
    |
    v
Trigger Gold DAG
    |
    v
Gold Transformations
```

Keeping the layers in separate DAGs makes the pipeline modular while preserving execution dependencies.

---

## 39. Bronze DAG

DAG ID:

```text
movielens_bronze_ingestion
```

Task flow:

```text
check_source_files
        |
        v
ingest_links
        |
        v
ingest_movies
        |
        v
ingest_tags
        |
        v
ingest_ratings
        |
        v
validate_bronze
        |
        v
trigger_silver_dag
```

---

## 40. Silver DAG

The Silver transformation flow includes:

```text
transform_links
        |
        v
transform_movies
        |
        v
transform_ratings
        |
        v
transform_tags
        |
        v
transform_movie_metadata
        |
        v
transform_user_ratings_master
```

The logical dependencies for the derived Silver assets are:

```text
transform_links ---------+
                         |
                         +--> transform_movie_metadata
                         |
transform_movies --------+

transform_movie_metadata --------+
                                  |
transform_ratings ----------------+--> transform_user_ratings_master
                                  |
transform_tags -------------------+
```

The Silver DAG uses a single active run to avoid concurrent transformations using the same staging-table names.

---

## 41. Gold DAG

Gold transformations are executed sequentially:

```text
transform_movie_insight
        |
        v
transform_user_insight
        |
        v
transform_genre_insight
        |
        v
transform_yearly_insight
```

Sequential execution prevents multiple large Spark transformations from competing for memory simultaneously.

---

# DOCKER AND LOCAL EXECUTION

## 42. Docker Setup

Apache Airflow runs locally using Docker.

Docker provides the Airflow runtime environment and associated Airflow services.

The project is mounted into the Airflow environment at:

```text
/opt/movielens
```

This allows Airflow tasks to execute the project source code.

PostgreSQL containing the MovieLens schemas runs on the Windows host.

From inside Docker, the host PostgreSQL instance is accessed through:

```text
host.docker.internal
```

Local execution can use:

```text
localhost
```

---

## 43. PostgreSQL JDBC Connectivity

PySpark communicates with PostgreSQL through JDBC.

The PostgreSQL JDBC driver used during development is:

```text
postgresql-42.7.13.jar
```

The SparkSession is configured with the required JDBC driver.

---

# TABLEAU REPORTING

## 44. Tableau Dashboard

The processed data is used to create an interactive Tableau dashboard.

The dashboard contains two major analytical views:

```text
View 1 — Movie & Ratings Insights
View 2 — User, Tags & Engagement Analysis
```

The reporting requirements were first compared with the available Gold datasets through a Gold-layer gap analysis.

Where the existing Gold tables directly supported the requirement, Gold was used as the reporting source.

Where the required fields were not available together at the necessary grain, PostgreSQL reporting views were created on top of the Silver layer and exposed to Tableau.

This avoided changing the analytical grain of existing Gold assets only for dashboard-specific requirements.

---

## 45. View 1 — Movie & Ratings Insights

The Movie & Ratings dashboard includes KPIs such as:

- Total Movies
- Distinct Genres
- Total Ratings
- Overall Average Rating
- Top Rated Movie
- Most Rated Genre

The required visualizations include:

- Average Rating by Genre
- Movie Distribution by Genre
- Movie Releases over Time
- Ratings Distribution
- Top 10 Movies by Number of Ratings
- Rating Count vs Average Rating
- Ratings Submitted Over Time

Dashboard filters include:

```text
Genre
Year
Rating Range
```

---

## 46. View 2 — User, Tags & Engagement

The User, Tags & Engagement dashboard includes KPIs such as:

- Total Users
- Average Ratings per User
- Most Active User
- Total Tags
- Most Tagged Movie
- Most Used Tag

The required visualizations include:

- Top 10 Users by Ratings Count
- User Rating Behaviour
- Ratings Activity by Day of Week vs Time of Day
- Top 10 Movies by Number of Tags
- Movie → User → Tags → Rating Matrix
- Tags Count vs Ratings Count per Movie

Dashboard filters include:

```text
User ID
Genre
Year
Tag Keyword
```

---

## 47. Gold Layer Gap Analysis

Before Tableau development, the existing Gold assets were compared with dashboard requirements.

Several requirements were directly supported.

Examples include:

```text
Average Rating by Genre
Movies by Genre
Releases Over Time
Top Movies by Rating Count
Rating Count vs Average Rating
Top Users by Rating Count
Top Movies by Tag Count
Tags Count vs Ratings Count
```

Some requirements were only partially supported because required dimensions existed across different Gold grains.

Examples included:

- Common Genre + Year + Rating Range filtering
- User + Genre + Year + Tag filtering
- Overall rating distribution
- Monthly rating activity
- Global tag frequency

Other requirements needed lower-grain data that was not present in the aggregated Gold assets.

Examples included:

- Rating activity by day of week and time of day
- Movie → User → Tags → Rating matrix

---

## 48. PostgreSQL Reporting Views

For dashboard requirements that could not be satisfied directly from existing Gold assets, PostgreSQL views were created on top of the Silver layer.

These views expose the additional dimensions and lower-grain information required by Tableau without modifying the existing Gold analytical tables.

The reporting approach therefore became:

```text
                    PostgreSQL
                        |
             +----------+----------+
             |                     |
             v                     v
       Gold Tables          Silver-Based Views
             |                     |
             +----------+----------+
                        |
                        v
                     Tableau
```

This allows the dashboard to use the most appropriate data grain for each visualization.

---

# PROJECT CONFIGURATION

## 49. Environment Configuration

Project-level configuration is maintained in:

```text
src/python/config.py
```

Configuration includes:

- PostgreSQL host
- PostgreSQL port
- PostgreSQL database
- PostgreSQL username
- PostgreSQL password
- PostgreSQL JDBC URL
- JDBC properties
- Source data directory
- Expected source files

Sensitive configuration is maintained using environment variables.

A local:

```text
.env
```

file can be used for environment-specific credentials.

The `.env` file must not be committed to Git.

---

## 50. Utility Functions

Reusable functionality is maintained in:

```text
src/python/utils.py
```

Utilities include functionality for:

- PostgreSQL connections
- SparkSession creation
- UTC Spark configuration
- Ingestion logging
- Transformation logging
- PostgreSQL row-count retrieval
- Shared database operations

Separating reusable functions from individual transformations reduces duplicated code and improves maintainability.

---

# PROJECT STRUCTURE

## 51. Repository Structure

A simplified project structure is:

```text
MovieLens/
|
|-- data/
|   `-- raw/
|       |-- links.csv
|       |-- movies.csv
|       |-- tags.csv
|       |-- ratings_part1.csv
|       |-- ratings_part2.csv
|       |-- ratings_part3.csv
|       |-- ratings_part4.csv
|       `-- ratings_part5.csv
|
|-- src/
|   |-- python/
|   |   |-- config.py
|   |   `-- utils.py
|   |
|   `-- pyspark/
|       |-- ingest_bronze.py
|       |-- transform_silver.py
|       `-- transform_gold.py
|
|-- airflow_dags/
|   |-- movie_lens_ingestion_dag.py
|   |-- movie_lens_silver_dag.py
|   `-- movie_lens_gold_dag.py
|
|-- docs/
|   |-- data_profiling.md
|   |-- data_ingestion.md
|   |-- data_transformation.md
|   `-- gold_transformations.md
|
|-- drivers/
|   `-- postgresql-42.7.13.jar
|
|-- docker-compose.yaml
|-- Dockerfile
|-- requirements.txt
|-- .gitignore
`-- README.md
```

The exact filenames of documentation and DAG files may vary according to the repository structure.

---

# RUNNING THE PROJECT

## 52. Prerequisites

The development environment requires:

```text
Python
Java
Apache Spark / PySpark
PostgreSQL
PostgreSQL JDBC Driver
Docker Desktop
Apache Airflow
```

The local development environment used:

```text
Python 3.11
Java 17
Spark 3.5.x
PostgreSQL
Docker Desktop
```

On Windows, the Spark/Hadoop environment also requires the appropriate Hadoop configuration.

---

## 53. Environment Variables

Create the required environment configuration for PostgreSQL.

Example configuration variables include:

```text
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

Do not commit real credentials to Git.

---

## 54. Start Airflow

Start the Docker environment from the project directory:

```bash
docker compose up -d
```

The Airflow UI can then be used to monitor and trigger the pipeline.

---

## 55. Manual Bronze Execution

Run the complete Bronze pipeline:

```bash
python -m src.pyspark.ingest_bronze
```

Individual operations can also be executed:

```bash
python -m src.pyspark.ingest_bronze check_source_files
python -m src.pyspark.ingest_bronze ingest_links
python -m src.pyspark.ingest_bronze ingest_movies
python -m src.pyspark.ingest_bronze ingest_tags
python -m src.pyspark.ingest_bronze ingest_ratings
python -m src.pyspark.ingest_bronze validate_bronze
```

---

## 56. Manual Silver Execution

Individual Silver transformations can be executed using:

```bash
python -m src.pyspark.transform_silver transform_links
python -m src.pyspark.transform_silver transform_movies
python -m src.pyspark.transform_silver transform_ratings
python -m src.pyspark.transform_silver transform_tags
python -m src.pyspark.transform_silver transform_movie_metadata
python -m src.pyspark.transform_silver transform_user_ratings_master
```

---

## 57. Manual Gold Execution

Gold transformations can be executed individually through the Gold transformation module.

Examples:

```bash
python -m src.pyspark.transform_gold transform_movie_insight
python -m src.pyspark.transform_gold transform_user_insight
python -m src.pyspark.transform_gold transform_genre_insight
python -m src.pyspark.transform_gold transform_yearly_insight
```

For normal end-to-end execution, Airflow manages the layer dependencies and task order.

---

# PERFORMANCE AND TECHNICAL CHALLENGES

## 58. Large Ratings Dataset

The ratings dataset contains:

```text
32,000,204 records
```

Processing this dataset required special handling.

Techniques used include:

- Partitioned JDBC reads
- Sequential source-file ingestion
- UserId batching
- MovieId batching
- Avoiding unnecessary full-data operations
- Sequential Airflow execution for resource-intensive transformations

---

## 59. Spark Memory Management

During development, operations on the complete ratings dataset could cause memory pressure and Java heap-space errors.

The pipeline was adjusted to avoid processing every large operation as a single in-memory workload.

Different transformations therefore use an appropriate batching strategy depending on their analytical grain.

---

## 60. Timestamp Consistency

Unix timestamps are converted during Silver processing.

A timezone inconsistency between execution environments can produce different timestamp representations for the same Unix value.

This is especially important for:

```text
silver.tags
```

because `Timestamp` participates in its unique business key.

The Spark SQL session is therefore explicitly configured to:

```text
UTC
```

ensuring consistent processing between local and Docker/Airflow environments.

---

## 61. Safe Pipeline Re-runs

Repeated execution was an important design consideration.

The project handles reruns using:

- Controlled Bronze reload strategies
- Silver business-key deduplication
- Unique indexes
- Staging tables
- PostgreSQL UPSERT
- Quarantine UPSERT
- Preserved creation/quarantine timestamps

This allows the transformation layer to be rerun without creating duplicate business records.

---

# END-TO-END DATA FLOW

## 62. Complete Pipeline

```text
                       MovieLens Dataset
                              |
                              v
                    Raw CSV Source Files
                              |
                              v
                    Source File Validation
                              |
                              v
                    PySpark Bronze Ingestion
                              |
                              v
                     PostgreSQL Bronze
                              |
                              v
                     Bronze Validation
                              |
                              v
                    Trigger Silver DAG
                              |
                              v
                  PySpark Silver Processing
                              |
             +----------------+----------------+
             |                                 |
             v                                 v
      Valid Business Data              Rejected Records
             |                                 |
             v                                 v
     Staging + UPSERT                  Quarantine Schema
             |
             v
      PostgreSQL Silver
             |
             v
     Derived Silver Assets
             |
             v
       Trigger Gold DAG
             |
             v
       Movie Insight
             |
             v
        User Insight
             |
             v
       Genre Insight
             |
             v
       Yearly Insight
             |
             v
        PostgreSQL Gold
             |
             +---------------------+
             |                     |
             v                     v
        Gold Sources       Silver Reporting Views
             |                     |
             +----------+----------+
                        |
                        v
                     Tableau
                        |
                        v
             Interactive Dashboards
```

---

# KEY DESIGN DECISIONS

## 63. Why Bronze Keeps Raw/Reprocessed Data

Bronze represents the ingestion layer.

For smaller reference datasets such as links and movies, append-based loading preserves repeated raw ingestion events.

The Silver layer is responsible for applying business-key deduplication and producing clean datasets.

For much larger complete-refresh datasets such as tags and ratings, controlled truncation prevents unnecessary repeated growth.

---

## 64. Why Silver Is Required

Bronze contains raw source data and is not intended to be directly consumed as the trusted analytical layer.

Silver provides:

- Standardized column naming
- Correct datatypes
- UTC timestamps
- Deduplication
- Business-key enforcement
- Quarantine handling
- Audit columns
- Idempotent processing
- Reusable derived datasets

This creates a reliable foundation for Gold analytics.

---

## 65. Why Quarantine Is Used

Rejected records are not silently deleted.

Quarantine allows rejected data to be retained along with the reason for rejection.

This improves:

- Traceability
- Debugging
- Data-quality monitoring
- Auditability

---

## 66. Why UPSERT Is Used

A complete overwrite of Silver targets would recreate the target during every run and make audit behaviour harder to preserve.

UPSERT allows the pipeline to:

- Insert new business records
- Update changed business records
- Preserve existing records
- Preserve original creation timestamps
- Prevent duplicate business keys
- Support safe reruns

---

## 67. Why Gold Assets Are Aggregated

Gold datasets are designed for analytical consumption.

Instead of repeatedly calculating complex aggregations directly from millions of Silver records, Gold provides reusable analytical datasets at specific grains:

```text
Movie  → gold.movie_insight
User   → gold.user_insight
Genre  → gold.genre_insight
Year   → gold.yearly_insight
```

This simplifies downstream reporting and analysis.

---

## 68. Why PostgreSQL Views Were Used for Tableau Gaps

The Gold datasets intentionally have different analytical grains.

Some Tableau requirements needed combinations of fields that were not available together in an existing Gold asset.

Rather than changing the established Gold tables only for dashboard-specific needs, PostgreSQL views were created over Silver data for the missing reporting grains.

This allowed the project to retain the existing Gold model while still satisfying the Tableau reporting requirements.

---

# FINAL OUTPUTS

## 69. Final Database Assets

### Bronze

```text
bronze.links
bronze.movies
bronze.ratings
bronze.tags
bronze.ingestion_log
```

### Silver

```text
silver.links
silver.movies
silver.ratings
silver.tags
silver.movie_metadata
silver.user_ratings_master
silver.transformation_log
```

### Quarantine

Created as required:

```text
quarantine.links
quarantine.movies
quarantine.ratings
quarantine.tags
```

### Gold

```text
gold.movie_insight
gold.user_insight
gold.genre_insight
gold.yearly_insight
gold.transformation_log
```

### Reporting

PostgreSQL views on top of the processed Silver layer are used where Tableau requirements need additional or lower-grain reporting fields.

---

## 70. Key Project Features

The completed project demonstrates:

- End-to-end data engineering pipeline design
- Medallion-style Bronze, Silver, and Gold architecture
- Processing of more than 32 million ratings
- PySpark-based ingestion and transformations
- PostgreSQL JDBC integration
- Explicit schema handling
- Generic reusable ingestion logic
- Controlled Bronze reload strategies
- Business-key deduplication
- Rejected-record quarantine
- Staging-based PostgreSQL UPSERT
- Unique-index enforcement
- Idempotent Silver processing
- Audit and transformation logging
- UTC timestamp standardization
- Partitioned JDBC reads
- UserId and MovieId batching
- Derived Silver datasets
- Movie-level analytics
- User-level analytics
- Genre-level analytics
- Year-level analytics
- Airflow workflow orchestration
- Docker-based Airflow execution
- Data-quality validation
- Gold-layer dashboard gap analysisingestion events.

The Silver layer is responsible for applying business-key deduplication and producing clean datasets.

For much larger complete-refresh datasets such as tags and ratings, controlled truncation prevents unnecessary repeated growth.

---

## 64. Why Silver Is Required

Bronze contains raw source data and is not intended to be directly consumed as the trusted analytical layer.

Silver provides:

- Standardized column naming
- Correct datatypes
- UTC timestamps
- Deduplication
- Business-key enforcement
- Quarantine handling
- Audit columns
- Idempotent processing
- Reusable derived datasets

This creates a reliable foundation for Gold analytics.

---

## 65. Why Quarantine Is Used

Rejected records are not silently deleted.

Quarantine allows rejected data to be retained along with the reason for rejection.

This improves:

- Traceability
- Debugging
- Data-quality monitoring
- Auditability

---

## 66. Why UPSERT Is Used

A complete overwrite of Silver targets would recreate the target during every run and make audit behaviour harder to preserve.

UPSERT allows the pipeline to:

- Insert new business records
- Update changed business records
- Preserve existing records
- Preserve original creation timestamps
- Prevent duplicate business keys
- Support safe reruns

---

## 67. Why Gold Assets Are Aggregated

Gold datasets are designed for analytical consumption.

Instead of repeatedly calculating complex aggregations directly from millions of Silver records, Gold provides reusable analytical datasets at specific grains:

```text
Movie  → gold.movie_insight
User   → gold.user_insight
Genre  → gold.genre_insight
Year   → gold.yearly_insight
```

This simplifies downstream reporting and analysis.

---

## 68. Why PostgreSQL Views Were Used for Tableau Gaps

The Gold datasets intentionally have different analytical grains.

Some Tableau requirements needed combinations of fields that were not available together in an existing Gold asset.

Rather than changing the established Gold tables only for dashboard-specific needs, PostgreSQL views were created over Silver data for the missing reporting grains.

This allowed the project to retain the existing Gold model while still satisfying the Tableau reporting requirements.

---

# FINAL OUTPUTS

## 69. Final Database Assets

### Bronze

```text
bronze.links
bronze.movies
bronze.ratings
bronze.tags
bronze.ingestion_log
```

### Silver

```text
silver.links
silver.movies
silver.ratings
silver.tags
silver.movie_metadata
silver.user_ratings_master
silver.transformation_log
```

### Quarantine

Created as required:

```text
quarantine.links
quarantine.movies
quarantine.ratings
quarantine.tags
```

### Gold

```text
gold.movie_insight
gold.user_insight
gold.genre_insight
gold.yearly_insight
gold.transformation_log
```

### Reporting

PostgreSQL views on top of the processed Silver layer are used where Tableau requirements need additional or lower-grain reporting fields.

---

## 70. Key Project Features

The completed project demonstrates:

- End-to-end data engineering pipeline design
- Medallion-style Bronze, Silver, and Gold architecture
- Processing of more than 32 million ratings
- PySpark-based ingestion and transformations
- PostgreSQL JDBC integration
- Explicit schema handling
- Generic reusable ingestion logic
- Controlled Bronze reload strategies
- Business-key deduplication
- Rejected-record quarantine
- Staging-based PostgreSQL UPSERT
- Unique-index enforcement
- Idempotent Silver processing
- Audit and transformation logging
- UTC timestamp standardization
- Partitioned JDBC reads
- UserId and MovieId batching
- Derived Silver datasets
- Movie-level analytics
- User-level analytics
- Genre-level analytics
- Year-level analytics
- Airflow workflow orchestration
- Docker-based Airflow execution
- Data-quality validation
- Gold-layer dashboard gap analysis
- PostgreSQL reporting views
- Tableau dashboard development
- Git-based version control

---

## 71. Conclusion

The MovieLens Data Analysis project implements a complete data engineering and analytics workflow for a large-scale dataset containing more than 32 million movie ratings.

Raw MovieLens files are first ingested into PostgreSQL through the Bronze layer. The Silver layer then standardizes and cleans the data, removes business-key duplicates, preserves rejected records through Quarantine, and maintains trusted datasets using staging-based UPSERT processing.

Derived Silver datasets provide reusable movie and user-rating information for downstream processing.

The Gold layer converts the cleaned Silver data into movie-level, user-level, genre-level, and yearly analytical assets.

Apache Airflow orchestrates the Bronze → Silver → Gold workflow, while batching and partitioned processing allow large datasets to be handled within the available environment.

Finally, the Gold datasets are validated against Tableau reporting requirements. Where an existing Gold grain cannot directly support a dashboard requirement, PostgreSQL views built on top of the processed Silver layer provide the additional reporting data.

The final result is an end-to-end, auditable, rerunnable, and analytics-ready MovieLens data pipeline supporting both analytical processing and interactive Tableau visualization.
- PostgreSQL reporting views
- Tableau dashboard development
- Git-based version control

---

## 71. Conclusion

The MovieLens Data Analysis project implements a complete data engineering and analytics workflow for a large-scale dataset containing more than 32 million movie ratings.

Raw MovieLens files are first ingested into PostgreSQL through the Bronze layer. The Silver layer then standardizes and cleans the data, removes business-key duplicates, preserves rejected records through Quarantine, and maintains trusted datasets using staging-based UPSERT processing.

Derived Silver datasets provide reusable movie and user-rating information for downstream processing.

The Gold layer converts the cleaned Silver data into movie-level, user-level, genre-level, and yearly analytical assets.

Apache Airflow orchestrates the Bronze → Silver → Gold workflow, while batching and partitioned processing allow large datasets to be handled within the available environment.

Finally, the Gold datasets are validated against Tableau reporting requirements. Where an existing Gold grain cannot directly support a dashboard requirement, PostgreSQL views built on top of the processed Silver layer provide the additional reporting data.

The final result is an end-to-end, auditable, rerunnable, and analytics-ready MovieLens data pipeline supporting both analytical processing and interactive Tableau visualization.