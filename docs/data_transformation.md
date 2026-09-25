# MovieLens Data Cleaning & Transformation — Silver Layer

## 1. Overview

The Silver layer contains cleaned, standardized, and validated data created from the Bronze layer of the MovieLens pipeline.

The Bronze tables are read from PostgreSQL using PySpark, transformed according to the required standards, and written into the `silver` schema in PostgreSQL.

Silver tables are maintained using an idempotent PostgreSQL UPSERT strategy instead of replacing the complete target table during every execution.

Incoming transformed data is first deduplicated according to the business key. Rejected duplicate records are identified for the Quarantine layer, while valid records are merged into the Silver tables using staging tables, unique indexes, and PostgreSQL `ON CONFLICT` handling.

The Silver transformation process is orchestrated using Apache Airflow. The Silver DAG is automatically triggered after successful completion of the Bronze ingestion DAG.

---

## 2. Source and Target

### Source Schema

The Silver transformation uses the following Bronze tables as input:

- `bronze.links`
- `bronze.movies`
- `bronze.ratings`
- `bronze.tags`

### Target Schema

The transformed data is stored in:

- `silver.links`
- `silver.movies`
- `silver.ratings`
- `silver.tags`

---

### Quarantine Schema

Rejected duplicate records identified during Silver processing are stored, when applicable, in:

- `quarantine.links`
- `quarantine.movies`
- `quarantine.ratings`
- `quarantine.tags`

Quarantine tables are created when rejected records are available for the corresponding dataset.

## 3. Transformation Process

The Silver transformation is implemented using PySpark in:

`src/pyspark/transform_silver.py`

Separate transformation functions are created for each MovieLens dataset:

- `transform_links()`
- `transform_movies()`
- `transform_ratings()`
- `transform_tags()`

Each function performs the required Bronze-to-Silver processing independently.

The general data flow is:

```text
Bronze PostgreSQL Tables
          |
          v
      PySpark Read
          |
          v
 Schema Enforcement
          |
          v
Deduplication / Cleaning
       /       \
      v         v
Quarantine   Valid Records
                |
                v
      Standardization
                |
                v
        Add Audit Columns
                |
                v
       Staging Table
                |
                v
 PostgreSQL UPSERT / MERGE
                |
                v
          Silver Layer
---

## 4. Silver Links Transformation

Source:

`bronze.links`

Target:

`silver.links`

The links dataset is read from the Bronze layer and transformed into the standardized Silver structure.

The Silver table contains:

- `MovieId`
- `ImdbId`
- `TmdbId`
- `CreateDtTm`
- `UpdateDtTm`

`MovieId` is used as the business key for Silver deduplication and UPSERT processing.

The transformation:

- Enforces the expected source schema.
- Identifies repeated records for the same `movieId`.
- Retains one deterministic record for each `movieId`.
- Identifies rejected duplicate records for quarantine.
- Standardizes column names.
- Adds audit timestamp columns.
- UPSERTs valid records into `silver.links`.
- Records source, target, and rejected row counts in the transformation log.

Rejected duplicate representations are stored in `quarantine.links` with:

```text
Reason = DUPLICATE_MOVIE_ID

---

## 5. Silver Movies Transformation

Source:

`bronze.movies`

Target:

`silver.movies`

The Silver movies table contains:

- `MovieId`
- `Title`
- `Genres`
- `CreateDtTm`
- `UpdateDtTm`

The transformation includes:

`MovieId` is used as the business key.

The transformation:

- Enforces the expected source schema.
- Identifies repeated records for the same `movieId`.
- Retains one deterministic record for each `movieId`.
- Identifies rejected duplicate records for quarantine.
- Standardizes column names.
- Preserves the MovieLens genre representation.
- Adds audit timestamp columns.
- UPSERTs valid records into `silver.movies`.
- Records transformation statistics in the transformation log.

Rejected duplicate representations are stored in `quarantine.movies` with:

```text
Reason = DUPLICATE_MOVIE_ID

The `Genres` column is retained in its source representation because genre splitting was not required for the current Silver asset.

Example:

```text
Action|Comedy|Drama|Romance
```

---

## 6. Silver Ratings Transformation

Source:

`bronze.ratings`

Target:

`silver.ratings`

The ratings dataset is significantly larger than the other MovieLens datasets, containing approximately 32 million records.

The Silver ratings table contains:

- `UserId`
- `MovieId`
- `Rating`
- `Timestamp`
- `CreateDtTm`
- `UpdateDtTm`

### Large Dataset Handling

Reading the complete ratings table through a single JDBC partition caused high memory utilization during local PySpark execution.

Therefore, the ratings table is read from PostgreSQL using partitioned JDBC reading.

The JDBC read uses `UserId` as the partitioning column so that the source data can be distributed across multiple Spark partitions instead of being processed as one large partition.

This improves scalability and reduces the amount of data handled by an individual Spark task.

### Deduplication Strategy

The combination of:

```text
UserId + MovieId
```

---

## 7. Silver Tags Transformation

Source:

`bronze.tags`

Target:

`silver.tags`

The Silver tags table contains:

- `UserId`
- `MovieId`
- `Tag`
- `Timestamp`
- `CreateDtTm`
- `UpdateDtTm`

The transformation includes:

- Standardizing column names.
- Removing duplicate records.
- Adding audit timestamp columns.
- Writing the transformed records into `silver.tags`.
- Recording the transformation execution in the transformation log.

---

## 8. Naming Standards

Column names in the Silver layer were standardized to follow a consistent naming convention.

Examples:

```text
movieId   -> MovieId
userId    -> UserId
rating    -> Rating
timestamp -> Timestamp
title     -> Title
genres    -> Genres
tag       -> Tag
```

Audit columns follow the same standard:

```text
CreatedDtTm
UpdatedDtTm
```

This provides consistent naming across the Silver assets.

---

## 9. Audit Columns

Two audit columns are added during the Silver transformation:

```text
CreateDtTm
UpdateDtTm
```

Both columns store transformation timestamps.

The Spark SQL session timezone is configured as:

```text
UTC
```

This ensures that timestamps generated during processing follow a consistent timezone standard.

---

## 10. Bronze-to-Silver Row Count Validation

Row counts are monitored during Silver transformations.

Bronze and Silver row counts are not expected to always match because the Bronze layer can preserve repeated raw ingestions, while the Silver layer retains records according to defined business keys.

For example, repeated executions can result in multiple raw copies in `bronze.links` and `bronze.movies`, while Silver deduplication retains one record for each `MovieId`.

The transformation log records:

- Source row count
- Target row count
- Bad/rejected row count
- Transformation status

The `bad_row_count` represents the number of physical source rows excluded from the Silver result.

Quarantine storage may contain fewer rows than `bad_row_count` because identical rejected copies are collapsed into unique rejected record representations.

---

## 11. Null and Audit Column Validation

Null checks were performed on the important business columns and audit columns of each Silver table.

### silver.links

Validated:

- `MovieId`
- `ImdbId`
- `CreateDtTm`
- `UpdateDtTm`

Result:

```text
NULL count = 0
```

for all validated columns.

### silver.movies

Validated:

- `MovieId`
- `Title`
- `Genres`
- `CreateDtTm`
- `UpdateDtTm`

Result:

```text
NULL count = 0
```

for all validated columns.

### silver.ratings

Validated:

- `UserId`
- `MovieId`
- `Rating`
- `Timestamp`
- `CreateDtTm`
- `UpdateDtTm`

Result:

```text
NULL count = 0
```

for all validated columns.

### silver.tags

Validated:

- `UserId`
- `MovieId`
- `Tag`
- `Timestamp`
- `CreateDtTm`
- `UpdateDtTm`

Result:

```text
NULL count = 0
```

for all validated columns.

Therefore, the required business and audit columns were successfully populated during the Silver transformation.

---

## 12. Duplicate Validation

Duplicate handling is performed according to the business key of each Silver asset.

The primary business keys are:

| Silver Asset | Business Key |
|---|---|
| `silver.links` | `MovieId` |
| `silver.movies` | `MovieId` |
| `silver.ratings` | `UserId`, `MovieId` |
| `silver.tags` | `UserId`, `MovieId`, `Tag`, `Timestamp` |
| `silver.movie_metadata` | `MovieId` |
| `silver.user_ratings_master` | `UserId`, `MovieId` |

Unique indexes are created on these keys to enforce target-level uniqueness and support PostgreSQL UPSERT processing.

Validation confirmed that duplicate business keys were not present in the resulting Silver assets.

---

## 13. Rating Range Validation

The MovieLens ratings were validated to ensure that rating values are within the expected range.

Validation result:

```text
Minimum Rating = 0.5
Maximum Rating = 5.0
Invalid Rating Count = 0
```

Therefore, all ratings are within the valid MovieLens rating range of:

```text
0.5 to 5.0
```

---

## 14. Data Quality Validation Summary

The following Data Quality checks were performed after creating the Silver assets:

| Validation | Result |
|---|---|
| Source and target row count | Passed |
| Schema / datatype validation | Passed |
| Required column null validation | Passed |
| Audit column validation | Passed |
| Duplicate validation | Passed |
| Rating range validation | Passed |

The Silver assets passed the performed Data Quality validations.

---

## 15. Transformation Logging

Transformation logging is implemented to maintain information about Silver processing.

After successful transformation of a Silver asset, execution information is written to the transformation log.

This provides traceability of the transformation process and helps with monitoring and debugging pipeline executions.

The transformation logs are stored in PostgreSQL and record successful Silver transformation activity.

---

## 16. UPSERT and Idempotent Loading

Silver tables are maintained using a staging-based PostgreSQL UPSERT process.

For each Silver asset:

1. Valid transformed data is prepared in PySpark.
2. The data is written to a temporary staging table.
3. A unique index is maintained on the target business key.
4. Records are merged into the Silver target using PostgreSQL `INSERT ... ON CONFLICT`.
5. Existing records are updated only when relevant business data has changed.
6. New business keys are inserted.
7. The staging table is removed after successful processing.

`CreateDtTm` is excluded from normal update operations so that the original creation timestamp is preserved.

This makes repeated Silver executions idempotent: rerunning the same source data does not create duplicate Silver business records.

---

## 17. Quarantine Layer

The `quarantine` PostgreSQL schema stores rejected record representations identified during Silver processing.

The quarantine process captures:

- The rejected source record
- A rejection `Reason`
- `QuarantinedDtTm`

Examples of rejection reasons include:

```text
DUPLICATE_MOVIE_ID
OLDER_DUPLICATE_RATING
EXACT_DUPLICATE_TAG
```

---

## 18. Airflow Orchestration

The MovieLens pipeline uses separate Airflow DAGs for Bronze ingestion and Silver transformation.

### Bronze DAG

DAG ID:

```text
movielens_bronze_ingestion
```

The Bronze DAG executes:

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

After Bronze validation completes successfully, `trigger_silver_dag` triggers the Silver transformation DAG.

---

## 19. Silver Transformation DAG

DAG ID:

```text
movielens_silver_transformation
```

The Silver DAG executes:

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
```

The Silver DAG has no independent schedule because it is triggered after successful Bronze processing.

This prevents the Silver transformation from running before the required Bronze data is available.

---

## 20. Bronze-to-Silver DAG Dependency

The Bronze and Silver processes are maintained as separate DAGs to keep the pipeline modular.

The dependency between the two layers is:

```text
movielens_bronze_ingestion
              |
              v
       validate_bronze
              |
              v
      trigger_silver_dag
              |
              v
movielens_silver_transformation
```

This design separates ingestion and transformation responsibilities while still maintaining the required execution order.

---

## 21. Complete Day 3 Data Flow

The completed data flow for the Silver layer is:

```text
MovieLens CSV Files
        |
        v
Bronze Ingestion DAG
        |
        v
PostgreSQL Bronze
        |
        v
Trigger Silver DAG
        |
        v
PySpark Silver Transformations
        |
        v
Schema Enforcement
        |
        v
Deduplication
      /     \
     v       v
Quarantine  Valid Records
                 |
                 v
        Cleaning / Standardization
                 |
                 v
            Audit Columns
                 |
                 v
            Staging Tables
                 |
                 v
          PostgreSQL UPSERT
                 |
                 v
             Silver Layer
                 |
                 v
     Transformation Logging
```

---

## 22. Final Silver Assets

At the completion of Day 3, the following Silver assets were successfully created:

```text
silver.links
silver.movies
silver.ratings
silver.tags
```

The Silver layer now provides standardized and validated datasets that can be used for further transformations and downstream MovieLens analytical assets.

---

## 23. Day 3 Completion Summary

The following activities were completed as part of the Silver layer implementation:

- Created Silver assets from Bronze objects.
- Implemented PySpark transformations.
- Standardized column naming.
- Added `CreateDtTm` and `UpdateDtTm` audit columns.
- Configured UTC timestamp handling.
- Handled the large ratings dataset using partitioned JDBC reading.
- Validated Bronze and Silver row counts.
- Validated schemas and datatypes.
- Performed null checks.
- Performed duplicate checks.
- Validated the ratings range.
- Implemented transformation logging.
- Created the Silver Airflow DAG.
- Added Bronze-to-Silver DAG dependency.
- Successfully tested the complete Bronze-to-Silver pipeline locally.