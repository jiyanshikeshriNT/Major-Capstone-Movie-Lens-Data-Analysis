# MovieLens Data Cleaning & Transformation — Silver Layer

## 1. Overview

The Silver layer contains cleaned, standardized, and validated data created from the Bronze layer of the MovieLens pipeline.

The Bronze tables are read from PostgreSQL using PySpark, transformed according to the required standards, and written into the `silver` schema in PostgreSQL.

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
 Cleaning / Standardization
          |
          v
 Add Audit Columns
          |
          v
   Schema Validation
          |
          v
    PostgreSQL Silver
```

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
- `CreatedDtTm`
- `UpdatedDtTm`

The transformation includes:

- Standardizing column names.
- Removing duplicate records.
- Adding audit timestamp columns.
- Writing the transformed records to `silver.links`.
- Recording the transformation execution in the transformation log.

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
- `CreatedDtTm`
- `UpdatedDtTm`

The transformation includes:

- Standardizing column names.
- Removing duplicate records.
- Preserving the MovieLens genre representation.
- Adding audit timestamp columns.
- Writing the transformed data to `silver.movies`.
- Recording the transformation execution in the transformation log.

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
- `CreatedDtTm`
- `UpdatedDtTm`

### Large Dataset Handling

Reading the complete ratings table through a single JDBC partition caused high memory utilization during local PySpark execution.

Therefore, the ratings table is read from PostgreSQL using partitioned JDBC reading.

The JDBC read uses `UserId` as the partitioning column so that the source data can be distributed across multiple Spark partitions instead of being processed as one large partition.

This improves scalability and reduces the amount of data handled by an individual Spark task.

### Duplicate Validation

Applying a full Spark `dropDuplicates()` operation on the approximately 32 million row ratings dataset caused:

```text
java.lang.OutOfMemoryError: Java heap space
```

This happens because `dropDuplicates()` requires a large shuffle operation across the dataset.

To avoid an unnecessary expensive shuffle on the local environment, duplicate records were validated separately using PostgreSQL.

The duplicate validation returned:

```text
Duplicate Count = 0
```

Since no duplicate records were present, an additional full-data deduplication shuffle was avoided during the ratings transformation.

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
- `CreatedDtTm`
- `UpdatedDtTm`

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
CreatedDtTm
UpdatedDtTm
```

Both columns store transformation timestamps.

The Spark SQL session timezone is configured as:

```text
UTC
```

This ensures that timestamps generated during processing follow a consistent timezone standard.

---

## 10. Bronze-to-Silver Row Count Validation

Row counts were compared between Bronze and Silver tables after transformation.

The validation produced the following results:

| Dataset | Bronze Count | Silver Count |
|---|---:|---:|
| links | 87,585 | 87,585 |
| movies | 87,585 | 87,585 |
| tags | 2,000,072 | 2,000,072 |
| ratings | 32,000,204 | 32,000,204 |

The Bronze and Silver row counts match for all four datasets.

This confirms that no unexpected records were lost during the transformation process.

---

## 11. Null and Audit Column Validation

Null checks were performed on the important business columns and audit columns of each Silver table.

### silver.links

Validated:

- `MovieId`
- `ImdbId`
- `CreatedDtTm`
- `UpdatedDtTm`

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
- `CreatedDtTm`
- `UpdatedDtTm`

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
- `CreatedDtTm`
- `UpdatedDtTm`

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
- `CreatedDtTm`
- `UpdatedDtTm`

Result:

```text
NULL count = 0
```

for all validated columns.

Therefore, the required business and audit columns were successfully populated during the Silver transformation.

---

## 12. Duplicate Validation

Duplicate checks were performed on the Silver assets.

The duplicate validation result for all datasets was:

```text
Duplicate Count = 0
```

No duplicate records were detected in the validated Silver datasets.

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
| Bronze vs Silver row count | Passed |
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

## 16. Airflow Orchestration

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

## 17. Silver Transformation DAG

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

## 18. Bronze-to-Silver DAG Dependency

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

## 19. Complete Day 3 Data Flow

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
Bronze Validation
        |
        v
Trigger Silver DAG
        |
        v
PySpark Silver Transformations
        |
        +-----------------------+
        |                       |
        v                       v
Cleaning / Standardization   Audit Columns
        |                       |
        +-----------+-----------+
                    |
                    v
             PostgreSQL Silver
                    |
                    v
          Data Quality Validation
                    |
                    v
          Transformation Logging
```

---

## 20. Final Silver Assets

At the completion of Day 3, the following Silver assets were successfully created:

```text
silver.links
silver.movies
silver.ratings
silver.tags
```

The Silver layer now provides standardized and validated datasets that can be used for further transformations and downstream MovieLens analytical assets.

---

## 21. Day 3 Completion Summary

The following activities were completed as part of the Silver layer implementation:

- Created Silver assets from Bronze objects.
- Implemented PySpark transformations.
- Standardized column naming.
- Added `CreatedDtTm` and `UpdatedDtTm` audit columns.
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