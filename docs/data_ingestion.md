# MovieLens Data Ingestion Process

## 1. Overview

The MovieLens data ingestion pipeline loads raw CSV files into the PostgreSQL Bronze layer using PySpark.

Apache Airflow is used to orchestrate and schedule the ingestion pipeline, while Docker is used to run Airflow locally.

The overall ingestion flow is:

```text
MovieLens CSV Files
        ↓
Source File Validation
        ↓
PySpark Ingestion
        ↓
PostgreSQL Bronze Layer
        ↓
Bronze Validation
```

Airflow manages the execution order, task dependencies, logging, and failure handling of the ingestion pipeline.

---

## 2. Source Files

The following MovieLens CSV files are used as source files:

- `links.csv`
- `movies.csv`
- `tags.csv`
- `ratings_part1.csv`
- `ratings_part2.csv`
- `ratings_part3.csv`
- `ratings_part4.csv`
- `ratings_part5.csv`

The ratings dataset is divided into five files to implement and demonstrate incremental data loading.

All raw source files are stored inside:

```text
data/raw/
```

The `data/raw/` directory is excluded from Git using `.gitignore` because raw datasets should not be committed to the source code repository.

---

## 3. Bronze Layer

The raw MovieLens data is loaded into the `bronze` schema in PostgreSQL.

The following Bronze tables are used:

```text
bronze.links
bronze.movies
bronze.tags
bronze.ratings
bronze.ingestion_log
```

The Bronze layer represents the raw ingestion layer of the data pipeline. It stores source data before the major cleaning and transformation operations that will be performed in the Silver layer.

---

## 4. PySpark Ingestion

The main Bronze ingestion logic is implemented in:

```text
src/pyspark/ingest_bronze.py
```

The ingestion script uses a reusable generic ingestion function for loading CSV files into PostgreSQL:

```text
ingest_csv()
```

The main Bronze pipeline functions are:

```text
check_source_files()
ingest_links()
ingest_movies()
ingest_tags()
ingest_ratings()
validate_bronze()
```

The `ingest_csv()` function contains the common CSV reading, PostgreSQL JDBC writing, row counting, and ingestion logging logic.

The source-specific ingestion functions call this reusable function with the appropriate file name and target table.

The ratings ingestion is coordinated separately because the ratings dataset is divided into five source files and each file is loaded sequentially into the same Bronze table.

Keeping the ingestion operations in separate functions also allows each step to be executed independently by Apache Airflow.

---

## 5. Configuration

Project-level configuration is maintained in:

```text
src/python/config.py
```

The configuration module contains:

- PostgreSQL host
- PostgreSQL port
- PostgreSQL database name
- PostgreSQL username and password
- JDBC connection URL
- JDBC connection properties
- Raw data directory path
- Expected source file names

Environment variables are loaded using `python-dotenv`.

Sensitive information such as PostgreSQL credentials is stored in the `.env` file.

The `.env` file is excluded from Git using `.gitignore` so that credentials are not committed to the repository.

---

## 6. Utility Functions

Reusable helper functions are maintained in:

```text
src/python/utils.py
```

The utility module contains reusable functionality such as:

- Creating a PostgreSQL connection
- Creating a SparkSession
- Writing ingestion logs into PostgreSQL

Separating reusable utilities from ingestion logic makes the project more modular and easier to maintain.

---

## 7. Source File Validation

Before starting the ingestion process, the pipeline checks whether all expected MovieLens source files are available.

The `check_source_files()` function validates the presence of:

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

If any expected file is missing, the pipeline raises an error and ingestion does not continue.

If all expected files are available, the validation completes successfully.

Example output:

```text
Checking MovieLens source files...

FOUND: links.csv
FOUND: movies.csv
FOUND: ratings_part1.csv
FOUND: ratings_part2.csv
FOUND: ratings_part3.csv
FOUND: ratings_part4.csv
FOUND: ratings_part5.csv
FOUND: tags.csv

All expected source files are available.
```

---

## 8. PostgreSQL Connection

The project uses PostgreSQL as the database for storing the Bronze layer.

PySpark communicates with PostgreSQL using JDBC.

The JDBC driver used by the project is:

```text
postgresql-42.7.13.jar
```

The PostgreSQL JDBC URL is generated dynamically using environment variables.

For local execution, PostgreSQL can be accessed using:

```text
localhost
```

When the ingestion pipeline runs inside the Airflow Docker container, PostgreSQL running on the Windows host machine is accessed using:

```text
host.docker.internal
```

This allows the Docker container to communicate with the PostgreSQL database installed on the host machine.

---

## 9. Schema Definition

Explicit PySpark schemas are defined while reading the CSV files.

For example, the MovieLens source data uses fields such as:

```text
movieId
userId
imdbId
tmdbId
title
genres
rating
tag
timestamp
```

Appropriate PySpark data types such as `IntegerType`, `StringType`, `LongType`, and `DoubleType` are used.

Defining schemas explicitly provides better control over column data types compared to relying entirely on automatic schema inference.

---

## 10. Links Ingestion

The `links.csv` source file is ingested into:

```text
bronze.links
```

The file is loaded through the reusable `ingest_csv()` function and written to PostgreSQL using Spark JDBC.

The Bronze layer preserves ingested raw data and therefore uses:

```text
mode = append
```

The links table is not truncated before ingestion. Therefore, repeated executions may result in repeated raw records in the Bronze layer.

These records are intentionally retained at the Bronze level, while duplicate handling is performed during the Silver transformation process.

After successful ingestion, an entry is written into `bronze.ingestion_log`.

---

## 11. Movies Ingestion

The `movies.csv` source file is ingested into:

```text
bronze.movies
```

The source file contains movie information such as:

```text
movieId
title
genres
```

The file is loaded through the reusable `ingest_csv()` function and written to PostgreSQL using Spark JDBC.

The movies table uses:

```text
mode = append
```

The movies table is not truncated before ingestion. Therefore, repeated pipeline executions may preserve repeated raw records in the Bronze layer.

Duplicate records are identified and handled during the Silver-layer transformation process.

After successful ingestion, an ingestion log entry is created.

---

## 12. Tags Ingestion

The `tags.csv` source file is ingested into:

```text
bronze.tags
```

The source contains fields such as:

```text
userId
movieId
tag
timestamp
```

Before loading the source file, the existing `bronze.tags` table is truncated.

The ingestion flow is:

```text
Truncate bronze.tags
        ↓
Read tags.csv
        ↓
Append Source Data
        ↓
bronze.tags
```

The data is written using:

```text
mode = append
```

Truncating the table before ingestion prevents the complete tags dataset from accumulating again on every full pipeline execution.

After the data is successfully written to PostgreSQL, the ingestion event is recorded in `bronze.ingestion_log`.

---

## 13. Incremental Ratings Ingestion

The ratings dataset is divided into five source files:

```text
ratings_part1.csv
ratings_part2.csv
ratings_part3.csv
ratings_part4.csv
ratings_part5.csv
```

All five files are loaded into one PostgreSQL table:

```text
bronze.ratings
```

Because the ratings dataset is very large, the source files are processed sequentially rather than being combined into one large DataFrame before writing.

At the beginning of the ratings ingestion process, the existing `bronze.ratings` table is truncated.

The five ratings files are then processed in order and appended to the same Bronze table:

```text
Truncate bronze.ratings
          ↓
ratings_part1.csv → Append
          ↓
ratings_part2.csv → Append
          ↓
ratings_part3.csv → Append
          ↓
ratings_part4.csv → Append
          ↓
ratings_part5.csv → Append
          ↓
bronze.ratings
```

Each ratings part is loaded sequentially using:

```text
mode = append
```

This approach keeps the ingestion manageable and demonstrates file-by-file incremental loading within a complete ratings reload.

Truncating the table once before processing the five files prevents the complete ratings dataset from being duplicated across repeated full pipeline executions.

---

## 14. Bronze Reload Strategy

The Bronze ingestion strategy depends on the size and ingestion behavior of each source dataset.

### Links and Movies

`links` and `movies` use append-based ingestion without truncating their existing Bronze tables.

This allows the Bronze layer to preserve repeated raw ingestions. Duplicate records are identified and handled in the Silver layer.

### Tags

Because the complete tags dataset is reprocessed during each pipeline execution, `bronze.tags` is truncated before the source file is appended.

### Ratings

Because ratings contain approximately 32 million rows across five source files, `bronze.ratings` is truncated once before the five ratings parts are sequentially appended.

The Bronze ingestion strategy can therefore be summarized as:

```text
links    → Append
movies   → Append
tags     → Truncate + Append
ratings  → Truncate + Sequential Append
```

This prevents unnecessary growth of the larger Bronze tables while keeping duplicate handling and data-quality processing primarily within the Silver layer.

---

## 15. Ingestion Logging

The ingestion pipeline maintains audit information in:

```text
bronze.ingestion_log
```

The ingestion log records information such as:

```text
file_name
target_table
row_count
status
error_message
load_time
```

A successful ingestion is recorded with:

```text
SUCCESS
```

A failed ingestion can be recorded with:

```text
FAILED
```

The ingestion log provides basic auditing and helps track which files have already been processed.

It is especially important for the incremental ratings ingestion because it is used to prevent duplicate file processing.

---

## 16. Bronze Validation

After ingestion is complete, the Bronze layer is validated using the:

```text
validate_bronze()
```

function.

The following tables are validated:

```text
bronze.links
bronze.movies
bronze.tags
bronze.ratings
```

The pipeline performs a row count on each table.

Example validation:

```text
bronze.links: <row_count> rows
bronze.movies: <row_count> rows
bronze.tags: <row_count> rows
bronze.ratings: <row_count> rows
```

If any required Bronze table contains zero rows, the validation fails.

This ensures that the expected Bronze data is available before downstream Silver transformations begin.

---

## 17. Airflow Orchestration

Apache Airflow is used to orchestrate the Bronze ingestion pipeline.

The Airflow DAG is defined in:

```text
airflow_dags/movie_lens_ingestion_dag.py
```

The DAG contains the following tasks:

```text
check_source_files
        ↓
ingest_links
        ↓
ingest_movies
        ↓
ingest_tags
        ↓
ingest_ratings
        ↓
validate_bronze
```

The tasks execute sequentially based on their dependencies.

Each task executes a specific part of the PySpark Bronze ingestion pipeline.

---

## 18. Airflow Task Responsibilities

### check_source_files

Checks whether all required source CSV files are available.

### ingest_links

Loads `links.csv` into `bronze.links`.

### ingest_movies

Loads `movies.csv` into `bronze.movies`.

### ingest_tags

Loads `tags.csv` into `bronze.tags`.

### ingest_ratings

Incrementally loads all ratings files into `bronze.ratings`.

### validate_bronze

Checks that the required Bronze tables contain data after ingestion.

---

## 19. Airflow Scheduling

The ingestion DAG is configured for daily execution.

The DAG can also be triggered manually from the Airflow UI during local development and testing.

Airflow provides:

- Scheduling
- Task dependency management
- Execution monitoring
- Task-level logs
- Failure tracking
- Error visibility

Automatic task retries are disabled so that a failed task is immediately visible during the current development and testing process.

---

## 20. Docker Setup

Apache Airflow is executed locally using Docker.

The Docker configuration is maintained in:

```text
docker-compose.yaml
```

The project uses a custom Airflow Docker image defined in:

```text
Dockerfile
```

The Docker setup includes the services required for Airflow orchestration, including:

- Airflow Webserver
- Airflow Scheduler
- PostgreSQL metadata database

The custom Airflow image also contains the dependencies required by the ingestion pipeline, including Java, PySpark, PostgreSQL connectivity, and Python dependencies.

The PostgreSQL container used by Airflow stores Airflow metadata, while the MovieLens Bronze data is stored in the MovieLens PostgreSQL database running on the host machine.

---

## 21. Project Directory Inside Docker

The project directory is mounted inside the Airflow container at:

```text
/opt/movielens
```

This allows Airflow tasks running inside Docker to access the project source code and required project files.

The Airflow tasks change to the project directory before executing the ingestion commands.

---

## 22. Manual Pipeline Execution

The complete Bronze pipeline can be executed manually from the project root using:

```bash
python -m src.pyspark.ingest_bronze
```

This executes:

```text
check_source_files
        ↓
ingest_links
        ↓
ingest_movies
        ↓
ingest_tags
        ↓
ingest_ratings
        ↓
validate_bronze
```

---

## 23. Running Individual Ingestion Steps

Individual ingestion operations can also be executed separately.

### Check Source Files

```bash
python -m src.pyspark.ingest_bronze check_source_files
```

### Ingest Links

```bash
python -m src.pyspark.ingest_bronze ingest_links
```

### Ingest Movies

```bash
python -m src.pyspark.ingest_bronze ingest_movies
```

### Ingest Tags

```bash
python -m src.pyspark.ingest_bronze ingest_tags
```

### Ingest Ratings

```bash
python -m src.pyspark.ingest_bronze ingest_ratings
```

### Validate Bronze Layer

```bash
python -m src.pyspark.ingest_bronze validate_bronze
```

This modular execution approach also allows Airflow to execute each ingestion operation as an individual task.

---

## 24. Airflow Execution

Airflow tasks execute the Bronze ingestion module from inside the Docker container.

The tasks first move to the mounted project directory:

```bash
cd /opt/movielens
```

and then execute the required ingestion operation.

For example:

```bash
cd /opt/movielens && python -m src.pyspark.ingest_bronze check_source_files
```

Similarly, the remaining Airflow tasks execute their corresponding ingestion operations.

This ensures that Python can correctly resolve the project's `src` package and its modules.

---

## 25. Error Handling

The ingestion functions use exception handling to capture failures during data ingestion.

If an ingestion operation fails:

1. The exception is captured.
2. Failure information is logged where applicable.
3. The corresponding Airflow task is marked as failed.
4. Dependent downstream tasks do not continue normally.
5. The error can be reviewed through Airflow task logs.

This prevents downstream processing from continuing when an upstream ingestion operation has failed.

---

## 26. Security and Configuration Management

Sensitive configuration values are stored in environment variables instead of being hard-coded directly into the source code.

The `.env` file contains environment-specific configuration such as database credentials.

The `.env` file is excluded from Git using:

```text
.env
```

in `.gitignore`.

This prevents sensitive credentials from being pushed to the GitHub repository.

---

## 27. Final Bronze Data Flow

The complete Bronze ingestion flow is:

```text
                    MovieLens Raw CSV Files
                              ↓
                    Check Source Files
                              ↓
                         ingest_links
                          (Append)
                              ↓
                        ingest_movies
                          (Append)
                              ↓
                         ingest_tags
                    (Truncate + Append)
                              ↓
                       ingest_ratings
              (Truncate + Sequential Append)
                              ↓
                      validate_bronze
                              ↓
                 PostgreSQL Bronze Layer
                              ↓
        Silver Deduplication & Transformation
```

---

## 28. Conclusion

The MovieLens Bronze ingestion pipeline provides a structured process for loading raw CSV data into PostgreSQL using PySpark.

The pipeline includes:

- Source file validation
- Explicit schema definition
- PySpark-based ingestion
- PostgreSQL JDBC integration
- Incremental ratings loading
- Duplicate file prevention
- Ingestion logging
- Bronze table validation
- Airflow orchestration
- Docker-based Airflow execution
- Error handling
- Environment-based configuration

The successfully validated Bronze layer serves as the source for the next stage of the project: Silver layer data cleaning and transformation.