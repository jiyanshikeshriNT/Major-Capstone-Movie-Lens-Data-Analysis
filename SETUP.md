MovieLens Data Engineering Pipeline — Setup Guide

This guide explains how to set up and run the MovieLens Data Engineering project on a local system.

The project uses:

- Python 3.11
- Java 17
- Apache Spark / PySpark 3.5.x
- PostgreSQL
- Docker Desktop
- Apache Airflow
- PostgreSQL JDBC Driver
- Hadoop configuration for Windows

---

## 1. Clone the Repository

Open a terminal and clone the project repository.

git clone https://github.com/jiyanshikeshriNT/Major-Capstone-Movie-Lens-Data-Analysis

Move into the project directory:

cd MovieLens

---

## 2. Verify Project Structure

The repository should contain a structure similar to:

```text
MovieLens-Data-Analysis/
│
├── data/
│   └── raw/
│       ├── links.csv
│       ├── movies.csv
│       ├── tags.csv
│       ├── ratings_part1.csv
│       ├── ratings_part2.csv
│       ├── ratings_part3.csv
│       ├── ratings_part4.csv
│       └── ratings_part5.csv
│
├── src/
│   ├── python/
│   │   ├── config.py
│   │   └── utils.py
│   │
│   └── pyspark/
│       ├── ingest_bronze.py
│       ├── transform_silver.py
│       └── transform_gold.py
│
├── airflow_dags/
│
├── docs/
│
├── drivers/
│   └── postgresql-42.7.13.jar
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .gitignore
├── README.md
└── SETUP.md
```

---

## 3. Install Required Software

Install the following software before running the project.

Python

Recommended version:

Python 3.11

Verify:

python --version

Expected output should be similar to:

Python 3.11.x

---

Java

Install Java 17.

Verify:

java -version

The project was developed using:

Java 17

Make sure the "JAVA_HOME" environment variable points to the Java 17 installation.

Example:

JAVA_HOME=C:\Program Files\Java\jdk-17

Add Java to the system "PATH" if required.

---

Apache Spark

Install Apache Spark 3.5.x.

Verify:

spark-submit --version

The development environment used Spark 3.5.x.

On Windows, ensure the required Hadoop environment is also configured.

Example environment variables:

SPARK_HOME=C:\spark\spark-3.5.x-bin-hadoop3
HADOOP_HOME=C:\hadoop

Add the following directories to "PATH":

%SPARK_HOME%\bin
%HADOOP_HOME%\bin

The Hadoop "bin" directory should contain the required Windows Hadoop utilities such as "winutils.exe".

Restart the terminal after modifying environment variables.

---

## 4. Create a Python Virtual Environment

From the project root:

python -m venv venv

Activate it on Windows:

venv\Scripts\activate

For Linux/macOS:

source venv/bin/activate

---

## 5. Install Python Dependencies

Install the project dependencies:

pip install -r requirements.txt

Verify PySpark:

python -c "import pyspark; print(pyspark.__version__)"

---

## 6. Install PostgreSQL

Install PostgreSQL and ensure the PostgreSQL server is running.

Create the project database.

Example:

CREATE DATABASE movielens;

The pipeline automatically creates or manages the required Bronze, Silver, Gold, Quarantine, logging, staging, and analytical assets as required by the project logic.

The primary schemas used by the project are:

bronze
silver
quarantine
gold

---

## 7. Configure PostgreSQL Environment Variables

Create a ".env" file in the project root.

Example:

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=movielens
POSTGRES_USER=<your_postgres_username>
POSTGRES_PASSWORD=<your_postgres_password>

Replace the username and password with your own PostgreSQL credentials.

Do not commit the ".env" file to Git.

Make sure ".env" is included in ".gitignore".

---

## 8. PostgreSQL Host for Local and Docker Execution

When executing the pipeline directly from Windows, PostgreSQL can be accessed using:

POSTGRES_HOST=localhost

When Airflow runs inside Docker and PostgreSQL runs on the Windows host, Docker accesses the host PostgreSQL instance using:

host.docker.internal

Therefore the Airflow/Docker environment should use:

POSTGRES_HOST=host.docker.internal

while normal local execution can use:

POSTGRES_HOST=localhost

---

## 9. Verify PostgreSQL JDBC Driver

The project uses the PostgreSQL JDBC driver:

drivers/postgresql-42.7.13.jar

Verify that the file exists:

MovieLens/
`-- drivers/
    `-- postgresql-42.7.13.jar

The SparkSession uses this JDBC driver to communicate with PostgreSQL.

---

## 10. Add the MovieLens Dataset

Place the MovieLens source files inside:

data/raw/

The directory should contain:

data/raw/
|
|-- links.csv
|-- movies.csv
|-- tags.csv
|-- ratings_part1.csv
|-- ratings_part2.csv
|-- ratings_part3.csv
|-- ratings_part4.csv
`-- ratings_part5.csv

The pipeline performs source-file validation before starting ingestion.

If one of the expected source files is missing, the Bronze pipeline will fail before downstream processing begins.

---

## 11. Verify Local Configuration

Before running the complete pipeline, verify:

python --version

java -version

spark-submit --version

docker --version

docker compose version

Also make sure:

PostgreSQL is running
.env exists
JDBC driver exists
MovieLens source files exist in data/raw/
Docker Desktop is running

---

## 12. Run Bronze Pipeline Locally

From the project root:

python -m src.pyspark.ingest_bronze

The Bronze process performs:

Source File Validation
        ↓
Links Ingestion
        ↓
Movies Ingestion
        ↓
Tags Ingestion
        ↓
Ratings Ingestion
        ↓
Bronze Validation

Individual Bronze operations can also be executed using:

python -m src.pyspark.ingest_bronze check_source_files

python -m src.pyspark.ingest_bronze ingest_links

python -m src.pyspark.ingest_bronze ingest_movies

python -m src.pyspark.ingest_bronze ingest_tags

python -m src.pyspark.ingest_bronze ingest_ratings

python -m src.pyspark.ingest_bronze validate_bronze

---

## 13. Run Silver Transformations Locally

Silver transformations can be executed individually.

python -m src.pyspark.transform_silver transform_links

python -m src.pyspark.transform_silver transform_movies

python -m src.pyspark.transform_silver transform_ratings

python -m src.pyspark.transform_silver transform_tags

python -m src.pyspark.transform_silver transform_movie_metadata

python -m src.pyspark.transform_silver transform_user_ratings_master

The Silver layer performs operations including:

Schema Enforcement
Cleaning
Deduplication
Quarantine Handling
Column Standardization
Audit Column Generation
Staging
PostgreSQL UPSERT
Transformation Logging

---

## 14. Run Gold Transformations Locally

Gold transformations can also be executed individually.

python -m src.pyspark.transform_gold transform_movie_insight

python -m src.pyspark.transform_gold transform_user_insight

python -m src.pyspark.transform_gold transform_genre_insight

python -m src.pyspark.transform_gold transform_yearly_insight

For normal end-to-end execution, Airflow should be used to manage the pipeline dependencies.

---

## 15. Start Airflow Using Docker

Make sure Docker Desktop is running.

From the project root execute:

docker compose up -d

Check running containers:

docker compose ps

Airflow runs inside Docker while the project directory is mounted into the Airflow environment.

The project is available inside the Airflow containers at:

/opt/movielens

---

## 16. Airflow Pipeline

The complete Airflow workflow is:

Bronze DAG
    ↓
Bronze Validation
    ↓
Trigger Silver DAG
    ↓
Silver Transformations
    ↓
Trigger Gold DAG
    ↓
Gold Transformations

The main DAGs are:

movielens_bronze_ingestion
movielens_silver_transforms
movielens_gold_transforms

Start the pipeline using the Bronze ingestion DAG.

The downstream Silver and Gold processing follows the configured DAG dependencies.

---

## 17. Monitor Docker Logs

To inspect Airflow container logs:

docker compose logs

To continuously follow logs:

docker compose logs -f

To view the running containers:

docker compose ps

---

## 18. Stop the Airflow Environment

To stop Docker services:

docker compose down

To start them again:

docker compose up -d

---

## 19. Verify Database Output

After successful execution, PostgreSQL should contain the main assets below.

Bronze

bronze.links
bronze.movies
bronze.ratings
bronze.tags
bronze.ingestion_log

Silver

silver.links
silver.movies
silver.ratings
silver.tags
silver.movie_metadata
silver.user_ratings_master
silver.transformation_log

Quarantine

Created when rejected records exist:

quarantine.links
quarantine.movies
quarantine.ratings
quarantine.tags

Gold

gold.movie_insight
gold.user_insight
gold.genre_insight
gold.yearly_insight
gold.transformation_log

---

## 20. Expected Data Flow

After setup, the complete project flow is:

MovieLens CSV Files
        ↓
PySpark Bronze Ingestion
        ↓
PostgreSQL Bronze
        ↓
PySpark Silver Transformations
        ↓
PostgreSQL Silver
        ↓
Quarantine for Rejected Records
        ↓
PySpark Gold Transformations
        ↓
PostgreSQL Gold
        ↓
Reporting Views
        ↓
Tableau Dashboards

---

## 21. Common Setup Checks

If the project does not start correctly, verify the following first:

1. Python 3.11 is available
2. Java 17 is configured correctly
3. JAVA_HOME is configured
4. Spark is configured
5. SPARK_HOME is configured
6. Hadoop configuration is available on Windows
7. HADOOP_HOME is configured
8. PostgreSQL server is running
9. PostgreSQL credentials in .env are correct
10. movielens database exists
11. PostgreSQL JDBC driver exists in drivers/
12. Required CSV files exist in data/raw/
13. Docker Desktop is running
14. Airflow containers are running

Check Docker containers using:

docker compose ps

Check Python imports using:

python -c "import pyspark; import psycopg2; print('Python dependencies OK')"

---

## 22. Recommended Execution

For normal use, the recommended execution approach is:

1. Start PostgreSQL
2. Start Docker Desktop
3. Start Airflow using docker compose
4. Trigger the Bronze DAG
5. Allow Airflow to execute Bronze → Silver → Gold
6. Validate PostgreSQL tables and transformation logs
7. Connect the processed datasets/reporting views to Tableau

This keeps the complete processing sequence controlled through Airflow instead of manually running every transformation.