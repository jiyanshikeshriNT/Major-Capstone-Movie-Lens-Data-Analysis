import os

from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "movielens")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# JDBC URL used by PySpark
POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


# JDBC properties used by PySpark
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}


# Path Configuration
DATA_DIR = Path("data/raw")

EXPECTED_FILES = [
    "links.csv",
    "movies.csv",
    "ratings_part1.csv",
    "ratings_part2.csv",
    "ratings_part3.csv",
    "ratings_part4.csv",
    "ratings_part5.csv",
    "tags.csv"
]