FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/airflow/src/pyspark\
    /opt/airflow/data/raw\
    /opt/airflow/drivers

COPY drivers/postgresql-42.7.13.jar /opt/airflow/drivers/

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.6 \
    psycopg2-binary==2.9.12 \
    python-dotenv==1.2.3