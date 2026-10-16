# Spark 3.5 via the pyspark wheel on Python 3.11 (the official apache/spark image ships
# Python 3.8, which is too old for this package), plus Java 17 and the Postgres driver.
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/app
COPY pyproject.toml README.md ./
COPY src ./src
COPY jobs ./jobs
RUN pip install --no-cache-dir ".[spark]"
