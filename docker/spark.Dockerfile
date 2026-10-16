# Spark 3.5 with Python, plus our package and the Postgres driver.
FROM apache/spark:3.5.3-python3
USER root
WORKDIR /opt/app
COPY pyproject.toml README.md ./
COPY src ./src
COPY jobs ./jobs
RUN pip install --no-cache-dir "psycopg[binary]>=3.1" pydantic && pip install --no-cache-dir --no-deps .
ENV PATH="/opt/spark/bin:${PATH}"
