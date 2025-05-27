FROM bitnami/spark:3.5.0 AS spark_base

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER 1001

FROM spark_base AS app_builder

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY postgresql-42.6.0.jar /opt/bitnami/spark/jars/

COPY src/ ./src/
COPY tests/ ./tests/

CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/src/main.py"]