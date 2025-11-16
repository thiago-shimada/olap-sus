FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.3}

USER root

RUN apt-get update && \
    apt-get install -y default-jdk curl && \
    apt-get clean

# Create directory for Spark JARs and download dependencies
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    chmod 644 /opt/spark/jars/*.jar && \
    chown -R airflow:root /opt/spark/jars

USER airflow

RUN pip install --no-cache-dir \
    pyspark \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres