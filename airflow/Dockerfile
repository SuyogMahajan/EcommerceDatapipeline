FROM apache/airflow:2.8.1

USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Spark setup
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN mkdir -p /opt/spark

# Install Spark
RUN curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark.tgz -C /opt/spark --strip-components=1 && \
    rm spark.tgz

RUN curl -L -o /opt/spark/jars/mysql-connector-j-8.0.33.jar \
https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# Spark operator
USER airflow

RUN pip install apache-airflow-providers-apache-spark

USER airflow