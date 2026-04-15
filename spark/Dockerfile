FROM python:3.11-slim-bookworm

# update existing packages
# installing jdk 17
# cleaning unused packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# making enviroment variables
ENV SPARK_HOME="/opt/spark"
# this location hold java binaries like command "java"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# whenever we run a command on bash system check here , here are all the folders who holds different binaries
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN mkdir -p /opt/spark/logs && chmod -R 777 /opt/spark/logs
# -p says, make parent directories as well if needed
RUN mkdir -p ${SPARK_HOME}

# Fetch latest spark 3.5.x version and download the appropriate binary file
RUN curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark.tgz -C ${SPARK_HOME} --strip-components=1 && \
    rm spark.tgz
 # Unpack the file and cleanup the binary file
#RUN tar xvzf spark-3.5.*-bin-hadoop*.tgz --directory ${SPARK_HOME} --strip-components 1 \
#    && rm -rf spark-3.5.*-bin-hadoop*.tgz


RUN curl -L -o /opt/spark/jars/mysql-connector-j-8.0.33.jar \
https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"

ENTRYPOINT ["/bin/bash"]