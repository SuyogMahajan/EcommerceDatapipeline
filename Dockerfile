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
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# -p says, make parent directories as well if needed
RUN mkdir -p ${SPARK_HOME}

# Fetch latest spark 3.5.x version and download the appropriate binary file
RUN curl -s https://dlcdn.apache.org/spark/ | grep -oP 'spark-3\.5\.[0-9]+' | sort -V | tail -1 | \
    xargs -I {} curl -O https://dlcdn.apache.org/spark/{}/{}-bin-hadoop3.tgz

 # Unpack the file and cleanup the binary file
RUN tar xvzf spark-3.5.*-bin-hadoop*.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.*-bin-hadoop*.tgz


RUN curl -L -o /opt/spark/jars/mysql-connector-j-8.0.33.jar \
https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"

ENTRYPOINT ["/bin/bash"]