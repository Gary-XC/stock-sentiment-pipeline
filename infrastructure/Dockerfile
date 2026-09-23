FROM apache/airflow:2.8.1-python3.11

USER root

# Install Java and procps (needed by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Use the universal Debian symlink for Java to avoid version conflicts
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow