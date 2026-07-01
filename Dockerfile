FROM apache/airflow:2.8.1-python3.10

USER root

# Install OpenJDK-17 and procps (provides the missing 'ps' command for Spark process mapping)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable (Updated from amd64 to arm64 for Apple Silicon Mac)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

USER airflow

# Install Python project dependencies inside the container
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt