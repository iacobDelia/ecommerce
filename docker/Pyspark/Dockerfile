FROM ubuntu:22.04

# Install JDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install Python, pip, and venv support
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python3 -m venv /opt/venv

# Make venv the default Python & Pip
ENV PATH="/opt/venv/bin:$PATH"

# Install PySpark (downgraded to match Snowflake Spark connector)
RUN pip install --upgrade pip && \
    pip install pyspark==3.4.4 numpy snowflake-connector-python

# Make the directory for the jdbc driver and copy it
RUN mkdir -p /opt/spark/jars

COPY src/ /app/

WORKDIR /app


CMD ["python", "main.py"]


