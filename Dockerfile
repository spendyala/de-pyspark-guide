FROM python:3.10-slim

# Set environment variables to non-interactive
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages including Java (Spark dependency)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    bash \
    tini && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Apache Spark
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN curl -fsSL "${SPARK_TGZ_URL}" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt --owner root --group root --no-same-owner && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# Set Spark environment variables
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Create a working directory for notebooks
WORKDIR /opt/spark/work-dir
RUN chmod -R 777 /opt/spark/work-dir

# Expose Jupyter port
EXPOSE 8888

# Set entrypoint to use tini and start JupyterLab
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"] 