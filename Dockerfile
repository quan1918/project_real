FROM apache/airflow:2.9.3

USER root

# Cài đặt các gói cần thiết và Java
RUN apt-get update && \
    apt-get install -y curl wget software-properties-common gnupg openjdk-17-jdk && \
    apt-get clean

# Cài numpy vào Python hệ thống để Spa  `rk có thể dùng khi gọi spark-submit
RUN bash -c "sudo -u airflow pip install --no-cache-dir numpy pandas pyspark==3.5.1"

# Cài Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

RUN mkdir -p ${SPARK_HOME} && \
    curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME}/ && \
    rm -rf /tmp/spark.tgz

# Chuyển về user airflow
USER airflow

