FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm


RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    openjdk-17-jre \
    wget \
    curl \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


RUN cd /tmp && \
    wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark-3.5.0-bin-hadoop3.tgz -C / && \
    mv /spark-3.5.0-bin-hadoop3 /spark && \
    rm -rf /tmp/spark-3.5.0-bin-hadoop3.tgz


ENV SPARK_HOME=/spark


WORKDIR /api-integration


COPY . /api-integration


RUN pip3 install --no-cache-dir -r requirements.txt


EXPOSE 8080


CMD ["uvicorn", "api-integration:app", "--host", "0.0.0.0", "--port", "8080"]