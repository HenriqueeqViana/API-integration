FROM jupyter/base-notebook:latest

USER root


RUN apt-get update && \
    apt-get install -y openjdk-11-jdk





ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3


RUN wget "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz" && \
    tar -xvzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /usr/lib && \
    rm spark-3.5.1-bin-hadoop3.tgz

ENV SPARK_HOME=/usr/lib/spark-3.5.1-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin


WORKDIR /api-integration


COPY . /api-integration


RUN pip3 install --no-cache-dir -r requirements.txt


EXPOSE 8080


CMD ["uvicorn", "api-integration:app", "--host", "0.0.0.0", "--port", "8080"]