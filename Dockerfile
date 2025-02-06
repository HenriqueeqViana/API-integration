
FROM ubuntu:20.04


RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"


RUN curl -sLf --retry 3 --retry-delay 5 \
    "https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.tgz" \
    | tar xz -C /opt/ && \
    ln -s /opt/spark-3.2.1-bin-hadoop3 /opt/spark


WORKDIR /app


COPY . /app


RUN pip3 install --no-cache-dir -r requirements.txt


EXPOSE 8080


CMD ["uvicorn", "api-integration:app", "--host", "0.0.0.0", "--port", "8080"]