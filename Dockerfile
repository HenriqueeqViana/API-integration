
FROM datamechanics/spark:3.2.1-latest


RUN apt-get update && apt-get install -y \
    python3 python3-pip procps curl && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"


WORKDIR /app


COPY . /app


RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8080


CMD ["uvicorn", "api-integration:app", "--host", "0.0.0.0", "--port", "8080"]