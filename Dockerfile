FROM python:3.11-buster


ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app


COPY . /app


RUN pip3 install --no-cache-dir -r requirements.txt


EXPOSE 8080


CMD ["uvicorn", "api-integration:app", "--host", "0.0.0.0", "--port", "8080"]