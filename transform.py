import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from extract import ExtractAPI
import os


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

class Transform:
    def __init__(self, spark):
        self.spark = spark
        self.raw_data = None  

    def set_raw_data(self, raw_data):
        """Método para definir os dados brutos."""
        self.raw_data = raw_data

    def read_data(self):
        """Lê os dados brutos e cria um DataFrame."""
        if self.raw_data is None:
            raise ValueError("Os dados brutos não foram definidos. Use 'set_raw_data' para definir os dados.")
        return self.spark.createDataFrame(self.raw_data)

    def save_data(self, funding_year, format="csv", compression="none"):
        """Salva os dados no formato especificado."""
        df = self.read_data()
        
        directory_path = f"./bronze/{funding_year}/files"
        os.makedirs(directory_path, exist_ok=True)
        
        file_path = f"{directory_path}/raw_data"
        
        if format == "csv":
            df.write.csv(file_path, header=True, mode="overwrite", compression=compression)
        elif format == "json":
            df.coalesce(1).write.json(file_path, mode="overwrite", compression='gzip') 
        else:
            raise ValueError("Formato inválido especificado. Use 'csv' ou 'json'.")

    def save_to_silver(self, funding_year, compression="none"):
        """Salva os dados transformados na camada silver."""
        df = self.read_data()
        
        silver_directory_path = f"./silver/{funding_year}/files"
        os.makedirs(silver_directory_path, exist_ok=True)
        
        silver_file_path = f"{silver_directory_path}/processed_data.csv"
        
        df.write.csv(silver_file_path, header=True, mode="overwrite", compression=compression)

    def transform_data(self, file_path):
        """Transforma os dados lidos do arquivo CSV."""
        df = self.spark.read.csv(file_path, header=True)
        df.show()
        df.printSchema()
        
        
        df = df.filter((F.col('form_version') == 'Current') | (F.col('form_version') == 'Original'))

        df = df.withColumn(
            'total_services_requested',
            F.coalesce(F.col('quantity'), F.lit(1)) * F.coalesce(F.col('entities'), F.lit(1))
        )

        df = df.withColumn('minimum_capacity_value', F.regexp_extract(F.col('minimum_capacity'), r'(\d+\.?\d*)', 0)) \
            .withColumn('minimum_capacity_unit', F.regexp_extract(F.col('minimum_capacity'), r'(\D+)', 0))

        df = df.withColumn('maximum_capacity_value', F.regexp_extract(F.col('maximum_capacity'), r'(\d+\.?\d*)', 0)) \
            .withColumn('maximum_capacity_unit', F.regexp_extract(F.col('maximum_capacity'), r'(\D+)', 0))

        return df

    def process(self, funding_year, raw_data=None):
        """Processa os dados e salva na camada silver."""
        
        if raw_data is not None:
            self.set_raw_data(raw_data)  

        
        self.save_data(funding_year, format='json', compression='gzip')  

        bronze_file_path = f"./bronze/{funding_year}/files/raw_data"
        transformed_df = self.transform_data(bronze_file_path)

        
        transformed_df.write.csv(f"./silver/{funding_year}/files/processed_data.csv", header=True, mode="overwrite", compression='gzip')

    def test_transform_data(self, file_path):
        
        transformed_df = self.transform_data(file_path)
        return transformed_df

