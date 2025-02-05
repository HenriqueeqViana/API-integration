import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from extract import ExtractAPI
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

class Transform:
    def __init__(self, raw_data, spark):
        self.raw_data = raw_data
        self.spark = spark

    def read_data(self):
        return self.spark.createDataFrame(self.raw_data)

    def save_data(self, funding_year, format="csv", compression="none"):
        df = self.read_data()
        directory_path = f"./bronze/{funding_year}/files"
        os.makedirs(directory_path, exist_ok=True)
        file_path = f"{directory_path}/raw_data"
        
        if format == "csv":
            df.write.csv(file_path, header=True, mode="overwrite", compression=compression)
        elif format == "json":
            df.coalesce(1).write.json(file_path, mode="overwrite", compression='gzip') 
        else:
            raise ValueError("Invalid format specified. Please use 'csv' or 'json'.")

    def transform_data(self, file_path):
        df = self.spark.read.json(file_path)
        df.show(1)
        df.printSchema()
        df = df.filter((F.col('form_version') == 'Current') | (F.col('form_version') == 'Original'))
        required_columns = [
            'application_number', 'rfp_documents', 
            'rfp_upload_date', 'rfp_identifier', 
            'minimum_capacity', 'maximum_capacity', 
            'quantity', 'entities', 'funding_year', 
            'fcc_form_470_status', 'allowable_contract_date',
            'certified_date_time', 'last_modified_date_time'
        ]

        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"A coluna '{column}' não está presente no DataFrame.")

        df = df.withColumn(
            'total_services_requested',
            F.coalesce(F.col('quantity').cast('int'), F.lit(1)) * F.coalesce(F.col('entities').cast('int'), F.lit(1))
        )

        df = df.withColumn('minimum_capacity_value', F.regexp_extract(F.col('minimum_capacity'), r'(\d+\.?\d*)', 0)) \
            .withColumn('minimum_capacity_unit', F.regexp_extract(F.col('minimum_capacity'), r'(\D+)', 0))

        df = df.withColumn('maximum_capacity_value', F.regexp_extract(F.col('maximum_capacity'), r'(\d+\.?\d*)', 0)) \
            .withColumn('maximum_capacity_unit', F.regexp_extract(F.col('maximum_capacity'), r'(\D+)', 0))

        rfp_df = df.select(
            'application_number', 
            df.rfp_documents.url.alias('request_for_proposal_document'), 
            'rfp_identifier', 
            'rfp_upload_date', 
            'minimum_capacity_value', 
            'minimum_capacity_unit',
            'maximum_capacity_value', 
            'maximum_capacity_unit', 
            'total_services_requested', 
            'form_version', 
            'funding_year', 
            'fcc_form_470_status', 
            'allowable_contract_date', 
            'certified_date_time', 
            'last_modified_date_time'
        )

        return rfp_df

    def save_tables(self, funding_year):
        rfp_df, billed_entities_df, contacts_df, services_df = self.transform_data(f"./bronze/{funding_year}/files/raw_data")
        base_path = f"./silver/{funding_year}/files"
        os.makedirs(base_path, exist_ok=True)

        rfp_df.write.csv(f"{base_path}/rfp_table.csv", header=True, mode="overwrite")
        billed_entities_df.write.csv(f"{base_path}/billed_entities_table.csv", header=True, mode="overwrite")
        contacts_df.write.csv(f"{base_path}/contacts_table.csv", header=True, mode="overwrite")
        services_df.write.csv(f"{base_path}/services_table.csv", header=True, mode="overwrite")

        rfp_count = rfp_df.select('application_number').distinct().count()
        avg_services_requested = rfp_df.agg(F.avg('total_services_requested')).first()[0]

        stats_df = self.spark.createDataFrame([(funding_year, rfp_count, avg_services_requested)], 
                                               ['funding_year', 'rfps_analyzed', 'average_services_requested'])

        stats_file_path = f"./silver/{funding_year}/files/stats_table.csv"
        stats_df.write.csv(stats_file_path, header=True, mode="overwrite")

    def process(self, funding_year):
        self.save_data(funding_year, format='json', compression='gzip')  
        self.save_tables(funding_year)

if __name__ == "__main__":
    dataset_id = "jt8s-3q52"
    base_url = "opendata.usac.org"
    api_client = ExtractAPI(base_url, dataset_id)
    funding_year = "2024"
    
    spark = SparkSession.builder \
        .appName("Transform") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    data = api_client.get_complete_data(funding_year)
    transform = Transform(data, spark)
    transform.process(funding_year)

    print("Processing completed.")