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
        df = df.filter((F.col('form_version') == 'Current') | (F.col('form_version') == 'Original'))

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
        
        billed_entities_df = df.select(
            'billed_entity_name',
            'billed_entity_number',
            'billed_entity_phone',
            'billed_entity_email',
            'billed_entity_city',
            'billed_entity_state',
            'billed_entity_zip'
        )

        contacts_df = df.select(
            'contact_name',
            'contact_email',
            'contact_phone',
            'contact_phone_ext',
            'contact_address1',
            'contact_address2',
            'contact_city',
            'contact_state',
            'contact_zip'
        )

        services_df = df.select(
            'service_category',
            'service_type',
            'service_request_id'
        )

        return rfp_df, billed_entities_df, contacts_df, services_df

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

