import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import os
import logging
from extract import ExtractAPI

class Transform:
    def __init__(self, raw_data, spark):
        if not isinstance(raw_data, list):
            raise ValueError("raw_data must be a list of dictionaries.")
        self.raw_data = raw_data
        self.spark = spark
        logging.info(len(self.raw_data))

    def read_data(self):
        logging.info(self.spark.createDataFrame(self.raw_data).count())
        return self.spark.createDataFrame(self.raw_data )

    def save_data(self,funding_year, format="csv", compression="none"):
        df = self.read_data()
        directory_path = f"./bronze/{funding_year}/files"
        os.makedirs(directory_path, exist_ok=True)
        file_path = f"{directory_path}/raw"

        if format == "csv":
            df.write.csv(f"{file_path}.csv", header=True, mode="overwrite", compression=compression)
        elif format == "json":
            df.coalesce(1).write.json(f"{file_path}.json", mode="overwrite", compression='gzip')
        else:
            raise ValueError("Invalid format specified. Please use 'csv' or 'json'.")

    def transform_data(self):
        df = self.read_data()
    
   
        df = df.filter((F.col('form_version') == 'Current') | (F.col('form_version') == 'Original'))

    
        df = df.select(
        '*',
        F.coalesce(F.col('quantity').cast('int'), F.lit(1)).alias('total_services_requested'),
        F.regexp_extract(F.col('minimum_capacity'), r'(\d+\.?\d*)', 0).alias('minimum_capacity_value'),
        F.regexp_extract(F.col('minimum_capacity'), r'(\D+)', 0).alias('minimum_capacity_unit'),
        F.regexp_extract(F.col('maximum_capacity'), r'(\d+\.?\d*)', 0).alias('maximum_capacity_value'),
        F.regexp_extract(F.col('maximum_capacity'), r'(\D+)', 0).alias('maximum_capacity_unit')
        )

    
        rfp_df = df.select(
        'application_number', 
        df.rfp_documents.url.alias('request_for_proposal_document'), 
        'rfp_identifier', 
        'rfp_upload_date', 
        'total_services_requested', 
        'certified_date_time'
        )

    
        billed_entities_df = df.select(
        F.expr("uuid()").alias("unique_id"),
        'billed_entity_name',
        'billed_entity_number',
        'billed_entity_phone',
        'billed_entity_email',
        'billed_entity_city',
        'billed_entity_state',
        'billed_entity_zip'
        )

   
        contacts_df = df.select(
        F.expr("uuid()").alias("unique_id"),
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
        F.expr("uuid()").alias("unique_id"),
        'service_request_id',
        'service_category',
        'service_type'
        )

        return rfp_df, billed_entities_df, contacts_df, services_df  

    def save_tables(self, rfp_df, billed_entities_df, contacts_df, services_df, funding_year):
        base_path = f"./silver/{funding_year}/files"
        os.makedirs(base_path, exist_ok=True)

        rfp_df.write.csv(f"{base_path}/rfp_table.csv", header=True, mode="overwrite")
        billed_entities_df.write.csv(f"{base_path}/billed_entities_table.csv", header=True, mode="overwrite")
        contacts_df.write.csv(f"{base_path}/contacts_table.csv", header=True, mode="overwrite")
        services_df.write.csv(f"{base_path}/services_table.csv", header=True, mode="overwrite")

        monthly_summary = rfp_df.withColumn("month", F.month("certified_date_time")) \
                                 .groupBy("month") \
                                 .agg(F.round(F.avg("total_services_requested")).alias("avg_services_requested"))

        monthly_summary = monthly_summary.withColumn("month_name", F.expr("CASE month " +
            "WHEN 1 THEN 'january' " +
            "WHEN 2 THEN 'february' " +
            "WHEN 3 THEN 'march' " +
            "WHEN 4 THEN 'april' " +
            "WHEN 5 THEN 'may' " +
            "WHEN 6 THEN 'june' " +
            "WHEN 7 THEN 'july' " +
            "WHEN 8 THEN 'august' " +
            "WHEN 9 THEN 'september' " +
            "WHEN 10 THEN 'october' " +
            "WHEN 11 THEN 'november' " +
            "WHEN 12 THEN 'december' END"))

        monthly_summary = monthly_summary.select("month_name", "avg_services_requested","month")
        monthly_summary.write.csv(f"{base_path}/monthly_summary.csv", header=True, mode="overwrite")

    def process(self, funding_year):
        self.save_data(funding_year, format='json', compression='gzip')
        rfp_df, billed_entities_df, contacts_df, services_df = self.transform_data()
        self.save_tables(rfp_df, billed_entities_df, contacts_df, services_df, funding_year)


dataset_id = os.getenv("DATASET_ID", "jt8s-3q52")
base_url = os.getenv("BASE_URL", "opendata.usac.org")
funding_year = os.getenv("FUNDING_YEAR", "2024")

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

spark = SparkSession.builder \
    .appName("FastAPI-Spark") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

api_client = ExtractAPI(base_url, dataset_id)
data = api_client.get_complete_data(funding_year)
print(len(data))
transformer = Transform(data, spark)   
df= transformer.read_data()
df.count()
