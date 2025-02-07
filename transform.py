import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import os

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

        rfp_df = df.select(
            'application_number', 
            df.rfp_documents.url.alias('request_for_proposal_document'), 
            'rfp_identifier', 
            'rfp_upload_date', 
            'total_services_requested', 
            'certified_date_time'
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

    def save_tables(self, rfp_df, billed_entities_df, contacts_df, services_df, funding_year):
        base_path = f"./silver/{funding_year}/files"
        os.makedirs(base_path, exist_ok=True)

        rfp_df.write.csv(f"{base_path}/rfp_table.csv", header=True, mode="overwrite")
        billed_entities_df.write.csv(f"{base_path}/billed_entities_table.csv", header=True, mode="overwrite")
        contacts_df.write.csv(f"{base_path}/contacts_table.csv", header=True, mode="overwrite")
        services_df.write.csv(f"{base_path}/services_table.csv", header=True, mode="overwrite")

        monthly_summary = rfp_df.withColumn("month", F.month("certified_date_time")) \
                                 .groupBy("month") \
                                 .agg(F.avg("total_services_requested").alias("avg_services_requested"))

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

        monthly_summary = monthly_summary.select("month_name", "avg_services_requested")
        monthly_summary.write.csv(f"{base_path}/monthly_summary.csv", header=True, mode="overwrite")

    def process(self, funding_year):
        self.save_data(funding_year, format='json', compression='gzip')  
        rfp_df, billed_entities_df, contacts_df, services_df = self.transform_data(f"./bronze/{funding_year}/files/raw_data")
        self.save_tables(rfp_df, billed_entities_df, contacts_df, services_df, funding_year)