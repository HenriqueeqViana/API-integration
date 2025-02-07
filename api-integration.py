from fastapi import FastAPI, HTTPException
from extract import ExtractAPI
from transform import Transform
from pyspark.sql import SparkSession
import os
import json
import logging
import uvicorn
import pyspark.sql.functions as F

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dataset_id = os.getenv("DATASET_ID", "jt8s-3q52")
base_url = os.getenv("BASE_URL", "http://opendata.usac.org")
funding_year = os.getenv("FUNDING_YEAR", "2024")

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

spark = SparkSession.builder \
    .appName("FastAPI-Spark") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

os.makedirs(f"./bronze/{funding_year}/files", exist_ok=True)
os.makedirs(f"./silver/{funding_year}/files", exist_ok=True)

@app.post("/run_pipeline")
def run_pipeline():
    logger.info("Starting pipeline...")
    api_client = ExtractAPI(base_url, dataset_id)
    data = api_client.get_complete_data(funding_year)
    
    if not data:
        raise HTTPException(status_code=400, detail="No data extracted.")
    
    transformer = Transform(data, spark)
    transformer.process(funding_year)
    
    return generate_summary_response()

@app.post("/extract")
def extract_data():
    logger.info("Extracting data...")
    api_client = ExtractAPI(base_url, dataset_id)
    data = api_client.get_complete_data(funding_year)
    
    if not data:
        raise HTTPException(status_code=400, detail="No data extracted.")
    
    transformer = Transform(data, spark)
    transformer.save_data(funding_year, format='json', compression='gzip')  
    
    
    return {"message": "Data extracted successfully"}

@app.post("/transform")
def transform_data():
    logger.info("Transforming data...")
    raw_data_path = f"./bronze/{funding_year}/files/*.json"
    if not os.path.exists(raw_data_path):
        raise HTTPException(status_code=400, detail="No extracted data found for transformation.")
    
    with open(raw_data_path, "r") as f:
        data = json.load(f)
    
    transformer = Transform(data, spark)
    rfp_df, billed_entities_df, contacts_df, services_df = transformer.transform_data(f"./bronze/{funding_year}/files/raw_data")
    transformer.save_tables(rfp_df, billed_entities_df, contacts_df, services_df, funding_year)
    return generate_summary_response()

@app.get("/read_csv/{filename}")
def read_csv(filename: str):
    logger.info(f"Reading CSV file: {filename}")
    file_path = f"./silver/{funding_year}/files/{filename}"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        return df.limit(100).toJSON().collect()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV file: {str(e)}")

@app.get("/summary")
def generate_summary_response():
    logger.info("Generating summary...")
    base_path = f"./silver/{funding_year}/files"
    total_files = len([f for f in os.listdir(base_path) if f.endswith(".csv")])
    
    summary_file_path = f"{base_path}/monthly_summary.csv"
    if os.path.exists(summary_file_path):
        summary_df = spark.read.csv(summary_file_path, header=True, inferSchema=True)
        
        summary = {
            "ano": funding_year,
            "total_files_created": total_files,
            "rfps_analyzed": summary_df.count(),
        }

        monthly_counts = {row.month_name: row.avg_services_requested for row in summary_df.collect()}
        summary.update(monthly_counts)

    else:
        summary = {
            "ano": funding_year,
            "total_files_created": total_files,
            "rfps_analyzed": 0,
        }

    return summary

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)