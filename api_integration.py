from fastapi import FastAPI, HTTPException
from extract import ExtractAPI
from transform import Transform
from pyspark.sql import SparkSession
import os
import json

app = FastAPI()

dataset_id = "jt8s-3q52"
base_url = "opendata.usac.org"
funding_year = "2024"

# Initialize Spark
spark = SparkSession.builder \
    .appName("FastAPI-Spark") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

@app.post("/run_pipeline")
def run_pipeline():
    api_client = ExtractAPI(base_url, dataset_id)
    data = api_client.get_complete_data(funding_year)
    
    if not data:
        raise HTTPException(status_code=400, detail="No data extracted.")
    
    transformer = Transform(data, spark)
    transformer.process(funding_year)
    
    return generate_summary_response()

@app.post("/extract")
def extract_data():
    api_client = ExtractAPI(base_url, dataset_id)
    data = api_client.get_complete_data(funding_year)
    
    if not data:
        raise HTTPException(status_code=400, detail="No data extracted.")
    
    raw_data_path = f"./bronze/{funding_year}/files/raw_data.json"
    os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)
    
    with open(raw_data_path, "w") as f:
        json.dump(data, f)
    
    return {"message": "Data extracted successfully"}

@app.post("/transform")
def transform_data():
    raw_data_path = f"./bronze/{funding_year}/files/raw_data.json"
    if not os.path.exists(raw_data_path):
        raise HTTPException(status_code=400, detail="No extracted data found for transformation.")
    
    with open(raw_data_path, "r") as f:
        data = json.load(f)
    
    transformer = Transform(data, spark)
    transformer.process(funding_year)
    return generate_summary_response()

@app.get("/read_csv/{filename}")
def read_csv(filename: str):
    file_path = f"./silver/{funding_year}/files/{filename}"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df.limit(100).toJSON().collect()

@app.get("/summary")
def generate_summary_response():
    base_path = f"./silver/{funding_year}/files"
    total_files = len([f for f in os.listdir(base_path) if f.endswith(".csv")])
    
    rfp_file = f"{base_path}/rfp_table.csv"
    if os.path.exists(rfp_file):
        df = spark.read.csv(rfp_file, header=True, inferSchema=True)
        rfp_count = df.select("application_number").distinct().count()
        avg_services_requested = df.selectExpr("avg(total_services_requested)").collect()[0][0]
    else:
        rfp_count = 0
        avg_services_requested = 0
    
    return {
        "total_files_created": total_files,
        "rfps_analyzed": rfp_count,
        "average_services_requested_per_month": avg_services_requested
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
