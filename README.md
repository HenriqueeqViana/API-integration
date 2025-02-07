# API-integration

This project provides a FastAPI service to extract, transform, and store data using PySpark.

## 📌 Requirements
- Python 3.8+
- PySpark 3.5.1
- FastAPI
- Requests
- Sodapy (for API requests)
- JDK 11
## 🚀 How to Install local running using ( Not recomendary)
1. Clone this repo:
```bash
   git clone https://github.com/HenriqueeqViana/API-integration
   cd API-integration
``` 
2. Set up a virtual environment: 
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
``` 
3. Install dependencies:
```bash
pip install -r requirements.txt
``` 
## 🏃 Running the API Locally
1. Run the API:
```bash
python api-integration.py
``` 

📡 API Endpoints
### Method	Endpoint	Description
- POST	/run_pipeline	Extracts, transforms, and saves data.
- POST	/extract	Only extracts and saves raw data.
- POST	/transform	Processes raw data and stores the transformed data.
- GET	/read_csv/{filename}	Reads a stored CSV file.
- GET	/summary	Returns execution summary.
🔹 Example Requests

- Run Full Pipeline
```bash
curl -X 'POST' 'http://localhost:8080/run_pipeline' -H 'accept: application/json'
```
- Extract Data
```bash
curl -X 'POST' 'http://localhost:8080/extract' -H 'accept: application/json'
```
- Read CSV
```bash
curl -X 'GET' 'http://localhost:8080/read_csv/rfp_table.csv' -H 'accept: application/json'
```
- Get Summary
```bash
curl -X 'GET' 'http://localhost:8080/summary' -H 'accept: application/json'
```
🐳 Running with Docker
- Build the Docker Image
```bash
docker build -t api-integration-spark-service .
```
- Run the Container
```bash
docker run -p 8000:8000  api-integration-spark-service 
```
### The API will be available at:
- http://localhost:8000/

⚡ Notes

- The API automatically extracts data from the Socrata API and processes it using PySpark.
- The run_pipeline endpoint runs the full ETL process.
- Data is stored in CSV format inside the ./silver/{funding_year}/files directory.