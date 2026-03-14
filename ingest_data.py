import logging
import time
import requests
import pandas as pd
import os

def ingest_taxi_data(**kwargs):
    url = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    output_path = "/tmp/nyc_taxi_raw.csv"
    max_retries = 3

    output_dir = "/opt/airflow/dags/output"
    output_path = os.path.join(output_dir, "nyc_taxi_raw.csv")
    
    max_records = 1000
    max_retries = 3
    
    logger = logging.getLogger("airflow.task")

    # 2. สร้างโฟลเดอร์ปลายทางเตรียมไว้เลย (ถ้ามีอยู่แล้วก็ไม่เป็นไร)
    os.makedirs(output_dir, exist_ok=True)

    for attempt in range(max_retries):
        try:
            logging.info(f"Downloading NYC taxi data (Attempt {attempt + 1}/{max_retries})...")
            with requests.get(url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            logging.info("Download finished.")
            break
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise Exception("Failed to download NYC taxi data after 3 attempts.") from e

    logging.info("Validating downloaded dataset...")
    df = pd.read_csv(output_path)
    row_count = len(df)
    
    logging.info(f"Downloaded dataset contains {row_count} rows.")
    
    if row_count < 1000:
        raise ValueError(f"Data validation failed: Expected at least 1000 rows, but got {row_count}.")
    
    ti = kwargs['ti']
    ti.xcom_push(key='taxi_data_path', value=output_path)
    
    return output_path