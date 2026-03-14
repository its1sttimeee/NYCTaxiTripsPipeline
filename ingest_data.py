import logging
import time
import requests
import pandas as pd
import os
from requests.exceptions import RequestException

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
            logger.info(f"Attempt {attempt + 1}: Downloading data from {url}")
            
            with requests.get(url, stream=True, timeout=30) as response:
                response.raise_for_status()
                
                with open(output_path, 'wb') as f:
                    lines_written = 0
                    for line in response.iter_lines():
                        if line:
                            f.write(line + b'\n')
                            lines_written += 1
                            if lines_written > max_records:
                                break
            break
            
        except RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("Max retries reached. Failing task.")
                raise
            time.sleep(5)
            
    df = pd.read_csv(output_path)
    row_count = len(df)
    
    logger.info(f"Successfully downloaded and saved {row_count} records to {output_path}")
    
    if row_count < max_records:
        raise ValueError(f"Validation failed: File has {row_count} rows, expected at least {max_records}.")
        
    return output_path