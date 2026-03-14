import logging
import time
import requests
import pandas as pd
from requests.exceptions import RequestException

def ingest_taxi_data(**context):
    url = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    output_path = "/opt/airflow/dags/nyc_taxi_raw.csv"
    max_records = 1000
    max_retries = 3
    logger = logging.getLogger("airflow.task")

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
    if len(df) < max_records:
        raise ValueError(f"Validation failed: File has {len(df)} rows.")
        
    logger.info(f"Successfully downloaded {len(df)} records to {output_path}")
    
    # ส่ง Path เข้า XCom
    ti = context['ti']
    ti.xcom_push(key="raw_path", value=output_path)
    
    return output_path