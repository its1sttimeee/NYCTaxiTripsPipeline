import logging
import pandas as pd

def clean_taxi_data(**context):
    logger = logging.getLogger("airflow.task")
    ti = context['ti']
    
    # ดึง Path ของไฟล์ดิบจาก XCom
    raw_path = ti.xcom_pull(key="raw_path", task_ids="ingest_taxi_data")
    if not raw_path:
        raise ValueError("No raw_path found in XCom.")
        
    logger.info(f"Loading raw data from: {raw_path}")
    df = pd.read_csv(raw_path)
    initial_count = len(df)
    
    df.dropna(subset=['fare_amount', 'trip_distance', 'tpep_pickup_datetime'], inplace=True)
    df = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 500)]
    df = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 100)]
    
    coord_cols = ['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']
    if all(col in df.columns for col in coord_cols):
        lat_cond = (df['pickup_latitude'].between(40.4, 41.0)) & (df['dropoff_latitude'].between(40.4, 41.0))
        lon_cond = (df['pickup_longitude'].between(-74.3, -73.5)) & (df['dropoff_longitude'].between(-74.3, -73.5))
        df = df[lat_cond & lon_cond]
        
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    
    clean_path = "/opt/airflow/dags/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)
    
    logger.info(f"Cleaning complete. Reduced from {initial_count} to {len(df)} rows. Saved to {clean_path}")
    
    return clean_path