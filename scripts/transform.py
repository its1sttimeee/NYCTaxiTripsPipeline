import pandas as pd
import logging

def transform_taxi_data(**context):
    logger = logging.getLogger("airflow.task")
    ti = context['ti']
    
    # Pull the cleaned file path from the clean_taxi_data task
    # Note: Airflow automatically pushes the `return` value of a task to XCom
    clean_path = ti.xcom_pull(task_ids="clean_taxi_data") 
    if not clean_path:
        raise ValueError("No clean_path found in XCom.")
        
    logger.info(f"Loading clean data from: {clean_path}")
    df = pd.read_csv(clean_path)
    
    # Ensure datetime is the correct format
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    
    # Transformation: Create daily aggregations
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.date
    
    daily_summary = df.groupby('pickup_date').agg(
        total_trips=('tpep_pickup_datetime', 'count'),
        total_revenue=('fare_amount', 'sum'),
        avg_distance=('trip_distance', 'mean')
    ).reset_index()
    
    transform_path = "/opt/airflow/dags/nyc_taxi_daily_summary.csv"
    daily_summary.to_csv(transform_path, index=False)
    
    logger.info(f"Transformation complete. Aggregated data saved to {transform_path}")
    
    # Push the transformed path into XCom for the Load task
    ti.xcom_push(key="transform_path", value=transform_path)
    
    return transform_path