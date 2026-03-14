import sqlite3
import pandas as pd
import logging

def load_taxi_data(**context):
    logger = logging.getLogger("airflow.task")
    ti = context['ti']
    
    # Pull the transformed file path
    transform_path = ti.xcom_pull(key="transform_path", task_ids="transform_taxi_data")
    if not transform_path:
        raise ValueError("No transform_path found in XCom.")
        
    logger.info(f"Loading transformed data from: {transform_path}")
    df = pd.read_csv(transform_path)
    
    # Load into an SQLite database (acting as our data warehouse)
    db_path = "/opt/airflow/dags/nyc_taxi_warehouse.db"
    conn = sqlite3.connect(db_path)
    
    try:
        # Write the data to a table, replacing it if it already exists
        # In a production environment, you might use if_exists="append"
        df.to_sql("daily_taxi_metrics", conn, if_exists="replace", index=False)
        logger.info(f"Successfully loaded {len(df)} aggregated rows into SQLite DB at {db_path}")
    except Exception as e:
        logger.error(f"Failed to load data into database: {e}")
        raise
    finally:
        conn.close()