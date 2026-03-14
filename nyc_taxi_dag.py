from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# อิมพอร์ตฟังก์ชันจากโฟลเดอร์ scripts ที่เราสร้างไว้
from scripts.ingest import ingest_taxi_data
from scripts.clean import clean_taxi_data
from scripts.transform import transform_taxi_data
from scripts.load import load_taxi_model

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='nyc_taxi_modular_pipeline',
    default_args=default_args,
    description='Modularized Ingest and Clean NYC Taxi data',
    # เปลี่ยน schedule_interval เป็น schedule ตรงนี้ครับ 👇
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['taxi', 'modular'],
) as dag:

    # เรียกใช้ฟังก์ชันผ่าน python_callable
    task_ingest = PythonOperator(
        task_id='ingest_taxi_data',
        python_callable=ingest_taxi_data,
    )

    task_clean = PythonOperator(
        task_id='clean_taxi_data',
        python_callable=clean_taxi_data,
    )

    # กำหนด Pipeline Flow
    task_ingest >> task_clean >> transform_task >> load_task