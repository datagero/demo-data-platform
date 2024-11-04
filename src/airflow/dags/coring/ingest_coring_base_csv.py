from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import sys
from src.airflow.dags.utils import run_subprocess_with_logging

def run_csv_ingestion_process():
    config_path = 'project_files/configs/source/coring/Base_pipeline.yaml'
    command = [
        'python', 'src/pipelines/source/ingestion_csv.py',
        '--config_path', config_path,
        '--overwrite'
    ]
    run_subprocess_with_logging(command)  # Use the helper function

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='ingest_coring_base_csv',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_csv_ingestion_coring_base = PythonOperator(
        task_id='ingest_coring_csv_coring_base',
        python_callable=run_csv_ingestion_process
    )
