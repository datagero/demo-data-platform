from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.airflow.dags.utils import run_subprocess_with_logging

def run_ingestion_csv_variation_1A():
    config_path = 'project_files/configs/source/tsd/Variation_1A_pipeline.yaml'
    command = [
        'python', 'src/pipelines/source/ingestion_csv.py',
        '--config_path', config_path,
        '--overwrite'
    ]
    run_subprocess_with_logging(command)  # Use centralized logging
    print(f"Completed CSV ingestion process for Variation 1A with config: {config_path}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='ingest_tsd_variation_1A_csv',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_csv_ingestion_variation_1A = PythonOperator(
        task_id='ingest_csv_variation_1A',
        python_callable=run_ingestion_csv_variation_1A
    )
