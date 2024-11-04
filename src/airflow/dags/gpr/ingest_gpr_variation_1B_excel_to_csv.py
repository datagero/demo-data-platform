from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.airflow.dags.utils import run_subprocess_with_logging

def run_ingestion_excel_to_csv_variation_1B():
    config_path = 'project_files/configs/source/gpr_duckdb/Variation_1B_pipeline.yaml'
    
    command = [
        'python', 'src/pipelines/source/ingestion_excel_to_csv.py',
        '--config', config_path
    ]
    run_subprocess_with_logging(command)  # Use centralized logging
    print(f"Completed Excel to CSV conversion for Variation 1B with config: {config_path}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='ingest_gpr_variation_1B_excel_to_csv',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_ingestion_excel_to_csv_variation_1A = PythonOperator(
        task_id='ingestion_excel_to_csv_variation_1B',
        python_callable=run_ingestion_excel_to_csv_variation_1B
    )
