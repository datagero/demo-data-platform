from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.airflow.dags.utils import run_subprocess_with_logging

def run_preprocess_json_to_csv():
    file_json = 'project_files/datalake/source/Pavement Evaluation Summaries and Data/rp-23-04_data_evaluation_may1_I-59.json'
    output_csv = 'project_files/datalake/source/Pavement Evaluation Summaries and Data/rp-23-04_data_evaluation_may1_I-59.csv'
    
    command = [
        'python', 'src/pipelines/source/preprocess_json_to_csv.py',
        '--input_json', file_json,
        '--output_csv', output_csv
    ]
    run_subprocess_with_logging(command)  # Use centralized logging
    print(f"Completed JSON to CSV conversion for coring (validated) data from {file_json} to {output_csv}.")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='preprocess_coring_validated_json_to_csv',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust schedule as needed
    catchup=False
) as dag:

    task_convert_json_to_csv = PythonOperator(
        task_id='convert_json_to_csv',
        python_callable=run_preprocess_json_to_csv
    )
