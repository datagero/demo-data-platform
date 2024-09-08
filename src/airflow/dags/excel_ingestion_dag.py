from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# Function to process Excel files
def process_excel_file(input_dir, output_dir):
    import pandas as pd

    # Create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # List of processed files
    processed_files = set(os.listdir(output_dir))
    all_files = os.listdir(input_dir)

    for file_name in all_files:
        # Process only Excel files
        if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
            # Check if the file has already been processed
            base_name = os.path.splitext(file_name)[0]
            if base_name not in processed_files:
                file_path = os.path.join(input_dir, file_name)
                print(f"Processing {file_path}...")

                try:
                    # Load Excel file using pandas
                    excel_data = pd.read_excel(file_path, sheet_name=None)  # Load all sheets
                    for sheet_name, df in excel_data.items():
                        output_file = os.path.join(output_dir, f"{base_name}_{sheet_name}.csv")
                        df.to_csv(output_file, index=False)
                        print(f"Saved {output_file}")

                except Exception as e:
                    print(f"Failed to process {file_name}: {e}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='excel_ingestion_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Run once daily; adjust as needed
    catchup=False
) as dag:

    # PythonOperator to process Excel files
    task_process_excel_file = PythonOperator(
        task_id='process_excel_file',
        python_callable=process_excel_file,
        op_kwargs={
            'input_dir': '{{ dag_run.conf.get("input_dir", "datalake/source/test") }}',
            'output_dir': '{{ dag_run.conf.get("output_dir", "datalake/bronze/test") }}'
        }
    )

    task_process_excel_file
