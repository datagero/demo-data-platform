from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

# Define the directory to monitor for new Excel files
EXCEL_DIR = '/Users/datagero/Documents/offline_repos/vip-sci/datalake/source/Pavement Evaluation Summaries and Data/2022-04-11 I-285 MP27-34 Dekalb'
OUTPUT_DIR = '/Users/datagero/Documents/offline_repos/vip-sci/datalake/bronze/Pavement Evaluation Summaries and Data/2022-04-11 I-285 MP27-34 Dekalb'

# Create the output directory if it does not exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to process Excel files
def process_excel_file(file_name):
    file_path = os.path.join(EXCEL_DIR, file_name)
    print(f"Processing {file_path}...")

    # Load Excel file using pandas
    try:
        excel_data = pd.read_excel(file_path, sheet_name=None)  # Load all sheets
        for sheet_name, df in excel_data.items():
            output_file = os.path.join(OUTPUT_DIR, f"{os.path.splitext(file_name)[0]}_{sheet_name}.csv")
            df.to_csv(output_file, index=False)
            print(f"Saved {output_file}")

    except Exception as e:
        print(f"Failed to process {file_name}: {e}")

# Function to monitor the directory and process new files
def monitor_directory():
    processed_files = set(os.listdir(OUTPUT_DIR))
    all_files = os.listdir(EXCEL_DIR)

    for file_name in all_files:
        if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
            if file_name not in processed_files:
                process_excel_file(file_name)

# Define the Airflow DAG
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
    task_monitor_directory = PythonOperator(
        task_id='monitor_directory',
        python_callable=monitor_directory
    )

task_monitor_directory

if __name__ == "__main__":
    process_excel_file('I-285 EB Mainlines, Lanes 1-3_FINAL RESULTS.xlsx')
