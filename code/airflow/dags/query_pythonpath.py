from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 9, 1),
}

with DAG('check_pythonpath', default_args=default_args, schedule_interval='@once') as dag:
    check_pythonpath = BashOperator(
        task_id='print_pythonpath',
        bash_command='echo $PYTHONPATH'
    )
