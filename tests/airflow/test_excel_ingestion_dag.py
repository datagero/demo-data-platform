# tests/test_excel_ingestion.py

import os
import pandas as pd
import tempfile
import pytest
from src.airflow.dags.excel_ingestion_dag import process_excel_file

@pytest.fixture()
def temp_dirs():
    """Fixture to create temporary input and output directories for testing."""
    tmp_dir = 'tmp'
    os.makedirs(tmp_dir, exist_ok=True)
    input_dir = tempfile.mkdtemp(dir=tmp_dir, prefix='input_')
    output_dir = tempfile.mkdtemp(dir=tmp_dir, prefix='output_')
    yield input_dir, output_dir

    # Cleanup the directories after the test run
    for temp_dir in [input_dir, output_dir]:
        for file in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, file))
        os.rmdir(temp_dir)

def test_process_excel_file(temp_dirs):
    """Test the process_excel_file function."""
    input_dir, output_dir = temp_dirs

    # Create initial Excel file to simulate an already processed file
    already_processed_file = 'file1.xlsx'
    input_file_path = os.path.join(input_dir, already_processed_file)
    with pd.ExcelWriter(input_file_path, engine='openpyxl') as writer:
        pd.DataFrame({'A': [1, 2], 'B': [3, 4]}).to_excel(writer, sheet_name='Sheet1', index=False)

    # Process the initial file
    process_excel_file(input_dir=input_dir, output_dir=output_dir)

    # Ensure the initial file is processed
    output_files = os.listdir(output_dir)
    assert f"{os.path.splitext(already_processed_file)[0]}_Sheet1.csv" in output_files

    # Create new Excel files to simulate unprocessed files
    new_files = ['file2.xlsx', 'file3.xlsx']
    for file_name in new_files:
        input_file_path = os.path.join(input_dir, file_name)
        with pd.ExcelWriter(input_file_path, engine='openpyxl') as writer:
            pd.DataFrame({'C': [5, 6], 'D': [7, 8]}).to_excel(writer, sheet_name='Sheet2', index=False)

    # Call the function again to process new files only
    process_excel_file(input_dir=input_dir, output_dir=output_dir)