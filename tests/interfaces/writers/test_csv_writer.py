# tests/pipelines/writers/test_csv_writer.py

import os
import pandas as pd
import pytest
from src.interfaces.writers.csv_writer import CsvWriter

@pytest.fixture()
def create_temp_dir(tmp_path):
    """Fixture to create a temporary directory for testing."""
    return tmp_path

def test_csv_writer(create_temp_dir):
    """Test the CsvWriter's write function."""
    writer = CsvWriter()
    test_data = pd.DataFrame({'Column1': [1, 2], 'Column2': ['A', 'B']})
    output_path = os.path.join(create_temp_dir, 'test_output.csv')

    # Use the writer to write data to a CSV file
    writer.write(test_data, output_path)

    # Verify that the file is written correctly
    assert os.path.exists(output_path)
    written_data = pd.read_csv(output_path)
    assert written_data.equals(test_data)
