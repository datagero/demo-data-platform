# tests/pipelines/loaders/test_excel_loader.py

import os
import pandas as pd
import pytest
from src.interfaces.loaders.excel_loader import ExcelLoader

@pytest.fixture()
def create_test_excel_file(tmp_path):
    """Fixture to create a temporary Excel file for testing."""
    # Create a temporary Excel file
    test_file_path = os.path.join(tmp_path, 'test_file.xlsx')
    data = {'Column1': [1, 2, 3, 4], 'Column2': ['A', 'B', 'C', 'D']}
    with pd.ExcelWriter(test_file_path, engine='openpyxl') as writer:
        pd.DataFrame(data).to_excel(writer, sheet_name='Sheet1', index=False)
    return test_file_path

def test_excel_loader(create_test_excel_file):
    """Test the ExcelLoader's load function."""
    loader = ExcelLoader()
    file_path = create_test_excel_file
    sheet_names = ['Sheet1']

    # Use the loader to load data from the test Excel file
    data = loader.load(file_path, sheet_names)

    # Verify that the data is loaded correctly
    assert 'Sheet1' in data
    assert data['Sheet1'].equals(pd.DataFrame({'Column1': [1, 2, 3, 4], 'Column2': ['A', 'B', 'C', 'D']}))
