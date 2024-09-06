# src/pipelines/loaders/excel_loader.py
import pandas as pd
from src.interfaces.loaders.base_loader import BaseLoader

class ExcelLoader(BaseLoader):
    """
    Excel file loader.
    """

    def load(self, file_path: str, sheet_names: list) -> dict:
        """
        Load data from an Excel file.

        Args:
        - file_path (str): The path to the Excel file.
        - sheet_names (list): List of relevant sheet names to be read.

        Returns:
        - dict: Dictionary containing data for each sheet.
        """
        data = {}
        try:
            for sheet_name in sheet_names:
                sheet_data = pd.read_excel(file_path, sheet_name=sheet_name)
                data[sheet_name] = sheet_data
        except Exception as e:
            print(f"Error loading file {file_path} with sheets {sheet_names}: {e}")
        return data
