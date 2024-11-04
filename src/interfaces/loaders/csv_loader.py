# src/pipelines/loaders/csv_loader.py
import pandas as pd
from src.interfaces.loaders.base_loader import BaseLoader
from src.logging_config import logger

class CsvLoader(BaseLoader):
    """
    CSV file loader.
    """

    def load(self, file_path: str, sheet_names: list = None) -> dict:
        """
        Load data from a CSV file.

        Args:
        - file_path (str): The path to the CSV file.
        - sheet_names (list): Optional, not used in CSV files but kept for compatibility.

        Returns:
        - dict: Dictionary containing data with a single key.
        """
        data = {}
        try:
            logger.info(f"Loading CSV file from {file_path}")
            csv_data = pd.read_csv(file_path)
            data["sheet1"] = csv_data  # Using a single sheet name as CSVs do not have multiple sheets
            logger.info(f"CSV file {file_path} loaded successfully")
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {e}")
            raise

        return data
