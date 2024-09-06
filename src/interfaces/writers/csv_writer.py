# src/pipelines/writers/csv_writer.py
import pandas as pd
from src.interfaces.writers.base_writer import BaseWriter
import os

class CsvWriter(BaseWriter):
    """
    CSV file writer.
    """

    def write(self, dataframe: pd.DataFrame, file_path: str) -> None:
        """
        Write data to a CSV file.

        Args:
        - dataframe (pd.DataFrame): The data to write.
        - file_path (str): The path where data should be written.
        """
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        dataframe.to_csv(file_path, index=False)
        print(f"Normalized data saved to {file_path}")
