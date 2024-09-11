# src/pipelines/writers/base_writer.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseWriter(ABC):
    """
    Interface for all writers.
    """

    @abstractmethod
    def write(self, dataframe: pd.DataFrame, file_path: str) -> None:
        """
        Write data to the specified file path.

        Args:
        - dataframe (pd.DataFrame): The data to write.
        - file_path (str): The path where data should be written.
        """
        pass
