# src/pipelines/loaders/base_loader.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseLoader(ABC):
    """
    Interface for all loaders.
    """

    @abstractmethod
    def load(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Load data from the given file path.

        Args:
        - file_path (str): The path to the file to load.

        Returns:
        - pd.DataFrame: Loaded data as a DataFrame.
        """
        pass
