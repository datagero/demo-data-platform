import os
import pandas as pd
import pandera as pa
from pandera import DataFrameSchema

class SchemaManager:
    def __init__(self, schema_path: str):
        """
        Initializes the SchemaManager with a schema file path.

        Args:
        - schema_path (str): The full path to the schema YAML file.
        """
        self.schema_path = schema_path
        self.schema = None
        self.stage = None
        self.producer = None
        self.schema_name = None

        # Infer stage, producer, and schema name from the path
        self.infer_schema_details()

        # Initialize schema by reading from file
        self.read_schema()

    def infer_schema_details(self):
        """
        Infers the stage, producer, and schema name from the provided schema path.
        """
        try:
            # Split the path into components
            path_parts = self.schema_path.split(os.sep)

            # Assuming path follows the pattern: "src/stage/producer/schema.yaml"
            self.stage = path_parts[-3]  # e.g., "source"
            self.producer = path_parts[-2]  # e.g., "gpr"
            self.schema_name = os.path.splitext(path_parts[-1])[0]  # e.g., "Variation_1A"

            print(f"Inferred details - Stage: {self.stage}, Producer: {self.producer}, Schema Name: {self.schema_name}")
        except IndexError:
            print("Error: Unable to infer schema details from the provided path.")
        except Exception as e:
            print(f"An error occurred while inferring schema details: {e}")

    def read_schema(self):
        """
        Reads the schema YAML file from the specified path and initializes the Pandera schema.
        """
        try:
            self.schema = DataFrameSchema.from_yaml(self.schema_path)
            print(f"Schema '{self.schema_name}' loaded successfully from {self.schema_path}.")
        except FileNotFoundError:
            print(f"Schema file not found: {self.schema_path}.")
        except Exception as e:
            print(f"An error occurred while reading the schema: {e}")

    def validate_data(self, dataframe: pd.DataFrame):
        """
        Validates the given DataFrame against the initialized schema.

        Args:
        - dataframe (pd.DataFrame): The DataFrame to validate.

        Returns:
        - pd.DataFrame: The validated DataFrame if successful, or None if validation fails.
        """
        if not self.schema:
            print("No schema initialized for validation.")
            return None
        
        try:
            validated_df = self.schema.validate(dataframe)
            print(f"Data validated successfully against schema '{self.schema_name}'.")
            return validated_df
        except pa.errors.SchemaError as e:
            self.handle_validation_error(e)
            return None

    def handle_validation_error(self, error):
        """
        Handles validation errors and prints detailed information.

        Args:
        - error (pa.errors.SchemaError): The schema validation error to handle.
        """
        print(f"Validation error for schema '{self.schema_name}': {error}")
        # Additional error handling logic can be added here

    def is_schema_initialized(self):
        """
        Checks if a schema is initialized.

        Returns:
        - bool: True if the schema is initialized, False otherwise.
        """
        return self.schema is not None

    # Additional methods for schema management can be added here
