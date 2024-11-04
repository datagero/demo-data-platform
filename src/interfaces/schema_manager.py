import os
import numpy as np
import pandas as pd
import pandera as pa
from pandera import DataFrameSchema
import yaml 

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
            self.pandera_schema = DataFrameSchema.from_yaml(self.schema_path)
            with open(self.schema_path, 'r') as file:
                self.schema = yaml.safe_load(file)
            print(f"Schema '{self.schema_name}' loaded successfully from {self.schema_path}.")
        except FileNotFoundError:
            print(f"Schema file not found: {self.schema_path}.")
        except Exception as e:
            print(f"An error occurred while reading the schema: {e}")

    def convert_null_equivalents(self, dataframe: pd.DataFrame):
        """
        Converts null-equivalent values to NaN in the given DataFrame based on schema metadata.

        Args:
        - dataframe (pd.DataFrame): The DataFrame to process.

        Returns:
        - pd.DataFrame: The processed DataFrame with null-equivalent values converted to NaN.
        """
        if not self.schema:
            print("No schema initialized for null-equivalent conversion.")
            return dataframe
        
        # Iterate over each column defined in the schema
        for column_name, column in self.schema['columns'].items():
            # Retrieve null-equivalent metadata if available
            if column_name == 'SCI_8':
                1==1
            if 'metadata' in column:
                null_equivalents = column['metadata'].get("null_equivalents", [])
                if null_equivalents:
                    # Determine the appropriate null value based on the column's dtype
                    target_dtype = column.get('dtype', 'object')
                    
                    # Handle datetime columns with NaT
                    if target_dtype == 'datetime64[ns]':
                        null_value = pd.NaT
                    # Handle numeric columns with np.nan
                    elif target_dtype.startswith('float') or target_dtype.startswith('int'):
                        null_value = np.nan
                    # Default to pd.NA for all other types
                    else:
                        null_value = pd.NA
                    
                    # Replace null-equivalent values with the appropriate null representation
                    dataframe[column_name] = dataframe[column_name].replace(null_equivalents, null_value)

        print(f"Null-equivalent values converted to NaN for schema '{self.schema_name}'.")
        return dataframe

    def validate_data(self, dataframe: pd.DataFrame):
        """
        Validates the given DataFrame against the initialized schema.

        Args:
        - dataframe (pd.DataFrame): The DataFrame to validate.

        Returns:
        - pd.DataFrame: The validated DataFrame if successful, or None if validation fails.
        """
        if not self.pandera_schema:
            print("No schema initialized for validation.")
            return None
        
        try:
            validated_df = self.pandera_schema.validate(dataframe)
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
    def translate_pandera_to_dbt(self, pandera_schema_path, dbt_model_name, dbt_source_schema_name, output_path=None):
        # Read the Pandera schema YAML
        with open(pandera_schema_path, 'r') as file:
            pandera_schema = yaml.safe_load(file)
        
        # Initialize the DBT schema structure
        dbt_schema = {
            'version': 2,
            'models': [
                {
                    'name': dbt_model_name,
                    'description': '',
                    'schema': dbt_source_schema_name,
                    'columns': []
                }
            ]
        }
        
        # Translate each column from Pandera to DBT schema
        for column_name, column_props in pandera_schema.get('columns', {}).items():
            dbt_column = {
                'name': column_name,
                'description': column_props.get('description', ''),
                'tests': []
            }
            
            # Handle nullable constraints
            if not column_props.get('nullable', True):
                dbt_column['tests'].append('not_null')

            # Add the column to the DBT schema
            dbt_schema['models'][0]['columns'].append(dbt_column)
        
        # Write the DBT schema to the specified output path
        if output_path:
            with open(output_path, 'w') as output_file:
                yaml.dump(dbt_schema, output_file, default_flow_style=False, sort_keys=False)

        return dbt_schema
    
# if __name__ == '__main__':
#     # Example usage
#     schema_path = 'project_files/schemas/silver/tsd.yaml'
#     schema_manager = SchemaManager(schema_path)
#     dbt_schema = schema_manager.translate_pandera_to_dbt(schema_path, 'silver_unified_tsd', 'main_bronze', output_path='project_files/dbt_transforms/models/silver/tsd/schema.yml')
#     pass