import os
import json
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from src.schemas.schema_builder.source_schema_variations import DEFAULT_SCHEMAS_GPR, DEFAULT_SCHEMAS_PAVEMENT

def flatten_schemas(schemas):
    """
    Flattens the nested schema dictionary into a single dictionary where each entry
    contains the base columns plus any variation-specific columns.

    Args:
    schemas (dict): Nested dictionary of schemas and their variations.

    Returns:
    dict: Flattened dictionary with combined base and variation columns.
    """
    flattened_schemas = {}

    # Iterate over each schema group
    for group_name, group_data in schemas.items():
        # Extract the base schema columns
        base_schema_key = [key for key in group_data.keys() if key.startswith("Base")][0]
        base_columns = group_data[base_schema_key]['columns']

        # Add the base schema to the flattened dictionary
        flattened_schemas[base_schema_key] = list(base_columns)

        # Iterate over each variation in the group
        for variation_name, variation_data in group_data.items():
            if variation_name == base_schema_key:
                continue  # Skip the base schema as it is already added

            # Combine the base columns with variation-specific columns
            combined_columns = list(base_columns) + list(variation_data['columns'])
            flattened_schemas[variation_name] = combined_columns

    return flattened_schemas

def get_column_types_from_example(file_path, sheet_name):
    """
    Reads an example Excel file and returns a dictionary of column names and their inferred data types.
    
    Args:
    - file_path (str): Path to the Excel file.
    - sheet_name (str): Name of the sheet to read.
    
    Returns:
    - dict: A dictionary with column names as keys and Pandera-compatible data types as values.
    """
    # Load the data from the specified Excel file and sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Infer data types for each column and map to Pandera-compatible types
    column_types = {}
    for column_name, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            column_types[column_name] = pa.Int
        elif pd.api.types.is_float_dtype(dtype):
            column_types[column_name] = pa.Float
        elif pd.api.types.is_string_dtype(dtype):
            column_types[column_name] = pa.String
        elif pd.api.types.is_bool_dtype(dtype):
            column_types[column_name] = pa.Bool
        else:
            column_types[column_name] = pa.Object

    return column_types


def generate_pandera_schema(flattened_schemas, example_file_map):
    """
    Generate Pandera schema objects based on flattened schemas and example files.

    Args:
    - flattened_schemas (dict): Flattened schema dictionary.
    - example_file_map (dict): A mapping of schema names to their example files and sheets.

    Returns:
    - dict: A dictionary with schema names as keys and Pandera DataFrameSchema objects as values.
    """
    schema_objects = {}

    for schema_name, columns in flattened_schemas.items():
        example_info = example_file_map.get(schema_name)
        
        if example_info:
            file_path, sheet_name = example_info[0], example_info[1]
            column_types = get_column_types_from_example(file_path, sheet_name)

            # Create Pandera schema object with validated columns
            pandera_schema = pa.DataFrameSchema({
                col: Column(column_types.get(col, pa.Object)) for col in columns
            })

            schema_objects[schema_name] = pandera_schema

    return schema_objects

def map_example_files_to_schemas(categorized_files_path):
    """
    Maps example files to their respective schemas using the categorized files JSON.

    Args:
    - categorized_files_path (str): Path to the categorized files JSON.

    Returns:
    - dict: A dictionary mapping schema names to example file paths and sheet names.
    """
    with open(categorized_files_path, 'r') as f:
        categorized_files = json.load(f)

    example_file_map = {}

    # Extract example files from the 'exact_match_groups'
    for schema_name, file_info_list in categorized_files.get('exact_match_groups', {}).items():
        if file_info_list:
            # Use the first example file and sheet for each schema
            example_file_map[schema_name] = (file_info_list[0][0], file_info_list[0][1])

    return example_file_map

def save_schemas_to_yaml(schema_objects, output_folder):
    """
    Saves each Pandera schema as a YAML file in the specified output folder using Pandera's to_yaml() method.
    """
    os.makedirs(output_folder, exist_ok=True)  # Create the output folder if it doesn't exist

    for schema_name, pandera_schema in schema_objects.items():
        # Save schema directly to a YAML file using Pandera's to_yaml() method
        yaml_file_path = os.path.join(output_folder, f"{schema_name.replace(' ', '_')}.yaml")
        pandera_schema.to_yaml(yaml_file_path)
        print(f"Saved schema '{schema_name}' to '{yaml_file_path}'.")

if __name__ == "__main__":
    flattened_schemas_gpr = flatten_schemas(DEFAULT_SCHEMAS_GPR)
    flattened_schemas_pavement = flatten_schemas(DEFAULT_SCHEMAS_PAVEMENT)

    # Map example files to schemas
    example_file_map_gpr = map_example_files_to_schemas('src/data_profiling/categorized_files/categorized_files_gpr.json')

    # Generate Pandera schemas
    pandera_schemas_gpr = generate_pandera_schema(flattened_schemas_gpr, example_file_map_gpr)
    
    # Save Pandera schemas to files or use them directly in your pipeline
    save_schemas_to_yaml(pandera_schemas_gpr, 'src/schemas/source/gpr/')
