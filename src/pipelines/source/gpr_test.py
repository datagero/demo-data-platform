import os
import json
import pandas as pd
from collections import defaultdict
from src.interfaces.schema_manager import SchemaManager

def load_categorized_files(categorized_file_path):
    """
    Load the categorized files from a JSON file.

    Args:
    - categorized_file_path (str): Path to the categorized files JSON.

    Returns:
    - categories (dict): Dictionary containing categorized file information.
    """
    with open(categorized_file_path, 'r') as f:
        categories = json.load(f)
    return categories

def load_gpr_file(file_path, sheet_names):
    """
    Load a GPR file and read relevant sheets.

    Args:
    - file_path (str): File path of the GPR file.
    - sheet_names (list): List of relevant sheet names to be read.

    Returns:
    - data (dict): Dictionary containing data for each sheet.
    """
    data = {}
    try:
        # Read the specified sheets from the Excel file
        for sheet_name in sheet_names:
            sheet_data = pd.read_excel(file_path, sheet_name=sheet_name)
            data[sheet_name] = sheet_data
    except Exception as e:
        print(f"Error loading file {file_path} with sheets {sheet_names}: {e}")

    return data

def normalize_data(file_path, sheet_name, sheet_data, schema_columns):
    """
    Normalize data for a specific sheet based on schema.

    Args:
    - file_path (str): File path of the GPR file.
    - sheet_name (str): Sheet name being processed.
    - sheet_data (pd.DataFrame): DataFrame containing sheet data.
    - schema_columns (list): List of schema columns to normalize against.

    Returns:
    - normalized_data (pd.DataFrame): DataFrame containing normalized data.
    """
    # Initialize normalized DataFrame with None for missing values
    normalized_df = pd.DataFrame(columns=schema_columns)

    # Only keep columns that are in the schema and add missing columns as None
    for col in schema_columns:
        if col in sheet_data.columns:
            normalized_df[col] = sheet_data[col]
        else:
            print(f"Column {col} not found in sheet {sheet_name}. Adding as None.")
            normalized_df[col] = None

    # Add metadata columns for tracking the file and sheet names
    normalized_df['File'] = file_path
    normalized_df['Sheet'] = sheet_name

    return normalized_df

def store_normalized_data(normalized_data, output_path):
    """
    Store the normalized data in a structured format.

    Args:
    - normalized_data (pd.DataFrame): Normalized data.
    - output_path (str): Output file path.
    """
    normalized_data.to_csv(output_path, index=False)
    print(f"Normalized data saved to {output_path}")

def extract_sheet_names_for_schema(categorized_files, schema_name):
    """
    Extracts relevant sheet names for each file path based on a specific schema from the categorized files.

    Args:
    - categorized_files (dict): Dictionary containing categorized file information.
    - schema_name (str): Name of the schema to filter for.

    Returns:
    - file_sheets (dict): Dictionary where keys are file paths and values are lists of relevant sheet names.
    """
    file_sheets = defaultdict(list)

    # tmp for compatibility, need to replace spaces of keys with underscores
    categorized_files["exact_match_groups"] = {k.replace(" ", "_"): v for k, v in categorized_files["exact_match_groups"].items()}
    categorized_files["extended_match_groups"] = {k.replace(" ", "_"): v for k, v in categorized_files["extended_match_groups"].items()}

    # Extract from exact match groups
    for entry in categorized_files.get("exact_match_groups", {}).get(schema_name, []):
        file_path = entry[0]
        sheet_name = entry[1]
        file_sheets[file_path].append(sheet_name)

    # Extract from extended match groups
    for entry in categorized_files.get("extended_match_groups", {}).get(schema_name, []):
        file_path = entry[0]
        sheet_name = entry[1]
        file_sheets[file_path].append(sheet_name)

    return dict(file_sheets)

# Main pipeline function
def ingest_pipeline(schema_path, categorized_file_path, output_path):
    """
    Ingest pipeline to process GPR files and normalize them into a common schema.

    Args:
    - schema_path (str): Path to schema YAML file.
    - categorized_file_path (str): Path to categorized files JSON file.
    - output_path (str): Output directory path for normalized data.
    """
    # Initialize SchemaManager for the target schema
    schema_manager = SchemaManager(schema_path)

    # Load categorized files
    categorized_files = load_categorized_files(categorized_file_path)

    # Extract schema name from schema path (e.g., Variation_1A from the path)
    schema_name = os.path.basename(schema_path).replace('.yaml', '')

    # Process each file for the current schema
    file_dict = extract_sheet_names_for_schema(categorized_files, schema_name)
    
    for file_path, sheet_names in file_dict.items():
        # Load the data for the current file
        data = load_gpr_file(file_path, sheet_names)

        # Process each sheet separately
        for sheet_name, sheet_data in data.items():
            # Normalize data for the current sheet
            normalized_data = normalize_data(file_path, sheet_name, sheet_data, schema_manager.schema.columns.keys())

            # Validate normalized data
            validated_data = schema_manager.validate_data(normalized_data)
            if validated_data is not None:
                # Store normalized and validated data
                output_file = os.path.join(output_path, f"{schema_name}_{os.path.basename(file_path).replace('.xlsx', f'_{sheet_name}_normalized.csv')}")
                store_normalized_data(validated_data, output_file)

if __name__ == '__main__':
    # Example usage
    schema_path = 'project_files/schemas/source/gpr/schemas.json'
    categorized_file_path = 'project_files/data_profiling/schema_category/gpr.json'
    output_path = 'project_files/datalake/bronze/'
    ingest_pipeline(schema_path, categorized_file_path, output_path)
