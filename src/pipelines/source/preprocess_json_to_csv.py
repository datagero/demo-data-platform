import os
import json
import argparse
import pandas as pd
from src.logging_config import logger
from pathlib import Path

# Read JSON
def load_json_file(file_path):
    logger.info(f"Loading JSON file from {file_path}")
    with open(file_path, 'r') as f:
        data = json.load(f)
    logger.info("JSON file loaded successfully")
    return data

def expand_json_column(df, column_name):
    """
    Expand a column containing nested JSON or lists into a flattened format.
    """
    logger.info(f"Expanding column '{column_name}' containing nested JSON or lists")
    # Explode the column if it contains a list
    if df[column_name].apply(lambda x: isinstance(x, list)).any():
        logger.debug(f"Column '{column_name}' contains lists, exploding rows")
        df = df.explode(column_name)

    # Normalize the column with nested JSON into a flattened DataFrame
    normalized = pd.json_normalize(df[column_name])

    # Combine the normalized data with the original DataFrame
    expanded_df = pd.concat([df.drop(column_name, axis=1).reset_index(drop=True), normalized], axis=1)

    logger.info(f"Column '{column_name}' expanded successfully")
    return expanded_df

def flatten_json_data(df):
    """
    Recursively flatten the DataFrame columns that contain nested JSON structures.
    """
    logger.info("Starting recursive JSON flattening process")
    # List of columns that are still JSON or lists
    json_columns = df.applymap(lambda x: isinstance(x, (list, dict))).any()

    # While there are columns to expand
    while json_columns.any():
        column_to_expand = json_columns.idxmax()  # Get the first column to expand
        logger.debug(f"Flattening column '{column_to_expand}'")
        df = expand_json_column(df, column_to_expand)  # Expand it
        json_columns = df.applymap(lambda x: isinstance(x, (list, dict))).any()  # Check again

    logger.info("JSON flattening process completed")
    return df

def main(input_json, output_csv):
    # Validate that input JSON exists
    input_path = Path(input_json)
    assert input_path.is_file(), f"Input JSON file not found: {input_json}"

    # Validate that output directory exists or create it
    output_dir = Path(output_csv).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load JSON and flatten
    try:
        df = pd.read_json(input_json)
        logger.info("DataFrame created from JSON file")
    except Exception as e:
        logger.error(f"Failed to load JSON into DataFrame: {e}")
        raise
    flattened_df = flatten_json_data(df)

    # Save to CSV
    try:
        flattened_df.to_csv(output_csv, index=False)
        logger.info(f"Flattened DataFrame saved to CSV: {output_csv}")
    except Exception as e:
        logger.error(f"Failed to save DataFrame to CSV: {e}")
        raise

    print(f"Converted JSON {input_json} to CSV {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON to CSV with flattening")
    parser.add_argument('--input_json', type=str, required=True, help="Path to the input JSON file")
    parser.add_argument('--output_csv', type=str, required=True, help="Path to the output CSV file")
    args = parser.parse_args()
    
    main(args.input_json, args.output_csv)

    # # Create schema definition
    # import pandera as pa
    # schema_variation_1b = pa.infer_schema(flattened_df)
    # for column in schema_variation_1b.columns.values():
    #     column.checks = []

    # schema_yaml = "project_files/schemas/source/coring/Base_inferred.yaml"
    # try:
    #     schema_variation_1b.to_yaml(schema_yaml)
    #     logger.info(f"Schema definition saved to YAML: {schema_yaml}")
    # except Exception as e:
    #     logger.error(f"Failed to save schema to YAML: {e}")