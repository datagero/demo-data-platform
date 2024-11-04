import os
import json
import yaml
import argparse
import duckdb
import importlib
import pandas as pd
from src.logging_config import logger
from collections import defaultdict
from src.interfaces.schema_manager import SchemaManager

def load_config(config_path):
    logger.info(f"Loading pipeline configuration from {config_path}")
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    logger.info("Configuration loaded successfully")
    return config

def dynamic_import(module_name, class_name):
    """
    Dynamically import a module and class by name.
    """
    logger.debug(f"Importing {class_name} from {module_name}")
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

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

def load_csv_file(file_path):
    try:
        logger.info(f"Loading CSV file from {file_path}")
        data = pd.read_csv(file_path)
        logger.info(f"CSV file {file_path} loaded successfully")
        return data
    except Exception as e:
        logger.error(f"Error loading CSV file {file_path}: {e}")
        raise

def normalize_data(file_path, sheet_name, sheet_data, schema_manager):
    schema_columns_and_types = {x: item['dtype'] for x, item in schema_manager.schema['columns'].items()}
    normalized_df = pd.DataFrame(columns=schema_columns_and_types.keys())
    logger.info(f"Starting normalization for file {file_path}, sheet {sheet_name}")

    for column, dtype in schema_columns_and_types.items():
        if column in sheet_data.columns:
            normalized_df[column] = sheet_data[column]
        else:
            logger.warning(f"Column '{column}' not found in {sheet_name}; defaulting to None")
            normalized_df[column] = None

    extra_columns = set(sheet_data.columns) - set(schema_columns_and_types.keys())
    if extra_columns:
        for column in extra_columns:
            logger.warning(f"Column '{column}' in sheet '{sheet_name}' is not in the schema and will be ignored.")

    schema_manager.convert_null_equivalents(normalized_df)

    for column, dtype in schema_columns_and_types.items():
        try:
            if dtype == 'datetime64[ns]':  # Handle datetime separately
                normalized_df[column] = pd.to_datetime(normalized_df[column], errors='coerce')
            elif dtype.startswith('float') or dtype.startswith('int'):  # Handle numeric types
                if dtype.startswith('int'):
                    normalized_df[column] = pd.to_numeric(normalized_df[column], errors='coerce')
                    normalized_df[column] = normalized_df[column].astype('Int64')
                else:
                    normalized_df[column] = pd.to_numeric(normalized_df[column], errors='coerce')
            else:
                normalized_df[column] = normalized_df[column].astype(dtype, errors='ignore')
        except KeyError:
            logger.error(f"Column '{column}' not found in the DataFrame; skipping type casting.")

    normalized_df['source_filepath'] = file_path
    normalized_df['source_sheetname'] = sheet_name
    normalized_df['created_time'] = pd.Timestamp.now()
    logger.info("Normalization complete")
    return normalized_df

def store_normalized_data_filesystem(normalized_data, output_path):
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        normalized_data.to_csv(output_path, index=False)
        logger.info(f"Normalized data saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save normalized data to {output_path}: {e}")
        raise

def store_normalized_data(normalized_data, db_path, schema, table_name, partition_columns):
    """
    Perform an upsert operation to store the normalized data in a DuckDB schema and table,
    partitioning by dynamic columns.

    Args:
    - normalized_data (pd.DataFrame): Normalized data.
    - db_path (str): Path to the DuckDB database file.
    - schema (str): The schema name (e.g., 'bronze', 'silver', 'gold').
    - table_name (str): The table name to write data to.
    - partition_columns (list): List of partition columns for managing data.
    """

    # Connect to DuckDB
    conn = duckdb.connect(db_path)

    # Add partition columns to the normalized data
    for col in partition_columns:
        if col not in normalized_data.columns:
            normalized_data[col] = ""

    # Create table if it doesn't exist
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS 
        SELECT * FROM normalized_data WHERE 1=0
    """)

    # Prepare the WHERE clause dynamically
    where_clause = ' AND '.join([f"{col} = '{normalized_data[col][0]}'" for col in partition_columns])

    # Perform the upsert operation
    # Step 1: Delete existing records for the same partition columns
    conn.execute(f"""
        DELETE FROM {schema}.{table_name} 
        WHERE {where_clause}
    """)

    # Step 2: Insert new data
    conn.execute(f"""
        INSERT INTO {schema}.{table_name} 
        SELECT * FROM normalized_data
    """)

    print(f"Upserted data into DuckDB table '{schema}.{table_name}' with partitions on {partition_columns}.")

    # Close the connection
    conn.close()

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

def ingest_pipeline(config_path, overwrite=False):
    """
    Ingestion pipeline to load CSV files, normalize data into a common schema, and store results.
    """
    logger.info("Starting ingestion pipeline")
    config = load_config(config_path)

    loader_dict = {}
    writer_dict = {}

    # Identify unique loaders
    for source_file in config['source_files']:
        loader_type = source_file['file_type']
        if loader_type not in loader_dict:
            loader_module = f"src.interfaces.loaders.{loader_type}_loader"
            loader_class = f"{loader_type.capitalize()}Loader"
            loader_dict[loader_type] = dynamic_import(loader_module, loader_class)()
            logger.info(f"Loader {loader_class} initialized for {loader_type}")

    # Identify writer
    writer_type = config['target']['type']
    if writer_type not in writer_dict:
        writer_module = f"src.interfaces.writers.{writer_type}_writer"
        writer_class = f"{writer_type.capitalize()}Writer"
        writer_dict[writer_type] = dynamic_import(writer_module, writer_class)()
        logger.info(f"Writer {writer_class} initialized for {writer_type}")

    schema_manager = SchemaManager(config['target']['schema']['path'])
    logger.debug(f"SchemaManager initialized with schema path: {config['target']['schema']['path']}")

    # Handle overwrite option for databases
    if overwrite and writer_type == 'duckdb':
        writer = writer_dict[writer_type]
        db_path = config['target']['writer_config']['destination']
        namespace = config['target']['writer_config'].get('namespace', 'public')
        tables_to_delete = [config['target']['writer_config']['table_name']]
        writer.delete_tables(db_path, namespace, tables_to_delete)
        logger.info(f"Existing tables deleted in {db_path} for overwrite")

    for source_file in config['source_files']:
        file_path = source_file['path']
        loader_type = source_file['file_type']
        loader = loader_dict[loader_type]

        logger.info(f"Processing file: {file_path} with loader {loader_type}")
        data = loader.load(file_path)

        # Normalize and process each sheet in the data
        for sheet_name, sheet_data in data.items():
            logger.info(f"Processing sheet: {sheet_name}")
            normalized_data = normalize_data(file_path, sheet_name, sheet_data, schema_manager)

            # Validate normalized data
            validated_data = schema_manager.validate_data(normalized_data)
            if validated_data is not None:
                writer = writer_dict[writer_type]
                output_path = config['target']['writer_config']['destination']

                if writer_type == 'duckdb':
                    db_path = config['target']['writer_config']['destination']
                    namespace = config['target']['writer_config']['namespace']
                    table_name = config['target']['writer_config']['table_name']
                    partition_columns = config['target']['writer_config'].get('partition_by', [])
                    writer.write(validated_data, db_path, namespace, table_name, partition_columns)
                    logger.info(f"Data written to DuckDB table: {table_name} in {db_path}")
                else:
                    fulloutput_path = f"{output_path}/{source_file['file_name']}.csv"
                    writer.write(validated_data, fulloutput_path)
                    logger.info(f"Data written to CSV: {fulloutput_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data using YAML configuration")
    parser.add_argument('--config_path', type=str, required=True, help="Path to the YAML configuration file")
    parser.add_argument('--overwrite', action='store_true', help="Overwrite existing data if set (default is False)")
    args = parser.parse_args()
    
    ingest_pipeline(args.config_path, args.overwrite)
    # ingest_pipeline('project_files/configs/source/tsd/Variation_1A_pipeline.yaml', True)
