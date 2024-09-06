# excel_ingestion_process.py
import importlib
import yaml
import argparse
import pandas as pd
from src.interfaces.schema_manager import SchemaManager

def load_config(config_path):
    """
    Load pipeline configuration from a YAML file.
    """
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def dynamic_import(module_name, class_name):
    """
    Dynamically import a module and class by name.
    """
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

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
    normalized_df['source_filepath'] = file_path
    normalized_df['source_sheetname'] = sheet_name
    normalized_df['created_time'] = pd.Timestamp.now()

    return normalized_df

def ingest_pipeline(config_path, overwrite=False):
    """
    Ingest pipeline to process files and normalize them into a common schema.
    """
    # Load pipeline configuration
    config = load_config(config_path)

    # Determine loaders and writers from configuration
    loader_dict = {}
    writer_dict = {}

    # Identify unique loaders required
    for source_file in config['source_files']:
        loader_type = source_file['file_type']
        if loader_type not in loader_dict:
            # Define loader module and class names based on file type
            loader_module = f"src.interfaces.loaders.{loader_type}_loader"
            loader_class = f"{loader_type.capitalize()}Loader"
            loader_dict[loader_type] = dynamic_import(loader_module, loader_class)()

    # Identify writer required
    writer_type = config['target']['type']
    if writer_type not in writer_dict:
        writer_module = f"src.interfaces.writers.{writer_type}_writer"
        writer_class = f"{writer_type.capitalize()}Writer"
        writer_dict[writer_type] = dynamic_import(writer_module, writer_class)()

    # Initialize SchemaManager for the target schema
    schema_manager = SchemaManager(config['target']['schema']['path'])

    # If overwrite is true, delete existing tables
    if overwrite and writer_type == 'duckdb':
        writer = writer_dict[writer_type]
        db_path = config['target']['writer_config']['destination']
        namespace = config['target'].get('namespace', 'public')
        tables_to_delete = [config['target']['table_name']]
        writer.delete_tables(db_path, namespace, tables_to_delete)

    # Process each source file as specified in the config
    for source_file in config['source_files']:
        file_path = source_file['path']
        sheet_names = source_file['loader_config']['tab_names']
        loader_type = source_file['file_type']
        loader = loader_dict[loader_type]

        # Load the data for the current file
        data = loader.load(file_path, sheet_names=sheet_names)

        # Process each sheet separately
        for sheet_name, sheet_data in data.items():
            # Normalize data for the current sheet
            normalized_data = normalize_data(file_path, sheet_name, sheet_data, schema_manager.schema.columns.keys())

            # Validate normalized data
            validated_data = schema_manager.validate_data(normalized_data)
            if validated_data is not None:
                writer = writer_dict[writer_type]
                output_path = config['target']['writer_config']['destination']
                if writer_type == 'duckdb':
                    # Extract specific DuckDB writer parameters
                    db_path = config['target']['writer_config']['destination']
                    namespace = config['target']['writer_config']['namespace']
                    table_name = config['target']['writer_config']['table_name']
                    partition_columns = config['target']['writer_config'].get('partition_by', [])

                    # Call the write method for DuckDB writer
                    writer.write(validated_data, db_path, namespace, table_name, partition_columns)

                else:
                    # Handle other writer types, e.g., CSV
                    fulloutput_path = f"{output_path}/{source_file['file_name']}.csv"
                    writer.write(validated_data, fulloutput_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the Excel Ingestion Pipeline.')
    parser.add_argument('--config', type=str, required=True, help='Path to the YAML configuration file.')
    args = parser.parse_args()
    ingest_pipeline(args.config)
