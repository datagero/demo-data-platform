import argparse
import importlib
import yaml
import pandas as pd
from pathlib import Path
from src.logging_config import logger
from src.interfaces.schema_manager import SchemaManager

def load_config(config_path):
    """
    Load pipeline configuration from a YAML file.
    """
    logger.info(f"Loading configuration from {config_path}")
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

def normalize_data(file_path, sheet_name, sheet_data, schema_columns):
    """
    Normalize data for a specific sheet based on schema.
    """
    logger.info(f"Normalizing data for sheet: {sheet_name} in file: {file_path}")
    normalized_df = pd.DataFrame(columns=schema_columns)

    # Only keep columns that are in the schema and add missing columns as None
    for col in schema_columns:
        if col in sheet_data.columns:
            normalized_df[col] = sheet_data[col]
        else:
            logger.warning(f"Column {col} not found in sheet {sheet_name}. Adding as None.")
            normalized_df[col] = None

    # Add metadata columns for tracking the file and sheet names
    normalized_df['source_filepath'] = file_path
    normalized_df['source_sheetname'] = sheet_name
    normalized_df['created_time'] = pd.Timestamp.now()

    logger.info(f"Normalization complete for sheet: {sheet_name}")
    return normalized_df

def ingestion_pipeline(config_path, overwrite=False):
    """
    Ingestion pipeline to convert Excel files to CSV, normalizing them into a common schema.
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
        namespace = config['target'].get('namespace', 'public')
        tables_to_delete = [config['target']['table_name']]
        writer.delete_tables(db_path, namespace, tables_to_delete)
        logger.info(f"Existing tables deleted in {db_path} for overwrite")

    for source_file in config['source_files']:
        file_path = source_file['path']
        sheet_names = source_file['loader_config']['tab_names']
        loader_type = source_file['file_type']
        loader = loader_dict[loader_type]

        logger.info(f"Processing file: {file_path} with loader {loader_type}")
        data = loader.load(file_path, sheet_names=sheet_names)

        for sheet_name, sheet_data in data.items():
            logger.info(f"Processing sheet: {sheet_name}")
            normalized_data = normalize_data(file_path, sheet_name, sheet_data, schema_manager.schema['columns'].keys())

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Excel files to CSV, normalizing data as per schema")
    parser.add_argument('--config', type=str, required=True, help="Path to the YAML configuration file")
    parser.add_argument('--overwrite', action='store_true', help="Overwrite existing data if set (default is False)")
    args = parser.parse_args()

    ingestion_pipeline(args.config, args.overwrite)
