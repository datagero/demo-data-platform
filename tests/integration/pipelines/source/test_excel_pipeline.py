# tests/test_pipeline_integration.py

import os
import pandas as pd
import tempfile
import pytest
import uuid  # To create a unique identifier for each test run
from src.pipelines.source.excel_ingestion_process import ingest_pipeline

@pytest.fixture()
def temp_dirs():
    """Fixture to create unique temporary directory structure for testing."""
    # Generate a unique identifier for this test run
    unique_id = uuid.uuid4()

    # Create the unique top-level temporary directory
    tmp_base_dir = os.path.join('tmp', f'test_run_{unique_id}')
    os.makedirs(tmp_base_dir, exist_ok=True)

    # Create subdirectories within the unique temporary directory
    datalake_dir = os.path.join(tmp_base_dir, 'datalake')
    source_dir = os.path.join(datalake_dir, 'source')
    bronze_dir = os.path.join(datalake_dir, 'bronze')
    configs_dir = os.path.join(tmp_base_dir, 'configs')
    schemas_dir = os.path.join(tmp_base_dir, 'schemas')
    os.makedirs(source_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(configs_dir, exist_ok=True)
    os.makedirs(schemas_dir, exist_ok=True)

    yield source_dir, bronze_dir, configs_dir, schemas_dir

    # Cleanup: Remove all created files and directories after the test run
    for sub_dir in [source_dir, bronze_dir, configs_dir, schemas_dir]:
        for file in os.listdir(sub_dir):
            os.remove(os.path.join(sub_dir, file))
        os.rmdir(sub_dir)
    os.rmdir(tmp_base_dir)  # Remove the top-level test directory

@pytest.fixture()
def create_test_files(temp_dirs):
    """Fixture to create test Excel files in the source subdirectory."""
    source_dir, _, _, _ = temp_dirs

    # Create initial Excel files for the test
    test_files = ['test_file_1.xlsx', 'test_file_2.xlsx']
    data1 = {'Column1': [1, 2, 3, 4], 'Column2': ['A', 'B', 'C', 'D']}
    data2 = {'Column1': [5, 6, 7, 8], 'Column2': ['E', 'F', 'G', 'H']}

    for file_name, data in zip(test_files, [data1, data2]):
        input_file_path = os.path.join(source_dir, file_name)
        with pd.ExcelWriter(input_file_path, engine='openpyxl') as writer:
            pd.DataFrame(data).to_excel(writer, sheet_name='Sheet1' if '1' in file_name else 'Sheet2', index=False)

    return source_dir

@pytest.fixture()
def create_test_schema(temp_dirs):
    """Fixture to create a schema file in the schemas subdirectory."""
    _, _, _, schemas_dir = temp_dirs
    schema_path = os.path.join(schemas_dir, 'test_excel_schema.yaml')

    schema_content = """
    schema_type: dataframe
    version: 0.20.4
    columns:
      Column1:
        dtype: int64
        nullable: false
      Column2:
        dtype: str
        nullable: true
      source_filepath:
        dtype: str
        nullable: false
      source_sheetname:
        dtype: str
        nullable: false
      created_time:
        dtype: datetime64[ns]
        nullable: false
    """
    # Write the schema content to a file
    with open(schema_path, 'w') as schema_file:
        schema_file.write(schema_content)

    return schema_path

def test_ingest_pipeline_csv(create_test_files, temp_dirs, create_test_schema):
    """Test the ingest_pipeline function with unique directories and inline schema."""
    source_dir, bronze_dir, configs_dir, schemas_dir = temp_dirs
    schema_path = create_test_schema

    # Create a unique configuration file dynamically
    unique_id = uuid.uuid4()
    config_path = os.path.join(configs_dir, f'test_config_{unique_id}.yaml')
    with open(config_path, 'w') as config_file:
        config_file.write(f"""
        name: "Test_Variation_1B_pipeline_{unique_id}"
        version: "1.0"
        
        metadata:
          domain: "Test"
          owner: "test_team"
          source_system: "test_internal"
          refresh_frequency: "adhoc"
          description: "Test pipeline for loading and profiling Excel data."

        source_files:
          - file_name: "test_file_1.xlsx"
            file_type: "excel"
            path: "{os.path.join(source_dir, 'test_file_1.xlsx')}"
            loader_config:
              tab_names:
                - "Sheet1"

          - file_name: "test_file_2.xlsx"
            file_type: "excel"
            path: "{os.path.join(source_dir, 'test_file_2.xlsx')}"
            loader_config:
              tab_names:
                - "Sheet2"

        target:
          type: "csv"
          writer_config:
            destination: "{bronze_dir}"
            table_name_prefix: "test_"
            append_mode: false
            partition_by: "date"
          schema:
            path: "{schema_path}"
        """)

    # Run the ingest pipeline
    ingest_pipeline(config_path)

    # Check if output files are generated correctly
    output_files = os.listdir(bronze_dir)
    assert len(output_files) > 0, "No output files generated!"

    # Print the output files for debugging
    print(f"Output files: {output_files}")

    # Validate the output content
    for output_file in output_files:
        output_file_path = os.path.join(bronze_dir, output_file)
        
        # Try different encodings and delimiters to read the CSV correctly
        try:
            df = pd.read_csv(output_file_path, encoding='ISO-8859-1', delimiter=',')  # Assuming default delimiter
        except pd.errors.ParserError:
            print(f"Failed to read {output_file_path} with default delimiter. Trying semicolon...")
            df = pd.read_csv(output_file_path, encoding='ISO-8859-1', delimiter=';')
        
        # Output the contents for further debugging
        print(f"Contents of {output_file_path}:\n", df.head())
        expected_columns = ['Column1', 'Column2', 'source_filepath', 'source_sheetname', 'created_time']
        assert all(col in df.columns for col in expected_columns), f"Expected columns are missing in {output_file}!"

    print("All tests passed!")

def test_ingest_pipeline_duckdb(create_test_files, temp_dirs, create_test_schema):
    import duckdb
    """Test the ingest_pipeline function with DuckDB writer."""
    source_dir, bronze_dir, configs_dir, schemas_dir = temp_dirs
    schema_path = create_test_schema

    # Create a unique configuration file dynamically
    unique_id = uuid.uuid4()
    config_path = os.path.join(configs_dir, f'test_config_duckdb_{unique_id}.yaml')
    db_path = os.path.join(bronze_dir, 'test_duckdb.db')
    
    # Create a DuckDB connection and schema
    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS main_bronze;")

    with open(config_path, 'w') as config_file:
        config_file.write(f"""
        name: "Test_Variation_1B_pipeline_DuckDB_{unique_id}"
        version: "1.0"
        
        metadata:
          domain: "Test"
          owner: "test_team"
          source_system: "test_internal"
          refresh_frequency: "adhoc"
          description: "Test pipeline for loading and profiling Excel data into DuckDB."

        source_files:
          - file_name: "test_file_1.xlsx"
            file_type: "excel"
            path: "{os.path.join(source_dir, 'test_file_1.xlsx')}"
            loader_config:
              tab_names:
                - "Sheet1"

          - file_name: "test_file_2.xlsx"
            file_type: "excel"
            path: "{os.path.join(source_dir, 'test_file_2.xlsx')}"
            loader_config:
              tab_names:
                - "Sheet2"

        target:
          type: "duckdb"
          writer_config:
            destination: "{db_path}"
            namespace: "main_bronze"
            table_name: "example"
            append_mode: false
            partition_by:
              - source_filepath
              - source_sheetname
          schema:
            path: "{schema_path}"
        """)

    # Run the ingest pipeline
    ingest_pipeline(config_path)

    # Connect to DuckDB and check if data is written correctly
    result_df = conn.execute("SELECT * FROM main_bronze.example").fetchdf()

    # Validate that data was written to DuckDB
    assert len(result_df) > 0, "No data was written to DuckDB!"

    # Print the output for debugging
    print(f"Data in DuckDB:\n{result_df.head()}")

    expected_columns = ['Column1', 'Column2', 'source_filepath', 'source_sheetname', 'created_time']
    assert all(col in result_df.columns for col in expected_columns), "Expected columns are missing in DuckDB table!"

    # Close the DuckDB connection
    conn.close()

    print("DuckDB writer test passed successfully!")

if __name__ == "__main__":
    test_ingest_pipeline_csv()
    test_ingest_pipeline_duckdb()
