# tests/pipelines/writers/test_duckdb_writer.py

import os
import pandas as pd
import pytest
import duckdb
from src.interfaces.writers.duckdb_writer import DuckdbWriter

@pytest.fixture()
def create_temp_duckdb_db(tmp_path):
    """Fixture to create a temporary DuckDB database for testing."""
    db_path = os.path.join(tmp_path, 'test_db.duckdb')
    conn = duckdb.connect(db_path)
    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS test_schema;")
    yield db_path
    # Close the connection
    conn.close()

def test_duckdb_writer(create_temp_duckdb_db):
    """Test the DuckdbWriter's write function."""
    writer = DuckdbWriter()
    db_path = create_temp_duckdb_db
    schema = 'test_schema'
    table_name = 'test_table'
    partition_columns = ['Column1']

    # Create a test DataFrame
    test_data = pd.DataFrame({'Column1': [1, 2], 'Column2': ['A', 'B']})

    # Write the data using DuckdbWriter
    writer.write(test_data, db_path, schema, table_name, partition_columns)

    # Connect to DuckDB to verify the data
    conn = duckdb.connect(db_path)

    # Ensure schema and table exist
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    result = conn.execute(f"SELECT * FROM {schema}.{table_name}").fetchdf()

    # Validate the contents
    assert result.equals(test_data)

    # Close the connection
    conn.close()
