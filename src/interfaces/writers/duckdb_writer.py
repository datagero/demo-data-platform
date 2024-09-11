# src/pipelines/writers/duckdb_writer.py

import duckdb

class DuckdbWriter:
    def __init__(self):
        pass

    def write(self, normalized_data, db_path, schema, table_name, partition_columns):
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

    def delete_tables(self, db_path, schema, tables):
        """
        Delete tables from DuckDB if they exist, to start with a clean slate for development.

        Args:
        - db_path (str): Path to the DuckDB database file.
        - schema (str): The schema name (e.g., 'bronze', 'silver', 'gold').
        - tables (list): List of table names to delete.
        """

        # Connect to DuckDB
        conn = duckdb.connect(db_path)

        for table in tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
                print(f"Table '{schema}.{table}' deleted successfully.")
            except Exception as e:
                print(f"Error deleting table '{schema}.{table}': {e}")

        conn.close()
