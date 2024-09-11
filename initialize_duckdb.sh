#!/bin/bash

# Define the path to your DuckDB database file
DUCKDB_PATH="./dbt_transforms/dev.duckdb"  # Use a correct path with the appropriate .duckdb extension

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
  echo "DuckDB is not installed. Please install DuckDB before running this script."
  exit 1
fi

# Initialize DuckDB and create the schemas
echo "Initializing DuckDB database and creating schemas..."

# Delete Schemas
duckdb "$DUCKDB_PATH" <<EOF
DROP SCHEMA IF EXISTS main_bronze CASCADE;
DROP SCHEMA IF EXISTS main_silver CASCADE;
DROP SCHEMA IF EXISTS main_gold CASCADE;
EOF

# Execute DuckDB commands
duckdb "$DUCKDB_PATH" <<EOF
CREATE SCHEMA IF NOT EXISTS main_bronze;
CREATE SCHEMA IF NOT EXISTS main_silver;
CREATE SCHEMA IF NOT EXISTS main_gold;
EOF

echo "DuckDB database initialized and schemas created successfully at $DUCKDB_PATH."
