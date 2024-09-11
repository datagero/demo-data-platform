## Writers

Writers are responsible for storing processed and validated data in the desired format and location. Each writer is designed to handle a specific output type (e.g., CSV, DuckDB) and can be dynamically configured via the pipeline configuration file.

### Available Writers

1. **CSVWriter**
   - **Description**: Stores normalized data in CSV format.
   - **Configuration**:
     - **`type`**: `"csv"` — Specifies the writer type.
     - **`writer_config`**:
       - **`destination`**: Directory where the CSV files will be saved.
       - **`table_name_prefix`**: Prefix for the output filenames.
   - **Usage**:
     ```yaml
     target:
       type: "csv"
       writer_config:
         destination: "project_files/datalake/bronze/example"
         table_name_prefix: "normalized_"
     ```

2. **DuckDBWriter**
   - **Description**: Stores normalized data in a DuckDB database.
   - **Configuration**:
     - **`type`**: `"duckdb"` — Specifies the writer type.
     - **`writer_config`**:
       - **`destination`**: Path to the DuckDB database file.
       - **`namespace`**: Schema name within the DuckDB database.
       - **`table_name`**: The table name to which data will be written.
       - **`append_mode`**: Whether to append to existing data or overwrite.
       - **`partition_by`**: Columns to partition by.
   - **Usage**:
     ```yaml
     target:
       type: "duckdb"
       writer_config:
         destination: "dbt_transforms/dev.duckdb"
         namespace: "main_bronze"
         table_name: "example"
         append_mode: false
         partition_by:
           - source_filepath
           - source_sheetname
     ```

### Dynamic Writer Loading

Writers are dynamically loaded based on the `type` specified in the configuration file. The writer module and class names are specified in the configuration, allowing the pipeline to import and use them at runtime.

### How Writers Work

- The pipeline reads the configuration file to determine the writer type.
- It dynamically imports the appropriate writer class (e.g., `CSVWriter`, `DuckDBWriter`).
- The writer receives the processed and validated data and writes it to the specified target location and format.
