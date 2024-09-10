## README for the Excel Ingestion Pipeline

This README provides instructions for configuring and running the Excel Ingestion Pipeline. This pipeline processes Excel files based on a user-defined configuration, validates the data against predefined schemas, and outputs normalized data in CSV format with additional metadata fields.

### Overview

The Excel Ingestion Pipeline:
1. **Loads Excel files** from specified sources.
2. **Normalizes and validates data** according to predefined schemas.
3. **Writes output** to a target location in CSV format, enriched with metadata fields like file paths, sheet names, and creation timestamps.

### Configuration

The pipeline is driven by a YAML configuration file that defines:
- **Sources**: Excel files and their respective sheets to be loaded.
- **Targets**: Output format and destination.
- **Metadata**: Additional metadata to describe the process.

#### Example Configuration File

Create a YAML configuration file (`pipeline_config.yaml`) as follows:

```yaml
name: "Example_Excel_Pipeline"
version: "1.0"

metadata:
  domain: "Data Processing"
  owner: "data_team"
  source_system: "internal"
  refresh_frequency: "adhoc"
  description: "Pipeline for loading and processing Excel data into CSV format."

source_files:
  - file_name: "example_file_1.xlsx"
    file_type: "excel"
    path: "project_files/datalake/source/example_file_1.xlsx"
    loader_config:
      tab_names:
        - "Sheet1"
  - file_name: "example_file_2.xlsx"
    file_type: "excel"
    path: "project_files/datalake/source/example_file_2.xlsx"
    loader_config:
      tab_names:
        - "Sheet2"

target:
  type: "csv"
  writer_config:
    destination: "project_files/datalake/bronze"
    table_name_prefix: "normalized_"
  schema: 
    path: "project_files/schemas/excel_schema.yaml"
```

### Predefined Schemas, Loaders, and Writers

#### Schema Definition

Schemas define the expected data structure and types. This allows the pipeline to validate and normalize data accordingly. 

Example schema (`excel_schema.yaml`):

```yaml
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
```

#### Loader Configuration

The loader configuration specifies how to load data from source files:
- **File Path**: Location of the Excel file.
- **Tab Names**: Excel sheet names to be processed.

#### Writer Configuration

The writer configuration determines the format and destination of the output:
- **Destination**: Directory for output files.
- **Table Name Prefix**: Prefix for output filenames.
- **Normalization**: Adds metadata fields (`source_filepath`, `source_sheetname`, `created_time`) to the output.

### Running the Pipeline

1. **Prepare Your Files**:
   - **Configuration File**: Define sources, targets, and schemas in a YAML file.
   - **Schema File**: Define expected data structure in a YAML file.

2. **Execute the Pipeline**:
   Run the pipeline using the `ingest_pipeline` function:

```python
from src.pipelines.source.excel_ingestion_process import ingest_pipeline

# Run the pipeline with the configuration file
ingest_pipeline('project_files/configs/pipeline_config.yaml')
```

Alternatively, execute from the command line:

```bash
python src/pipelines/source/excel_ingestion_process.py --config project_files/configs/source/example/pipeline_config.yaml
```

### Normalization Details

During processing, the pipeline adds the following metadata columns to the output CSV:
- **`source_filepath`**: Path of the original Excel file.
- **`source_sheetname`**: Name of the processed sheet.
- **`created_time`**: Timestamp when the data was processed.

### Supported Formats

- **Input**: Excel files (`.xlsx`).
- **Output**: CSV files.

### Directory Structure

Ensure your project is structured like this:

```
project/
│
├── src/
│   └── pipelines/
│       └── source/
│           └── excel_ingestion_process.py
│
├── tests/
│   └── integration/
│       └── pipelines/
│           └── source/
│               └── test_excel_pipeline.py
│
├── project_files/
│   ├── configs/
│   │   └── pipeline_config.yaml
│   ├── schemas/
│   │   └── excel_schema.yaml
│   └── datalake/
│       ├── source/
│       └── bronze/
```

### Conclusion

By following these instructions, you can configure and run the Excel Ingestion Pipeline to load, normalize, validate, and store data as needed. This modular design allows for easy integration with various data sources and targets.