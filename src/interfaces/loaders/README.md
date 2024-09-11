## Loaders

Loaders are responsible for reading data from various sources and converting it into a format suitable for further processing. Each loader is designed to handle a specific file type (e.g., Excel, CSV) and can be dynamically configured via the pipeline configuration file. 

### Available Loaders

1. **ExcelLoader**
   - **Description**: Loads data from Excel files (`.xlsx`) by reading specified sheets.
   - **Configuration**:
     - **`file_type`**: `"excel"` — Specifies the loader type.
     - **`loader_config`**:
       - **`tab_names`**: List of Excel sheet names to load.
   - **Usage**:
     ```yaml
     source_files:
       - file_name: "example_file_1"
         file_type: "excel"
         path: "project_files/datalake/source/example/example_file_1.xlsx"
         loader_config:
           tab_names:
             - "Sheet1"
     ```

### Dynamic Loader Loading

Loaders are dynamically loaded based on the `file_type` specified in the configuration file. The loader module and class names are specified in the configuration, allowing the pipeline to import and use them at runtime.

### How Loaders Work

- The pipeline reads the configuration file to determine the loader type.
- It dynamically imports the appropriate loader class (e.g., `ExcelLoader`).
- The loader reads the specified source files and returns the data in a format that the pipeline can process (usually a `pandas.DataFrame`).
