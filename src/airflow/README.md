## Airflow Setup and Usage Guide

This guide provides a quick start to setting up Apache Airflow and running the `excel_ingestion_dag` to process Excel files.

### 1. Install and Configure Airflow

For detailed installation instructions, refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html).

After installing Airflow:

1. **Configure your DAGs folder**:
   - Make sure Airflow knows where to look for your DAG files. By default, Airflow looks in the `dags_folder` directory specified in your `airflow.cfg`. Ensure this setting points to the correct directory where your DAG files are stored.

   - Optionally, change the `show_trigger_form_if_no_params` setting in `airflow.cfg` to `True` to allow triggering DAGs with parameters from the web UI:
     ```ini
     show_trigger_form_if_no_params = True
     ```

### 2. Start Airflow

Start Airflow in standalone mode to run the scheduler and web server:

```bash
airflow standalone
```

This will start the Airflow web server on `http://localhost:8080`.

### 3. Trigger the DAG

You can trigger the `excel_ingestion_dag` either from the Airflow web UI or via the command line.

#### Trigger from the CLI

Use the following command to trigger the DAG with specific input and output directories:

```bash
airflow dags trigger excel_ingestion_dag --conf '{"input_dir": "datalake/source/test", "output_dir": "datalake/bronze/test"}'
```

This will process the Excel files from the specified `input_dir` and save the converted CSV files in the `output_dir`.
