# tests/test_dag_loader.py

import pytest
from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    """Fixture for loading the Airflow DAG bag."""
    return DagBag()

def test_dag_loaded(dagbag):
    """Test if the DAG is loaded correctly."""
    dag = dagbag.get_dag(dag_id="excel_ingestion_dag")
    assert dag is not None, "DAG 'excel_ingestion_dag' is not loaded"
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert len(dag.tasks) == 1, "DAG should have exactly one task"
