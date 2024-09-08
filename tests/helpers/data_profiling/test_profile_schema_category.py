import os
import json
from src.helpers.data_profiling.profile_schema_category import (
    load_json_file,
    traverse_files,
    filter_columns,
    check_schema_match,
    categorize_files
)


def test_traverse_files(tmp_path):
    # Create a temporary directory with mock JSON files
    json_dir = tmp_path / "json_files"
    json_dir.mkdir()

    file1 = json_dir / "file1.json"
    file2 = json_dir / "file2.json"
    with open(file1, 'w') as f:
        json.dump({"data": "file1"}, f)
    with open(file2, 'w') as f:
        json.dump({"data": "file2"}, f)

    # Test traverse_files function
    files = traverse_files(json_dir)
    assert len(files) == 2
    assert str(file1) in files
    assert str(file2) in files

def test_filter_columns():
    columns = ["Unnamed: 0", "1.1-2.1", "Column A", "Column B", "3.2-4.2", "Unnamed: 1"]
    filtered = filter_columns(columns)
    assert filtered == ["Column A", "Column B", "3.2-4.2"]  # Updated expected result

def test_check_schema_match():
    schema = ["Column A", "Column B", "Column C"]
    sheet_info = {"columns": ["Column A", "Column B", "Column C", "Column D"]}

    match_type, additional_columns, match_percentage = check_schema_match(schema, sheet_info)
    assert match_type == "extended"
    assert additional_columns == 1
    assert match_percentage == 1.0

def test_categorize_files():
    json_files = {
        "file1.json": {"filepath": "file1.json", "Sheet1": {"columns": ["Column A", "Column B"]}},
        "file2.json": {"filepath": "file2.json", "Sheet1": {"columns": ["Column C", "Column D"]}},
    }
    schemas = {
        "Schema 1": ["Column A", "Column B"],
        "Schema 2": ["Column C", "Column D"]
    }
    categorized = categorize_files(json_files, schemas)
    assert len(categorized['exact_match_groups']) == 2
    assert "Schema 1" in categorized['exact_match_groups']
    assert "Schema 2" in categorized['exact_match_groups']
