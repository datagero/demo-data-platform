# src/helpers/data_profiling/profile_schema_category.py

import os
import json
import re
from collections import defaultdict

# Define function to load and analyze a JSON file
def load_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except Exception as e:
        return {'error': str(e)}

# Function to traverse a directory and analyze all JSON files
def traverse_files(directory_path, base_folder=''):
    json_structures = {}

    # Traverse the directory
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                print(f"Analyzing {file_path}...")
                json_data = load_json_file(file_path)
                json_structures[file_path] = json_data
    
    return json_structures


def filter_columns(columns):
    """
    Filter out columns that are 'Unnamed' or represent digit ranges following an 'Unnamed' column.

    Args:
    columns (list): List of column names.

    Returns:
    list: Filtered list of column names.
    """
    # Regular expressions for matching rules
    unnamed_pattern = re.compile(r"^Unnamed")
    digit_range_pattern = re.compile(r"^\d+\.\d+-\d+\.\d+$")

    # Initialize the flag to determine when to ignore digit ranges
    ignore_following_digits = False
    filtered_columns = []

    str_columns = [str(col) for col in columns] # Convert all columns to strings

    for col in str_columns:
        if unnamed_pattern.match(col):
            # Set flag if "Unnamed" column is found
            ignore_following_digits = True
        elif digit_range_pattern.match(col) and ignore_following_digits:
            # Ignore digit ranges after an "Unnamed" column
            continue
        else:
            # Valid column, reset flag and add to filtered list
            ignore_following_digits = False
            filtered_columns.append(col)

    return filtered_columns

# Function to check if a sheet matches a default schema
def check_schema_match(schema, sheet_info):
    required_columns = set(schema)
    # Filter out columns according to defined rules
    filtered_columns = filter_columns(sheet_info.get('columns', []))
    file_columns = set(filtered_columns)
    common_columns = required_columns.intersection(file_columns)

    # Check for exact match
    if file_columns == required_columns:
        match_percentage = len(common_columns) / len(required_columns)
        return "exact", 0, match_percentage

    # Check for extended match
    if required_columns.issubset(file_columns):
        match_percentage = len(common_columns) / len(required_columns)
        additional_columns = file_columns - required_columns
        return "extended", len(additional_columns), match_percentage

    # Check for partial match
    match_percentage = len(common_columns) / len(required_columns)
    if match_percentage >= 0.7:  # Define partial match threshold (70%)
        additional_columns = file_columns - common_columns
        return "partial", len(additional_columns), match_percentage
    
    # No match
    return None, 0, match_percentage

def remove_lower_priority_entries(groups, priority_order):
    """
    Remove entries from lower priority groups based on the entries present in higher priority groups.

    Args:
    groups (dict): Dictionary where keys are group names and values are lists of entries.
    priority_order (list): List of group names sorted by priority (highest priority first).

    Returns:
    dict: Updated groups with lower priority entries removed.
    """
    # Initialize a set to keep track of all entries that have already been assigned to a higher-priority group
    assigned_entries = set()

    # Iterate through each group in priority order
    for group_name in priority_order:
        current_group = groups[group_name]

        # Remove entries that are already assigned to a higher-priority group
        groups[group_name] = [
            entry for entry in current_group
            if (entry[0], entry[1]) not in assigned_entries
        ]

        # Add the current group's entries to the assigned_entries set
        assigned_entries.update((file_name, sheet_name) for file_name, sheet_name, *_ in groups[group_name])

    return groups

# Function to categorize JSON structures based on schema match types at the sheet level
def categorize_files(json_files, schemas, out_scope_sheetnames=None):
    # Prepare categories
    exact_match_groups = defaultdict(list)
    extended_match_groups = defaultdict(list)
    partial_match_groups = defaultdict(list)
    no_match_sheets = defaultdict(list)
    out_scope_sheets = defaultdict(list)

    if out_scope_sheetnames is None:
        out_scope_sheetnames = []

    # Iterate through the JSON files and analyze structures at the sheet level
    for file_name, content in json_files.items():
        filepath = content.get('filepath', '')
        for sheet_name, sheet_info in content.items():
            
            if sheet_name == 'filepath':
                # Skip admin keys
                continue

            if sheet_name in out_scope_sheetnames:
                out_scope_sheets[filepath].append(sheet_name)
                continue

            # Initialize flags for the highest-priority match found
            highest_priority_match = None
            match_details = None

            # # Debug
            if (file_name, sheet_name) == ('I-285 EB Mainlines, Lanes 4-6_FINAL RESULTS.json', 'Lane 4, LWP'):
                1==1

            # Check against schemas in order of priority
            for schema_name, schema in schemas.items():
                if not schema:
                    # Skip empty schemas
                    continue
                match_type, additional_columns, match_percentage = check_schema_match(schema, sheet_info)

                if match_type == "exact":
                    # Found an exact match; stop further checks
                    highest_priority_match = ('exact', schema_name)
                    match_details = (filepath, sheet_name, match_percentage)
                    break

                elif match_type == "extended" and highest_priority_match is None:
                    # Track extended match if no higher-priority match was found
                    highest_priority_match = ('extended', schema_name)
                    match_details = (filepath, sheet_name, additional_columns, match_percentage)

                elif match_type == "partial" and highest_priority_match is None:
                    # Track partial match if no higher-priority match was found
                    highest_priority_match = ('partial', schema_name)
                    match_details = (filepath, sheet_name, additional_columns, match_percentage)

            # Append to the appropriate group based on the highest-priority match found
            if highest_priority_match:
                match_type, schema_name = highest_priority_match
                if match_type == 'exact':
                    exact_match_groups[schema_name].append(match_details)
                elif match_type == 'extended':
                    extended_match_groups[schema_name].append(match_details)
                elif match_type == 'partial':
                    partial_match_groups[schema_name].append(match_details)
            else:
                # No match was found for this sheet
                no_match_sheets[file_name].append(sheet_name)

    # Combine all groups into one dictionary
    all_groups = {
        'exact_match_groups': dict(exact_match_groups),
        'extended_match_groups': dict(extended_match_groups),
        'partial_match_groups': dict(partial_match_groups),
        'no_match_sheets': dict(no_match_sheets),
        'out_scope_sheets': dict(out_scope_sheets)
    }

    # Sort all_groups values by key name
    for group_name, group_data in all_groups.items():
        all_groups[group_name] = dict(sorted(group_data.items()))
 
    return all_groups

# Assuming `json_structures` contains your data and the `schemas_path` is None
def get_schemas_from_json(json_structures):
    # Use a set to gather unique column combinations
    unique_schemas = {
        tuple(val['columns'])
        for dict_obj in json_structures.values()
        for item_key, val in dict_obj.items()
        if item_key != 'filepath'
    }

    # Initialize a dictionary to hold the base schemas and their variations
    schema_dict = {}

    # Assign base schema names and variations
    for idx, schema in enumerate(unique_schemas):
        if idx == 0:
            # First schema will be considered the "Base Schema"
            schema_dict[f"Base Schema {idx + 1}"] = list(schema)
        else:
            # Subsequent schemas will be considered variations
            schema_dict[f"Variation {idx}"] = list(schema)

    return schema_dict

def main(json_files_path, output_path, filename, schemas_path=None, out_scope_sheetnames=None):
    """
    Main function to categorize files based on JSON profiles.

    Args:
    json_files_path (str): The path to the directory containing JSON files.
    output_path (str): The path to the output directory.
    filename (str): The name of the output file.
    schemas_path (str, optional): Path to a JSON file containing predefined schemas. 
                                  If not provided, schemas will be inferred from the input files.
    out_scope_sheetnames (list, optional): A list of sheet names to exclude from analysis.
    """
    # Traverse the JSON files directory and load file structures
    json_structures = traverse_files(json_files_path)

    if schemas_path is None:
        # Get schemas from input data if no predefined schemas are provided
        schemas = get_schemas_from_json(json_structures)
    else:
        # Load predefined schemas from the given schemas_path
        schemas = load_json_file(schemas_path)

    # Categorize the files based on the schemas
    categorized_files = categorize_files(json_structures, schemas, out_scope_sheetnames)
    
    # Ensure the output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Save the categorized files to a JSON output file
    output_file_path = os.path.join(output_path, f'{filename}.json')
    with open(output_file_path, 'w') as f:
        json.dump(categorized_files, f, indent=4)

    print(f"Categorized files are saved in '{output_file_path}'.")
