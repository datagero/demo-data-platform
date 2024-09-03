import os
import json
import re
from collections import defaultdict

schemas_gpr = json.load(open('code/schemas/source/gpr/schemas.json'))
schemas_pavement = json.load(open('code/schemas/source/pavement/schemas.json'))

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


def check_schema_of_file_tab(filename, tabname):
    [print(f"'{x}',") for x in json_structures_pavement[filename][tabname]['columns']]
    return json_structures_pavement[filename][tabname]['columns']

# List the contents of the extracted 'output' folder to check the files inside
output_folder_path = 'output'
output_files = os.listdir(output_folder_path)
output_files

# Explore the contents of the subdirectory '2019 TSD Route GPR Files' within 'GPR only'
gpr_files_path = os.path.join(output_folder_path, 'GPR only', '2019 TSD Route GPR Files')
# include_only = ['I_75_RWP.json', 'I_95_SB_RWP.json']

# Explore the contents of a few subdirectories in 'Pavement Evaluation Summaries and Data'
# Define the root directory path
pavement_files_path = os.path.join(output_folder_path, 'Pavement Evaluation Summaries and Data')


# # Load and analyze the selected example files
# json_structures = {os.path.basename(file_path): load_json_file(file_path) for file_path in example_files}

# json_structures

json_structures_gpr = traverse_files(gpr_files_path, base_folder=output_folder_path)
json_structures_pavement = traverse_files(pavement_files_path, base_folder=output_folder_path)

# json_structures = {key: json_structures[key] for key in include_only}

out_scope_sheetnames = ['Plots', 'Stats', 'Every 0.1 mile', 'Average Every 0.1 mile']
out_scope_pairs = [('US_23_RWP_001.json', 'Sheet1')]

# Categorize the files based on their structures
categorized_files_gpr = categorize_files(json_structures_gpr, schemas_gpr, out_scope_sheetnames)
categorized_files_pavement = categorize_files(json_structures_pavement, schemas_pavement, out_scope_sheetnames)

# Output the categorized files
with open('code/data_profiling/categorized_files/categorized_files_gpr.json', 'w') as f:
    json.dump(categorized_files_gpr, f, indent=4)

with open('code/data_profiling/categorized_files/categorized_files_pavement.json', 'w') as f:
    json.dump(categorized_files_pavement, f, indent=4)

pass

# # Use the previously loaded JSON structures to categorize the files
# categorized_files = categorize_files(json_structures)

# categorized_files
