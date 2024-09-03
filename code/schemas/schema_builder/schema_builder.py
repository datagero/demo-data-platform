
import json
from code.schemas.schema_builder.source_schema_variations import DEFAULT_SCHEMAS_GPR, DEFAULT_SCHEMAS_PAVEMENT

def flatten_schemas(schemas):
    """
    Flattens the nested schema dictionary into a single dictionary where each entry
    contains the base columns plus any variation-specific columns.

    Args:
    schemas (dict): Nested dictionary of schemas and their variations.

    Returns:
    dict: Flattened dictionary with combined base and variation columns.
    """
    flattened_schemas = {}

    # Iterate over each schema group
    for group_name, group_data in schemas.items():
        # Extract the base schema columns
        base_schema_key = [key for key in group_data.keys() if key.startswith("Base")][0]
        base_columns = group_data[base_schema_key]['columns']

        # Add the base schema to the flattened dictionary
        flattened_schemas[base_schema_key] = list(base_columns)

        # Iterate over each variation in the group
        for variation_name, variation_data in group_data.items():
            if variation_name == base_schema_key:
                continue  # Skip the base schema as it is already added

            # Combine the base columns with variation-specific columns
            combined_columns = list(base_columns) + list(variation_data['columns'])
            flattened_schemas[variation_name] = combined_columns

    return flattened_schemas

if __name__ == "__main__":
    flattened_schemas_gpr = flatten_schemas(DEFAULT_SCHEMAS_GPR)
    flattened_schemas_pavement = flatten_schemas(DEFAULT_SCHEMAS_PAVEMENT)

    # Save to json
    with open('code/schemas/source/gpr/schemas.json', 'w') as f:
        json.dump(flattened_schemas_gpr, f, indent=4)

    with open('code/schemas/source/pavement/schemas.json', 'w') as f:
        json.dump(flattened_schemas_pavement, f, indent=4)