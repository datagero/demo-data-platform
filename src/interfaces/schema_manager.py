
import pandas as pd

class SchemaManager:

    def validate_data_with_schema(dataframe: pd.DataFrame, schema_name: str):
        # Define a mapping of schema names to their Pandera schema objects
        schema_mapping = {
            "Schema 1": gpr_schema_1,
            "Variation 1A": gpr_schema_1a,
            # Add other schemas here...
        }

        schema = schema_mapping.get(schema_name)

        if schema:
            try:
                # Validate the dataframe with the selected schema
                validated_df = schema.validate(dataframe)
                print(f"Data for schema {schema_name} validated successfully.")
                return validated_df
            except pa.errors.SchemaError as e:
                print(f"Validation error for schema {schema_name}: {e}")
                return None
        else:
            print(f"No schema found for {schema_name}")
            return None
