import unittest
import pandas as pd
from src.interfaces.schema_manager import SchemaManager

class TestSchemaManager(unittest.TestCase):

    def setUp(self):
        """
        Set up the test case with a simplified schema path.
        """
        # Path to the simplified schema YAML for testing
        self.schema_path = 'tests/schemas/source/test/test_schema.yaml'
        self.schema_manager = SchemaManager(self.schema_path)

    def test_schema_loading(self):
        """
        Test that the schema is loaded correctly.
        """
        self.assertTrue(self.schema_manager.is_schema_initialized(), "Schema should be initialized correctly.")

    def test_valid_data(self):
        """
        Test validation of a DataFrame that matches the schema.
        """
        # Create a DataFrame that matches the provided schema
        valid_df = pd.DataFrame({
            "IntegerColumn": [1, 2],
            "FloatColumn": [10.5, 20.0],
            "StringColumn": ["A", "B"]
        })

        # Validate the DataFrame
        validated_df = self.schema_manager.validate_data(valid_df)

        # Check that validation passes and returns a DataFrame
        self.assertIsNotNone(validated_df, "Validation should succeed for a matching DataFrame.")

    def test_invalid_data(self):
        """
        Test validation of a DataFrame that does not match the schema.
        """
        # Create a DataFrame that does not match the schema
        invalid_df = pd.DataFrame({
            "IntegerColumn": ["A", "B"],  # Invalid type (string instead of int)
            "FloatColumn": [10.5, 20.0],
            "StringColumn": ["C", "D"]
        })

        # Validate the DataFrame
        validated_df = self.schema_manager.validate_data(invalid_df)

        # Check that validation fails and returns None
        self.assertIsNone(validated_df, "Validation should fail for a DataFrame with incorrect types.")

    def test_schema_details_inference(self):
        """
        Test if the schema manager correctly infers stage, producer, and schema name.
        """
        self.assertEqual(self.schema_manager.stage, "source", "Stage should be correctly inferred.")
        self.assertEqual(self.schema_manager.producer, "test", "Producer should be correctly inferred.")
        self.assertEqual(self.schema_manager.schema_name, "test_schema", "Schema name should be correctly inferred.")

if __name__ == '__main__':
    unittest.main()
