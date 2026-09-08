import unittest

from src.databricks.bronze_ingestion import (
    bronze_schema_ddl,
    validate_identifier,
    volume_root,
)


class BronzeIngestionTests(unittest.TestCase):
    def test_volume_root(self) -> None:
        self.assertEqual(
            volume_root("project", "bronze", "landing"),
            "/Volumes/project/bronze/landing",
        )

    def test_schema_contains_required_columns(self) -> None:
        schema = bronze_schema_ddl()

        for column in (
            "replay_id STRING",
            "schema_id STRING",
            "record_ids ARRAY<STRING>",
            "change_type STRING",
            "changed_fields ARRAY<STRING>",
            "commit_timestamp TIMESTAMP",
            "received_at TIMESTAMP",
            "topic STRING",
            "payload VARIANT",
            "_rescued_data STRING",
        ):
            self.assertIn(column, schema)

    def test_invalid_identifier_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            validate_identifier("bronze; DROP SCHEMA bronze")


if __name__ == "__main__":
    unittest.main()