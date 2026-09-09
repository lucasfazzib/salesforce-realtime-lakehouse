import unittest

from src.databricks.silver_opportunity import (
    BUSINESS_FIELDS,
    merge_sql,
    silver_table_ddl,
)


class SilverOpportunityTests(unittest.TestCase):
    def test_silver_schema_contains_business_and_operational_columns(self) -> None:
        ddl = silver_table_ddl("catalog.schema.opportunity")

        for column in (
            "opportunity_id STRING",
            "name STRING",
            "stage_name STRING",
            "amount DECIMAL(18, 2)",
            "close_date DATE",
            "last_modified_date TIMESTAMP",
            "is_deleted BOOLEAN",
            "last_commit_number BIGINT",
            "last_sequence_number BIGINT",
            "last_replay_id STRING",
        ):
            self.assertIn(column, ddl)

    def test_merge_preserves_unchanged_business_fields(self) -> None:
        sql = merge_sql("catalog.schema.opportunity", "source_view")

        for column in BUSINESS_FIELDS:
            self.assertIn(
                f"WHEN source.has_{column}_change THEN source.{column} "
                f"ELSE target.{column} END",
                sql,
            )

    def test_merge_uses_soft_delete_and_strong_ordering(self) -> None:
        sql = merge_sql("catalog.schema.opportunity", "source_view")

        self.assertIn("target.is_deleted = source.is_deleted", sql)
        self.assertIn("source.last_commit_timestamp", sql)
        self.assertIn("source.last_commit_number", sql)
        self.assertIn("source.last_sequence_number", sql)


if __name__ == "__main__":
    unittest.main()