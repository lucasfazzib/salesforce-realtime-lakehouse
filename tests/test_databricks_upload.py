import tempfile
import unittest
from pathlib import Path

from src.salesforce_cdc.databricks_upload import (
    destination_uri,
    event_files,
    volume_landing_uri,
)


class DatabricksUploadTests(unittest.TestCase):
    def test_volume_landing_uri(self) -> None:
        self.assertEqual(
            volume_landing_uri("project", "bronze", "landing"),
            "dbfs:/Volumes/project/bronze/landing/salesforce/opportunity",
        )

    def test_destination_preserves_partition_path(self) -> None:
        root = Path("raw_events/opportunity")
        local_file = root / "2026" / "09" / "07" / "event-abc.json"

        destination = destination_uri(
            local_file,
            root,
            "project",
            "bronze",
            "landing",
        )

        self.assertEqual(
            destination,
            "dbfs:/Volumes/project/bronze/landing/salesforce/opportunity/"
            "2026/09/07/event-abc.json",
        )

    def test_event_files_only_returns_event_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            partition = root / "2026" / "09" / "07"
            partition.mkdir(parents=True)
            expected = partition / "event-one.json"
            expected.write_text("{}", encoding="utf-8")
            (partition / "notes.txt").write_text("ignore", encoding="utf-8")

            self.assertEqual(event_files(root), [expected])


if __name__ == "__main__":
    unittest.main()