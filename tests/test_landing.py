import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from src.salesforce_cdc.landing import build_event_path, persist_event


class LandingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.landing_root = Path(self.temporary_directory.name)
        self.event = {
            "replay_id": b"stable-replay-id",
            "schema_id": "schema-id",
            "record_ids": ["record-id"],
            "change_type": "UPDATE",
            "changed_fields": ["StageName"],
            "commit_timestamp": "2026-09-07T04:31:23+00:00",
            "received_at": datetime(2026, 9, 7, 4, 32, tzinfo=UTC),
            "topic": "/data/OpportunityChangeEvent",
            "payload": {"StageName": "Closed Won", "binary": b"\x00\xff"},
            "landing_root": self.landing_root,
        }

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_event_path_is_partitioned_and_deterministic(self) -> None:
        first_path = build_event_path(
            b"stable-replay-id",
            "2026-09-07T04:31:23+00:00",
            self.landing_root,
        )
        second_path = build_event_path(
            b"stable-replay-id",
            "2026-09-07T04:31:23+00:00",
            self.landing_root,
        )

        self.assertEqual(first_path, second_path)
        self.assertEqual(first_path.relative_to(self.landing_root).parts[:3], (
            "2026",
            "09",
            "07",
        ))
        self.assertTrue(first_path.name.startswith("event-"))

    def test_duplicate_event_is_not_overwritten(self) -> None:
        first_result = persist_event(**self.event)
        original_contents = first_result.path.read_text(encoding="utf-8")
        duplicate_event = dict(self.event)
        duplicate_event["received_at"] = datetime(2026, 9, 7, 5, 0, tzinfo=UTC)

        second_result = persist_event(**duplicate_event)

        self.assertTrue(first_result.created)
        self.assertFalse(second_result.created)
        self.assertEqual(second_result.path, first_result.path)
        self.assertEqual(
            second_result.path.read_text(encoding="utf-8"), original_contents
        )

    def test_persisted_event_contains_required_fields(self) -> None:
        result = persist_event(**self.event)

        document = json.loads(result.path.read_text(encoding="utf-8"))
        self.assertEqual(
            set(document),
            {
                "replay_id",
                "schema_id",
                "record_ids",
                "change_type",
                "changed_fields",
                "commit_timestamp",
                "received_at",
                "topic",
                "payload",
            },
        )
        self.assertEqual(document["payload"]["binary"]["encoding"], "base64")


if __name__ == "__main__":
    unittest.main()