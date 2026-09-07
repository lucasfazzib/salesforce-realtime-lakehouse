import json
import tempfile
import unittest
from pathlib import Path

from src.salesforce_cdc.checkpoint import (
    decode_replay_id,
    encode_replay_id,
    load_checkpoint,
    save_checkpoint,
)


class CheckpointTests(unittest.TestCase):
    def test_replay_id_base64_round_trip(self) -> None:
        replay_id = b"\x00\xff\x10salesforce-replay"

        encoded = encode_replay_id(replay_id)

        self.assertEqual(decode_replay_id(encoded), replay_id)

    def test_checkpoint_save_and_load(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            checkpoint_path = Path(temporary_directory) / "opportunity.json"
            replay_id = b"replay-checkpoint"

            self.assertIsNone(load_checkpoint(checkpoint_path))
            save_checkpoint(replay_id, checkpoint_path)

            self.assertEqual(load_checkpoint(checkpoint_path), replay_id)
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(checkpoint["replay_id"], encode_replay_id(replay_id))
            self.assertEqual(
                checkpoint["topic"], "/data/OpportunityChangeEvent"
            )
            self.assertIn("updated_at", checkpoint)

    def test_invalid_checkpoint_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            checkpoint_path = Path(temporary_directory) / "opportunity.json"
            checkpoint_path.write_text('{"topic": "wrong"}', encoding="utf-8")

            with self.assertRaises(ValueError):
                load_checkpoint(checkpoint_path)


if __name__ == "__main__":
    unittest.main()