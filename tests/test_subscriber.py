import unittest
from pathlib import Path
from unittest.mock import patch

from generated import pubsub_api_pb2
from src.salesforce_cdc.subscriber import _build_fetch_request, _process_event


class SubscriberTests(unittest.TestCase):
    def test_fetch_request_uses_latest_without_checkpoint(self) -> None:
        request = _build_fetch_request(None, event_limit=5)

        self.assertEqual(request.replay_preset, pubsub_api_pb2.LATEST)
        self.assertEqual(request.replay_id, b"")

    def test_fetch_request_uses_custom_replay_id(self) -> None:
        replay_id = b"saved-replay-id"

        request = _build_fetch_request(replay_id, event_limit=5)

        self.assertEqual(request.replay_preset, pubsub_api_pb2.CUSTOM)
        self.assertEqual(request.replay_id, replay_id)

    @patch("src.salesforce_cdc.subscriber.save_checkpoint")
    @patch("src.salesforce_cdc.subscriber.persist_event")
    @patch("src.salesforce_cdc.subscriber._decode_payload")
    def test_landing_failure_does_not_advance_checkpoint(
        self,
        decode_payload,
        persist_event,
        save_checkpoint,
    ) -> None:
        decode_payload.return_value = {
            "ChangeEventHeader": {
                "recordIds": ["record-id"],
                "changeType": "UPDATE",
                "changedFields": [],
                "commitTimestamp": 1_788_756_683_000,
            }
        }
        persist_event.side_effect = OSError("landing unavailable")
        event = pubsub_api_pb2.ConsumerEvent(
            event=pubsub_api_pb2.ProducerEvent(
                schema_id="schema-id",
                payload=b"payload",
            ),
            replay_id=b"replay-id",
        )

        with self.assertRaises(OSError):
            _process_event(
                client=object(),
                schema_cache={"schema-id": object()},
                event=event,
                landing_root=Path("unused-landing"),
                checkpoint_path=Path("unused-checkpoint.json"),
            )

        save_checkpoint.assert_not_called()


if __name__ == "__main__":
    unittest.main()