import base64
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from src.salesforce_cdc.pubsub_client import OPPORTUNITY_CHANGE_TOPIC

DEFAULT_CHECKPOINT_PATH = Path("checkpoints/opportunity_replay.json")


def _fsync_directory(path: Path) -> None:
    directory_descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def encode_replay_id(replay_id: bytes) -> str:
    return base64.b64encode(replay_id).decode("ascii")


def decode_replay_id(encoded_replay_id: str) -> bytes:
    return base64.b64decode(encoded_replay_id, validate=True)


def load_checkpoint(
    path: Path = DEFAULT_CHECKPOINT_PATH,
    topic: str = OPPORTUNITY_CHANGE_TOPIC,
) -> bytes | None:
    if not path.exists():
        return None

    try:
        checkpoint = json.loads(path.read_text(encoding="utf-8"))
        if checkpoint["topic"] != topic:
            raise ValueError("Checkpoint topic does not match subscription topic")
        replay_id = decode_replay_id(checkpoint["replay_id"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid replay checkpoint: {path}") from error

    if not replay_id:
        raise ValueError(f"Invalid replay checkpoint: {path}")
    return replay_id


def save_checkpoint(
    replay_id: bytes,
    path: Path = DEFAULT_CHECKPOINT_PATH,
    topic: str = OPPORTUNITY_CHANGE_TOPIC,
) -> None:
    if not replay_id:
        raise ValueError("Replay ID cannot be empty")

    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "replay_id": encode_replay_id(replay_id),
        "updated_at": datetime.now(UTC).isoformat(),
        "topic": topic,
    }

    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as temporary_file:
            json.dump(checkpoint, temporary_file, indent=2)
            temporary_file.write("\n")
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)