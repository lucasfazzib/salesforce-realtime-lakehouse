import base64
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.salesforce_cdc.checkpoint import encode_replay_id

DEFAULT_LANDING_ROOT = Path("raw_events/opportunity")


@dataclass(frozen=True)
class LandingResult:
    path: Path
    created: bool


def _fsync_directory(path: Path) -> None:
    directory_descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _as_utc(timestamp: str | datetime) -> datetime:
    parsed = (
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if isinstance(timestamp, str)
        else timestamp
    )
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"Unsupported payload value type: {type(value).__name__}")


def build_event_path(
    replay_id: bytes,
    commit_timestamp: str | datetime,
    landing_root: Path = DEFAULT_LANDING_ROOT,
) -> Path:
    if not replay_id:
        raise ValueError("Replay ID cannot be empty")
    committed_at = _as_utc(commit_timestamp)
    event_key = hashlib.sha256(replay_id).hexdigest()
    return (
        landing_root
        / f"{committed_at.year:04d}"
        / f"{committed_at.month:02d}"
        / f"{committed_at.day:02d}"
        / f"event-{event_key}.json"
    )


def build_event_document(
    *,
    replay_id: bytes,
    schema_id: str,
    record_ids: list[str],
    change_type: str,
    changed_fields: list[str],
    commit_timestamp: str | datetime,
    received_at: datetime,
    topic: str,
    payload: dict,
) -> dict:
    return {
        "replay_id": encode_replay_id(replay_id),
        "schema_id": schema_id,
        "record_ids": record_ids,
        "change_type": change_type,
        "changed_fields": changed_fields,
        "commit_timestamp": _as_utc(commit_timestamp).isoformat(),
        "received_at": _as_utc(received_at).isoformat(),
        "topic": topic,
        "payload": _json_safe(payload),
    }


def _validate_existing_event(path: Path, expected: dict) -> None:
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Existing event file is invalid: {path}") from error

    comparable_fields = set(expected) - {"received_at"}
    if any(existing.get(field) != expected[field] for field in comparable_fields):
        raise ValueError(f"Existing event file does not match replayed event: {path}")


def persist_event(
    *,
    replay_id: bytes,
    schema_id: str,
    record_ids: list[str],
    change_type: str,
    changed_fields: list[str],
    commit_timestamp: str | datetime,
    topic: str,
    payload: dict,
    landing_root: Path = DEFAULT_LANDING_ROOT,
    received_at: datetime | None = None,
) -> LandingResult:
    path = build_event_path(replay_id, commit_timestamp, landing_root)
    document = build_event_document(
        replay_id=replay_id,
        schema_id=schema_id,
        record_ids=record_ids,
        change_type=change_type,
        changed_fields=changed_fields,
        commit_timestamp=commit_timestamp,
        received_at=received_at or datetime.now(UTC),
        topic=topic,
        payload=payload,
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        _validate_existing_event(path, document)
        return LandingResult(path=path, created=False)

    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as temporary_file:
            json.dump(document, temporary_file, indent=2, sort_keys=True)
            temporary_file.write("\n")
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            _validate_existing_event(path, document)
            return LandingResult(path=path, created=False)
        _fsync_directory(path.parent)
        return LandingResult(path=path, created=True)
    finally:
        temporary_path.unlink(missing_ok=True)