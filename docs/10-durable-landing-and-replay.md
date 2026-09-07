# 10 - Durable Landing and Replay

## Goal

Turn the local CDC subscriber from a console demonstration into a minimal
recoverable ingestion component without introducing Databricks.

The required ordering is:

```text
receive -> decode -> persist raw event -> update replay checkpoint
```

If raw persistence fails, the checkpoint does not advance. A restart can then
request the event again from Salesforce.

## Local Landing Layout

Events are partitioned by the Salesforce commit date in UTC:

```text
raw_events/
  opportunity/
    YYYY/
      MM/
        DD/
          event-<replay-sha256>.json
```

The layout is deterministic and compatible with future file discovery. One
file per event is intentionally simple for this phase; production landing will
eventually need bounded batching to avoid excessive small files.

## Raw Event Document

Each JSON file contains:

- Base64 replay ID;
- schema ID;
- Salesforce record IDs;
- change type;
- expanded changed fields;
- commit and receipt timestamps in ISO 8601 UTC;
- topic name;
- the complete decoded CDC payload.

Decoded byte values inside the payload are represented as an object containing
`encoding: base64` and the reversible encoded data. Credentials and gRPC
authentication metadata are never included.

## Deterministic Identity and Duplicates

The filename uses SHA-256 of the binary replay ID. The hash is safe for paths,
stable across restarts, and avoids lossy replay ID conversion.

Persistence writes and fsyncs a temporary file, then creates the final path
atomically without overwriting. If the final path already exists, the code
parses it and compares stable event fields. A matching event is reported as a
duplicate; a malformed or conflicting file stops processing.

## Replay Checkpoint

The checkpoint lives at:

```text
checkpoints/opportunity_replay.json
```

It contains a reversible Base64 replay ID, update timestamp, and topic. Writes
use a temporary file, flush, fsync, and atomic replace.

On startup:

- no checkpoint means `ReplayPreset.LATEST`;
- a valid checkpoint means `ReplayPreset.CUSTOM` with the decoded replay bytes.

Salesforce starts a custom subscription after the supplied replay ID, so the
checkpoint represents the last event safely persisted locally.

An invalid checkpoint is not silently ignored. Startup fails rather than
falling back to `LATEST` and risking unnoticed data loss.

## Failure Behavior

| Failure point | Result |
|---|---|
| before raw write | checkpoint remains unchanged |
| during temporary raw write | no final event file and no checkpoint change |
| after raw write, before checkpoint | event can be delivered again and recognized |
| during checkpoint temporary write | previous checkpoint remains valid |
| after checkpoint replace | restart resumes after the persisted event |

This is minimal at-least-once behavior, not exactly-once processing.

## Runtime Data and Git

`raw_events/`, `checkpoints/`, and `logs/` are ignored by Git. They can contain
business data and must not be committed, attached to public issues, or treated
as production storage.

## Tests

```bash
./.venv/bin/python -m unittest discover -s tests -v
```

The tests cover Base64 round trips, checkpoint save/load, invalid checkpoints,
deterministic partition paths, required event fields, duplicate persistence,
custom replay request construction, and the event-before-checkpoint guarantee.

## Known Limitations

- local disk is not durable across machine loss;
- one file per event creates small-file overhead;
- there is no file compaction or retention policy;
- there is no multi-process checkpoint locking;
- Salesforce replay retention remains finite;
- token refresh and reconnect are not implemented;
- cloud landing and Databricks ingestion are intentionally out of scope.