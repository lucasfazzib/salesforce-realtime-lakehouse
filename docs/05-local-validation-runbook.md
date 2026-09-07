# 05 - Local Validation Runbook

## Prerequisites

- a virtual environment at `.venv`;
- dependencies installed from `requirements.txt`;
- a local, Git-ignored `.env`;
- validated OAuth Client Credentials;
- CDC enabled for Opportunity;
- read and subscribe permissions for the integration user;
- a test Opportunity that can be edited.

Never paste `.env` contents into an issue, commit, screenshot, or shared shell.

## 1. Verify the Proto

```bash
wc -c -l proto/pubsub_api.proto
grep -nE \
  'service PubSub|rpc GetTopic|rpc Subscribe|message FetchRequest|message FetchResponse' \
  proto/pubsub_api.proto
```

Success means the file is non-empty and every required declaration is found.

## 2. Regenerate and Import Stubs

```bash
./.venv/bin/python -m grpc_tools.protoc \
  -Iproto \
  --python_out=generated \
  --grpc_python_out=generated \
  proto/pubsub_api.proto

./.venv/bin/python -c \
  "from generated import pubsub_api_pb2, pubsub_api_pb2_grpc; print('OK')"
```

Success means imports complete without an exception and the generated files
are substantially larger than the empty-proto artifacts.

## 3. Validate GetTopic

```bash
./.venv/bin/python -m src.salesforce_cdc.pubsub_client
```

Expected output shape:

```text
Pub/Sub connection established
Topic: /data/OpportunityChangeEvent
Schema ID: <schema-id>
Can subscribe: True
```

## 4. Validate the Subscriber

```bash
./.venv/bin/python -u -m src.salesforce_cdc.subscriber
```

Wait for:

```text
Waiting for events. Edit the Opportunity StageName in Salesforce.
```

In Salesforce:

1. open the test Opportunity;
2. choose a different `StageName` value;
3. save the record;
4. return to the terminal.

Success means the terminal reports `Change type: UPDATE`, includes `StageName`
among the changed fields, prints a UTC commit timestamp, persists an event under
`raw_events/opportunity/YYYY/MM/DD/`, and updates
`checkpoints/opportunity_replay.json`.

For restart recovery:

1. stop the subscriber with `Ctrl+C`;
2. change `StageName` again while it is stopped;
3. restart the subscriber before Salesforce replay retention expires;
4. verify `Checkpoint loaded. Resuming from saved replay ID.` appears;
5. verify the missed event is persisted without overwriting the first event.

## 5. Pre-Commit Safety Checks

```bash
git status --short
git check-ignore -v .env
git ls-files .env
git diff --check -- . ':!proto/pubsub_api.proto'
```

Expected results:

- `.env` is ignored;
- `git ls-files .env` prints nothing;
- no credential file or test payload appears in the diff;
- authored files contain no whitespace errors.

The official proto contains upstream whitespace. Preserving it byte for byte
makes source verification through hashes straightforward.

## Troubleshooting

### `KeyError: SALESFORCE_ORG_ID`

Add the organization ID only to the local `.env`. Keep `.env.example` empty.

### `UNAUTHENTICATED`

Check token expiration, application policy, run-as user, login URL, and org ID.
Do not print gRPC metadata while investigating.

### `PERMISSION_DENIED`

Review the integration user's object permissions and CDC access. Do not solve
the problem by granting a broad administrator profile.

### `NOT_FOUND` from GetTopic

Confirm the exact `/data/OpportunityChangeEvent` topic and CDC configuration.

### The Stream Keeps Waiting

This is normal when no new event exists. A first run starts at `LATEST`; after a
checkpoint exists, the subscriber resumes after the saved replay ID.

### StageName Is Missing

Confirm the value actually changed and the observed event belongs to the test
record. Salesforce-managed fields may appear in the same commit.