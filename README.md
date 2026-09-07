# Salesforce Real-Time Lakehouse

A production-inspired near-real-time Salesforce ingestion project using
Salesforce Change Data Capture, Pub/Sub API, Python and Databricks.

## Goal

Explore how a traditional scheduled Salesforce Developer Edition  batch ingestion architecture
can evolve toward a reliable near-real-time event-driven lakehouse.

## Target Architecture

Salesforce Developer Edition 
→ Change Data Capture
→ Pub/Sub API
→ Python Consumer
→ Landing
→ Databricks Auto Loader
→ Bronze
→ Silver
→ Gold

## Current Status

- [x] Salesforce Developer Edition created
- [x] Opportunity Change Data Capture enabled
- [x] Python environment configured
- [x] Salesforce authentication configured
- [x] Salesforce REST API connection validated
- [x] SOQL query against Opportunity validated
- [x] Test Opportunity created
- [x] Pub/Sub connection established
- [x] Opportunity CDC event received locally
- [x] Raw CDC event persistence implemented
- [x] Replay checkpoint persistence implemented
- [x] Subscriber recovery from replay ID validated
- [ ] Databricks ingestion implemented

## Local CDC Test

1. Run `./.venv/bin/python -m src.salesforce_cdc.subscriber`.
2. Edit the test Opportunity in Salesforce.
3. Change its `StageName` value and save the record.
4. Observe the CDC event metadata in the terminal.

## Local Recovery Test

1. Start the subscriber and change the test Opportunity `StageName`.
2. Confirm a JSON event appears under `raw_events/opportunity/YYYY/MM/DD/`.
3. Confirm `checkpoints/opportunity_replay.json` appears.
4. Stop the subscriber, change `StageName` again, and restart it shortly after.
5. Confirm the checkpoint is loaded and the missed event is persisted.

Salesforce replay retention is limited, so this validates only a short outage.

## Documentation

See the [study guide](docs/README.md) for implementation notes, validation,
architecture evolution, ingestion strategies, security and operations.