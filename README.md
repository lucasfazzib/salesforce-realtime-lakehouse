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
- [x] Databricks Volume landing configured
- [x] DAB validation successful
- [x] DAB deployment successful
- [x] Auto Loader Bronze ingestion implemented
- [x] Salesforce CDC event ingested into Bronze Delta
- [x] Databricks ingestion implemented
- [x] Silver Opportunity current-state table implemented
- [x] Salesforce UPDATE reflected in Silver
- [x] Silver idempotent rerun validated
- [x] Salesforce DELETE semantics validated
- [x] Lakeflow Bronze to Silver orchestration implemented
- [x] DAB Lakeflow workflow deployed
- [x] End-to-end Bronze to Silver workflow validated
- [x] DEV and PROD-SIMULATED bundle targets configured
- [ ] GitHub pull request CI validated
- [x] GitHub DEV deployment validated
- [x] GitHub PROD-SIMULATED deployment validated

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

## Databricks Bronze Test

1. Generate a Salesforce Opportunity CDC event with the local subscriber.
2. Confirm the raw JSON exists under `raw_events/opportunity/YYYY/MM/DD/`.
3. Run `./.venv/bin/python -m src.salesforce_cdc.databricks_upload`.
4. Run `databricks bundle run -t dev bronze_ingestion`.
5. Query `salesforce_realtime_lakehouse.bronze.opportunity_cdc`.

## Databricks Silver Test

1. Change one Opportunity field in Salesforce and run the local-to-Bronze flow.
2. Run `databricks bundle run -t dev silver_opportunity`.
3. Query `salesforce_realtime_lakehouse.silver.opportunity`.
4. Confirm the changed field was updated and unchanged fields were preserved.
5. Rerun the Silver job and confirm there is still one row per Opportunity.

## Lakeflow Workflow Test

1. Start the local subscriber and create, update, or delete an Opportunity.
2. Confirm the CDC event is persisted under `raw_events/opportunity/`.
3. Run `./.venv/bin/python -m src.salesforce_cdc.databricks_upload`.
4. Run `databricks bundle run -t dev salesforce_cdc_pipeline`.
5. Validate Bronze history and the Silver current state with SQL.

## CI/CD

```text
feature/* -> PR to dev -> CI -> merge -> automatic DEV deployment
dev      -> PR to main -> CI -> merge -> PROD-SIMULATED deployment
```

Both DAB targets use the same Databricks Free Edition workspace. DEV preserves
the validated `bronze` and `silver` schemas; PROD-SIMULATED uses `bronze_prod`
and `silver_prod`, a separate Volume, separate DAB state, and distinct Job
names. This is logical isolation for learning, not a production security
boundary. See the [CI/CD guide](docs/14-github-actions-cicd.md) for GitHub
Environment, authentication, branch protection, and setup instructions.

## Documentation

See the [study guide](docs/README.md) for implementation notes, validation,
architecture evolution, ingestion strategies, security and operations.