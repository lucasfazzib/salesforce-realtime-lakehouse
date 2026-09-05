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
- [ ] Pub/Sub connection established
- [ ] Opportunity CDC event received locally
- [ ] Databricks ingestion implemented