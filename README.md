# Salesforce Real-Time Lakehouse

A production-inspired near-real-time Salesforce ingestion project using
Salesforce Change Data Capture, Pub/Sub API, Python and Databricks.

## Goal

Explore how a traditional scheduled Salesforce batch ingestion architecture
can evolve toward a reliable near-real-time event-driven lakehouse.

## Target Architecture

Salesforce
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
- [ ] Opportunity Change Data Capture enabled
- [ ] Python environment configured
- [ ] Salesforce authentication configured
- [ ] Pub/Sub connection established
- [ ] Opportunity CDC event received locally
- [ ] Databricks ingestion implemented