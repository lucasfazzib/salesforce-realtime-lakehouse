# Study Guide

This directory records the project's decisions, concepts, and validation steps
without storing credentials or real Salesforce data.

## Learning Path

1. [Salesforce foundation](01-salesforce-foundation.md): OAuth Client
   Credentials, REST/SOQL, and Change Data Capture setup.
2. [Proto and gRPC stubs](02-proto-and-grpc-stubs.md): the official Pub/Sub API
   contract and Python client generation.
3. [Pub/Sub client](03-pubsub-client.md): TLS, authentication metadata,
   `GetTopic`, and `GetSchema`.
4. [CDC subscriber and Avro](04-cdc-subscriber.md): subscriptions, flow
   control, schemas, and event decoding.
5. [Local validation runbook](05-local-validation-runbook.md): a safe,
   repeatable end-to-end test.
6. [Future Databricks architecture](06-databricks-production-architecture.md):
   landing, Bronze, Silver, Gold, and deployment options.
7. [Ingestion strategy](07-ingestion-strategy.md): full batch, incremental,
   CDC, and hybrid approaches.
8. [Security and operations](08-security-and-operations.md): identity, secrets,
   networking, observability, replay, and recovery.
9. [Architecture trade-offs](09-architecture-tradeoffs.md): batch incremental
   ingestion, cumulative Bronze scans, CDC, and a practical hybrid migration.
10. [Durable landing and replay](10-durable-landing-and-replay.md): atomic raw
   event persistence, checkpoints, duplicate safety, and restart recovery.

## Current Scope

The repository currently implements only the Salesforce Pub/Sub foundation:

- OAuth Client Credentials authentication;
- REST/SOQL validation;
- a TLS connection to Salesforce Pub/Sub API;
- topic and schema discovery;
- local receipt and decoding of an Opportunity CDC event;
- local partitioned raw event persistence;
- a durable local replay checkpoint.

Cloud landing, Auto Loader, Delta tables, and reconciliation are future
architecture proposals, not implemented features.

## Documentation Security Rule

Examples use placeholders. Never add the following to these documents:

- `.env` contents;
- client secrets or access tokens;
- real record, user, or organization IDs;
- private workspace URLs;
- production replay IDs;
- payloads containing real personal or commercial data.