# 08 - Security and Operations

## Threat Model

Plan for credential leaks, excessive privileges, sensitive payloads in logs,
unauthorized landing changes, environment mixing, lost replay state,
duplicates, out-of-order application, compromised dependencies, human
credentials in automation, and outages longer than event retention.

Security includes confidentiality, integrity, and availability. Protecting a
secret while ignoring event integrity or recovery is insufficient.

## Salesforce Identity

Use a dedicated Connected App or External Client App per environment and a
non-human integration user. Grant only required API, object, field, CDC channel,
and Pub/Sub permissions through permission sets. Avoid System Administrator.

Define processes for secret rotation, immediate revocation, login and API
auditing, integration ownership, and periodic permission review.

## Secrets

### Local Development

- keep `.env` ignored;
- keep only empty placeholders in `.env.example`;
- never show tokens in terminals or screenshots;
- exclude gRPC metadata from errors and traces;
- use credentials that cannot access production.

### Cloud and Databricks

Prefer workload identity and a managed secret store:

- Azure Managed Identity and Key Vault;
- AWS IAM roles and Secrets Manager;
- GCP Workload Identity and Secret Manager;
- Databricks secret scopes only where direct integration requires them.

Secret scopes reduce accidental exposure but do not make printing safe. Combine
them with ACLs, cluster policies, and workload isolation.

Never place secrets in `databricks.yml`, notebooks, command-line arguments,
source control, storage access keys, or personal automation tokens.

## Databricks Identity and Governance

Use service principals for Jobs and CI/CD. Prefer OAuth machine-to-machine or
CI provider OIDC federation over long-lived tokens. Separate deployer and
runtime permissions and use distinct identities per environment.

With Unity Catalog:

- grant `USE CATALOG` and `USE SCHEMA` selectively;
- grant write access only to required landing or Bronze targets;
- use storage credentials and external locations;
- avoid legacy mounts and DBFS root for governed data;
- assign ownership to groups rather than individuals;
- use row filters, column masks, tags, lineage, and audit logs as needed.

## Landing Security

- encrypt in transit and at rest;
- keep the bucket or container private;
- separate consumer writer and Databricks reader identities;
- use append-only permissions where practical;
- evaluate versioning or object lock for critical data;
- define lifecycle and retention explicitly;
- audit reads, writes, and deletes;
- isolate environment prefixes, containers, and preferably cloud accounts.

## Network Controls

The consumer needs TLS egress to the Salesforce login domain on port 443,
`api.pubsub.salesforce.com` on port 7443, object storage, and the secret manager.
Use destination allowlists when practical. Proxies and firewalls must support
HTTP/2 for gRPC. Monitor DNS, TLS handshake, and idle connection failures.

Evaluate private endpoints or PrivateLink for Databricks and storage. The
public Salesforce endpoint still requires controlled outbound connectivity.

## Data Protection

Classify Opportunity fields before landing them, including personal data,
financial values, free text, retention requirements, and fields that should
never enter the lakehouse.

Raw landing may be needed for replay, but it should have tighter access and
possibly shorter retention than curated layers. Do not write complete payloads
to logs. Prefer technical IDs, counts, schema IDs, change types, and controlled
correlation identifiers.

## Replay Integrity

Store replay state as opaque bytes or reversible Base64, partitioned by
organization and topic. Update it only after durable landing, protect it from
concurrent writers, include it in backup and restore, and never use it as the
business key.

```text
receive -> validate envelope -> write landing -> confirm durability
        -> update replay checkpoint -> request more capacity
```

A crash before checkpointing should cause duplication rather than loss.
Downstream processing must be idempotent.

## Token Refresh and Reconnection

A production process should:

1. renew before token expiration or after `UNAUTHENTICATED`;
2. rebuild the channel and stream with fresh credentials;
3. resume after the last durable replay ID;
4. use exponential backoff with jitter;
5. bound retries and alert on extended failure;
6. distinguish transient errors from configuration errors;
7. never log old or new tokens.

## Observability

Track:

- received, landed, duplicate, and rejected events;
- bytes and throughput;
- lag from `commitTimestamp` to ingestion;
- age of the last event and checkpoint;
- reconnects and OAuth/gRPC failures by status;
- new schema IDs;
- landing file count and average size;
- quarantine volume;
- reconciliation differences.

Use structured logs with redaction. Traces may include `rpc_id` and an internal
run ID, but never authentication metadata.

Alert on disconnection, SLA lag, unexpected event silence, failed token refresh,
stalled replay state, unusual quarantine or duplicate rates, reconciliation
drift, and API quota or cost thresholds.

## Supply Chain and CI/CD

- pin and intentionally update dependencies;
- scan pull requests for vulnerabilities and secrets;
- review proto changes before regeneration;
- generate stubs reproducibly;
- promote one immutable artifact across environments;
- scan and sign the consumer image;
- enable branch protection and review;
- use OIDC for CI authentication when possible.

## Resilience Tests

- revoke the secret and verify safe alerting;
- remove a permission and verify an explicit failure;
- simulate DNS and network failures;
- stop after landing but before checkpointing;
- replay an event and verify deduplication;
- corrupt a payload and verify quarantine;
- introduce a schema change;
- restore a checkpoint backup;
- simulate an outage beyond retention and run a backfill.

## Incident Response

If credential exposure is suspected:

1. revoke the client secret and active sessions;
2. stop the compromised consumer;
3. preserve audit evidence without redistributing the secret;
4. rotate credentials and review permissions;
5. investigate Salesforce, cloud, and Databricks access;
6. verify landing and checkpoint integrity;
7. reconcile or reprocess the affected window;
8. document root cause and preventive controls.

Removing a secret from the latest Git commit is not sufficient. Revoke it
immediately and treat repository history as compromised.

## Production Checklist

- [ ] dedicated least-privilege integration user;
- [ ] managed secrets with tested rotation;
- [ ] service principal per environment;
- [ ] controlled TLS egress;
- [ ] private, encrypted, audited landing;
- [ ] durable and transactional replay checkpoint;
- [ ] tested deduplication and ordering;
- [ ] tested token refresh and reconnect;
- [ ] logs without secrets or payloads;
- [ ] metrics, alerts, and dashboards;
- [ ] quarantine and reprocessing path;
- [ ] `SystemModstamp` reconciliation;
- [ ] backfill and incident runbooks;
- [ ] reviewed CI/CD with scanning;
- [ ] approved classification and retention.

## Official References

- [Databricks security and compliance](https://docs.databricks.com/aws/en/security/)
- [Unity Catalog privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/)
- [Databricks secrets](https://docs.databricks.com/aws/en/security/secrets/)
- [Salesforce Security Guide](https://developer.salesforce.com/docs/atlas.en-us.securityImplGuide.meta/securityImplGuide/)