# 03 - Pub/Sub Client

## Responsibility

`src/salesforce_cdc/pubsub_client.py` owns shared Pub/Sub behavior:

- call `get_access_token()`;
- read `SALESFORCE_ORG_ID` from the environment;
- open a TLS-protected gRPC channel;
- build Salesforce authentication metadata;
- expose `GetTopic` and `GetSchema`;
- close the channel predictably.

The official endpoint is:

```text
api.pubsub.salesforce.com:7443
```

## TLS Channel

The client reads the CA bundle from `certifi`, creates credentials with
`grpc.ssl_channel_credentials`, and opens `grpc.secure_channel`. There is no
plaintext fallback.

TLS should be combined with dependency updates, certificate validation,
restricted outbound access, and monitoring of handshake failures. Certificate
verification must never be disabled for convenience.

## Authentication Metadata

Every RPC receives:

| Key | Source | Sensitivity |
|---|---|---|
| `accesstoken` | OAuth response | temporary secret |
| `instanceurl` | OAuth response | configuration data |
| `tenantid` | `SALESFORCE_ORG_ID` | internal identifier |

These values remain in memory. The client does not print or persist them.

## GetTopic

`GetTopic` validates authentication, topic existence, and subscription
permission. Safe output includes the topic name, schema ID, and
`can_subscribe`. A schema ID contains no event payload and grants no access.

## GetSchema

Events contain an Avro payload and a schema ID. `GetSchema` retrieves the writer
schema JSON needed for decoding. The subscriber caches schemas by ID to avoid
one RPC per event and to support schema evolution during a process lifetime.

The current cache is in memory only. A distributed cache is unnecessary for
this local foundation.

## Usage

```bash
./.venv/bin/python -m src.salesforce_cdc.pubsub_client
```

Expected safe output:

```text
Pub/Sub connection established
Topic: /data/OpportunityChangeEvent
Schema ID: <schema-id>
Can subscribe: True
```

## Known Limitations

- no automatic token refresh for long-running connections;
- no reconnect policy with exponential backoff and jitter;
- no stream deadline or health endpoint;
- no metrics or tracing;
- no production secret manager integration.

These belong to the production phase, not the local foundation.

## Official References

- [Authentication in Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/authentication.html)
- [Pub/Sub API RPC methods](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods)
- [gRPC authentication](https://grpc.io/docs/guides/auth/)