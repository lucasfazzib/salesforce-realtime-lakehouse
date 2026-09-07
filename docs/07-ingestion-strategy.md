# 07 - Batch, Incremental, CDC, or Hybrid

## Starting Point

A common first architecture performs a complete extraction on a schedule:

```text
Salesforce -> Bulk or REST full extract -> files -> lakehouse
```

It is easy to understand and recover, but repeatedly reads, transfers, stores,
and compares unchanged records. Data also remains stale until the next run.

## Strategy Comparison

| Strategy | Latency | Source cost | Deletes and order | Recovery | Complexity |
|---|---:|---:|---|---|---:|
| Full snapshot | hours or days | high | final state only | simple | low |
| `SystemModstamp` incremental | minutes or hours | medium to low | extra handling | medium | medium |
| Pub/Sub CDC | seconds | proportional to changes | explicit semantics | replay-dependent | high |
| Hybrid | seconds plus audit | balanced | strongest coverage | strong | medium to high |

## Full Snapshot

Strengths:

- creates a complete image;
- does not depend on replay IDs;
- supports bootstrap and broad recovery;
- is simple for small objects.

Limitations:

- repeatedly scans mostly unchanged data;
- increases API, network, storage, and compute use;
- makes latency equal to the schedule interval;
- can overlap when volume grows;
- loses intermediate transitions;
- requires separate delete handling;
- can mix states from different moments during a long extraction.

Running full extraction more often does not make it streaming. It repeats the
same expensive operation more frequently.

## Incremental Extraction by SystemModstamp

`SystemModstamp` is generally preferable to `LastModifiedDate` for
synchronization because Salesforce-managed processes can also advance it.

```sql
SELECT Id, Name, StageName, SystemModstamp
FROM Opportunity
WHERE SystemModstamp > :lower_bound
  AND SystemModstamp <= :upper_bound
ORDER BY SystemModstamp, Id
```

Benefits include smaller transfers, familiar batch operations, and time-based
backfills. Risks include boundary gaps, duplicate overlap, equal timestamps,
hard deletes missing from ordinary SOQL, lost intermediate changes, and API
consumption even when no records changed.

### Safe Watermarks

1. choose a fixed `upper_bound` for the run;
2. query from `last_successful_watermark - overlap` through `upper_bound`;
3. finish every page or Bulk API result for that window;
4. write idempotently;
5. advance the watermark only after complete success;
6. deduplicate the overlap by key and version.

The overlap protects against timestamp precision, pagination, delayed commits,
and retries. Its size should be based on observed behavior.

## CDC with Pub/Sub API

CDC provides low latency without constant polling, traffic proportional to
changes, explicit change types, changed-field information, and commit context.

It also introduces long-lived connection management, token refresh, durable
replay state, at-least-once delivery, finite event retention, consumer lag, and
schema evolution. CDC improves latency but does not prove that the lakehouse is
complete.

## Recommended Hybrid Model

```text
Bulk API 2.0 -> initial snapshots, backfills, and reconciliation
Pub/Sub CDC  -> low-latency changes
REST API     -> point queries, metadata, and operational control
```

Each mechanism handles the problem it is designed for.

## Gap-Free Bootstrap

1. start the CDC subscriber and durably buffer incoming events;
2. record the starting point without applying events to final state yet;
3. run the initial Bulk API 2.0 snapshot;
4. load the snapshot into Bronze and Silver;
5. apply buffered events after the snapshot cutoff;
6. deduplicate by event key and order by commit metadata;
7. continue normal incremental processing.

An overlapping snapshot and CDC time window is another valid design, provided
deduplication is explicit. Never leave an unobserved gap between mechanisms.

## Periodic Reconciliation

Even with healthy CDC, reconcile by `SystemModstamp`:

- choose frequency from business risk, such as daily or weekly;
- use an overlapping window;
- compare counts or hashes by suitable partitions;
- repair divergences idempotently;
- include a deleted-record strategy;
- alert when divergence exceeds a threshold.

Less frequent full snapshots can remain useful for audit and disaster recovery
when their cost is acceptable.

## Applying Events

Silver processing should not order only by arrival time. Consider object `Id`,
change type, `commitNumber`, `transactionKey`, `sequenceNumber`, and replay ID
for transport deduplication.

Maintain both representations when useful:

- an immutable change log;
- a current-state table keyed by Salesforce record ID.

## Recommendation for This Project

1. keep CDC as the near-real-time path;
2. add durable landing and replay checkpoints;
3. use Bulk API 2.0 for the initial snapshot;
4. build idempotent Bronze and Silver processing;
5. reconcile Opportunity by `SystemModstamp`;
6. measure lag, completeness, duplicates, and cost;
7. reduce frequent full extracts only after comparing results.

This gradual migration preserves a known recovery path while proving the new
architecture.