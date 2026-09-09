# 12 - Opportunity Silver Current State

## Goal

Maintain one current known row per Salesforce Opportunity from immutable Bronze
CDC history:

```text
bronze.opportunity_cdc
  -> normalize events
  -> select the latest change per field
  -> select the latest event per Opportunity
  -> Delta MERGE
  -> silver.opportunity
```

This phase does not implement reconciliation, business history, Gold, or dbt.

## Actual Bronze Findings

The implementation was based on the existing Bronze table rather than an
assumed Salesforce schema.

- Opportunity ID is provided by the top-level `record_ids` array.
- The payload does not expose a top-level `Id` in the observed CDC events.
- `Name`, `StageName`, `Amount`, `CloseDate`, and `LastModifiedDate` exist as
  payload keys.
- UPDATE payloads can retain keys with null values for unchanged fields.
- `changed_fields` is therefore the authority for applying field changes.
- `ChangeEventHeader` contains `commitNumber`, `sequenceNumber`, and
  `transactionKey` in addition to `commitTimestamp`.

The initial Bronze history contained UPDATE events rather than a CREATE event.
Consequently, fields never observed as changed remain null in Silver until a
future event provides their current known value. A later snapshot/reconciliation
phase will fill this historical bootstrap gap.

## Target Table

```text
salesforce_realtime_lakehouse.silver.opportunity
```

Business columns:

- `opportunity_id`;
- `name`;
- `stage_name`;
- `amount`;
- `close_date`;
- `last_modified_date`.

Operational columns:

- `is_deleted` and `deleted_at`;
- `last_change_type`;
- `last_commit_timestamp`;
- `last_commit_number`;
- `last_sequence_number`;
- `last_transaction_key`;
- `last_replay_id`;
- `updated_at`.

## Partial UPDATE Semantics

For each business field, the Spark transformation creates a nullable update
marker:

```text
struct(is_set = true, value = decoded Salesforce value)
```

The struct exists only when the event is CREATE or `changed_fields` contains
that Salesforce field name. A window ordered newest-first selects the first
non-null update struct per Opportunity and field.

This distinction is important:

- absent struct means the field was not changed and must be preserved;
- present struct with a null value means Salesforce explicitly changed the
  field to null and Silver must apply null.

The Delta MERGE uses `has_<field>_change` flags:

```sql
target.stage_name = CASE
  WHEN source.has_stage_name_change THEN source.stage_name
  ELSE target.stage_name
END
```

No rows are collected to the driver and no row-by-row Python updates are used.

## Ordering and Deduplication

Events are ordered per Opportunity by:

1. `commit_timestamp` descending;
2. `commitNumber` descending;
3. `sequenceNumber` descending;
4. Bronze `_ingested_at` descending;
5. `replay_id` descending as a deterministic final tie-breaker.

MERGE updates an existing row only when the incoming current-state event is
newer by commit timestamp, commit number, or sequence number. Rerunning against
unchanged Bronze history does not mutate the target row or its `updated_at`.

The job currently scans the available Bronze history to reconstruct each
field's latest known value. This is simple and deterministic for the current
small dataset. Incremental Silver state/checkpoint optimization belongs to a
later scaling phase.

## CREATE and Replay Behavior

A CREATE event treats every modeled business field as available, even if its
name is not listed in `changed_fields`. MERGE inserts a row when the Opportunity
does not exist. If replayed Bronze input produces the same latest commit
metadata, the matched row is not updated.

## Soft DELETE

DELETE uses a soft-delete strategy:

- `is_deleted = true`;
- `deleted_at = commit_timestamp`;
- prior business values remain available;
- operational metadata advances to the DELETE event.

UNDELETE or a newer non-delete event sets `is_deleted = false` and clears
`deleted_at`. The code path is implemented, but a real Salesforce DELETE was
not performed in this phase and remains explicitly unvalidated.

## DAB Resource

`resources/silver_job.yml` defines a separate serverless Lakeflow Job:

```bash
databricks bundle run -t dev silver_opportunity
```

It receives DAB variables for catalog, Bronze schema/table, and Silver
schema/table. It is intentionally separate from the Bronze Job.

## Validation Completed

- DAB validation and deployment succeeded.
- The Silver Job completed successfully.
- The target contains one distinct row for the observed Opportunity.
- A StageName test changed the modeled StageName value and preserved Name,
  Amount, and CloseDate; Salesforce also emitted derived system-field changes.
- An Amount event produced the latest Amount while StageName remained the value
  from the latest StageName event.
- A rerun without new Bronze events left both row count and state unchanged.

Comparisons used non-reversible hashes and booleans so business values were not
printed during validation.

## Validation SQL

```sql
SELECT
  opportunity_id,
  name,
  stage_name,
  amount,
  close_date,
  last_modified_date,
  is_deleted,
  last_change_type,
  last_commit_timestamp,
  updated_at
FROM salesforce_realtime_lakehouse.silver.opportunity
ORDER BY updated_at DESC;
```

Check the one-row-per-ID invariant:

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT opportunity_id) AS distinct_opportunities
FROM salesforce_realtime_lakehouse.silver.opportunity;
```

## Known Limitations

- no initial Salesforce snapshot exists, so never-observed fields may be null;
- DELETE behavior is implemented but not tested against a real delete;
- the Job rebuilds current source state from all available Bronze events;
- only six Opportunity business fields are modeled;
- no reconciliation or data quality contract exists yet.