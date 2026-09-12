# 13 - Lakeflow Orchestration and DELETE Validation

## Architecture Choice

The project keeps the standalone Bronze and Silver Jobs for focused manual
testing and debugging. A third DAB resource is the primary orchestration path:

```text
salesforce_cdc_pipeline
  ingest_opportunity_cdc
    -> merge_opportunity_current_state
```

This is Option A: standalone resources remain useful learning surfaces, while
the orchestrated Job represents the production-style dependency graph. The
Python transformation code is not duplicated; all three Jobs call the same
`bronze_ingestion.py` and `silver_opportunity.py` files.

## DAB Resource

`resources/lakeflow_job.yml` defines:

- resource key `salesforce_cdc_pipeline`;
- Bronze task `ingest_opportunity_cdc`;
- Silver task `merge_opportunity_current_state`;
- an explicit Silver `depends_on` reference to the Bronze task;
- the same catalog, schema, Volume, and table variables as the standalone Jobs;
- one shared serverless environment definition.

If Bronze fails, the default `ALL_SUCCESS` dependency condition prevents Silver
from starting.

## Deploy and Run

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev salesforce_cdc_pipeline
```

The deployed workflow was executed successfully. Databricks Jobs run metadata
confirmed that Bronze completed with `SUCCESS` before Silver started, and
Silver also completed with `SUCCESS`.

## Exact DELETE Validation Procedure

1. Confirm the test Opportunity currently exists in Silver with
   `is_deleted = false`.
2. Start the local subscriber:

   ```bash
   ./.venv/bin/python -u -m src.salesforce_cdc.subscriber
   ```

3. Delete the test Opportunity in Salesforce.
4. Confirm the terminal reports `Change type: DELETE`.
5. Confirm a new JSON file exists under
   `raw_events/opportunity/YYYY/MM/DD/`.
6. Upload local events:

   ```bash
   ./.venv/bin/python -m src.salesforce_cdc.databricks_upload
   ```

7. Run the orchestrated workflow:

   ```bash
   databricks bundle run -t dev salesforce_cdc_pipeline
   ```

8. Query Bronze and Silver using the SQL below.

Salesforce replay retention is finite, so the subscriber should not remain
offline for an extended period before this test.

## Bronze Validation SQL

```sql
SELECT
  change_type,
  commit_timestamp,
  changed_fields,
  _source_file,
  _ingested_at
FROM salesforce_realtime_lakehouse.bronze.opportunity_cdc
WHERE change_type = 'DELETE'
ORDER BY commit_timestamp DESC;
```

## Silver DELETE Validation SQL

```sql
SELECT
  opportunity_id,
  name,
  stage_name,
  amount,
  close_date,
  is_deleted,
  deleted_at,
  last_change_type,
  last_commit_timestamp,
  updated_at
FROM salesforce_realtime_lakehouse.silver.opportunity
WHERE is_deleted = true
ORDER BY deleted_at DESC;
```

Expected result:

- the Opportunity row still exists;
- `is_deleted = true`;
- `deleted_at` is populated;
- `last_change_type = 'DELETE'`;
- `last_commit_timestamp = deleted_at`;
- Name, StageName, Amount, and CloseDate remain at their latest values before
  the DELETE.

## Validation Result

A real Opportunity DELETE was received locally, persisted, uploaded, and
processed through the orchestrated workflow. Validation compared non-reversible
hashes of the Silver business columns with the latest pre-delete Bronze change
for each field. All business values were preserved, the soft-delete flags were
correct, and operational metadata advanced to the DELETE event.

No transformation code change was required for DELETE semantics.