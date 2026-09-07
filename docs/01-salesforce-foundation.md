# 01 - Salesforce Foundation

## Goal

Establish a working Salesforce source before introducing streaming. The order
matters: authenticate first, validate REST access next, and enable CDC last.

## 1. OAuth Client Credentials

The Client Credentials flow is suitable for machine-to-machine integration. A
Connected App or External Client App represents the application and runs as a
dedicated Salesforce integration user.

Local configuration uses environment variables:

```dotenv
SALESFORCE_LOGIN_URL=https://login.salesforce.com
SALESFORCE_CLIENT_ID=
SALESFORCE_CLIENT_SECRET=
SALESFORCE_ORG_ID=
```

`src/salesforce_cdc/auth.py` sends the client credentials to the OAuth endpoint
and returns the session response. Callers keep `access_token` and
`instance_url` in memory only.

### What Was Validated

- the application accepts the Client Credentials flow;
- the integration user can obtain a session;
- code does not print access tokens;
- credentials come from environment variables;
- `.env` is ignored by Git.

### Security Notes

- an access token is a temporary secret;
- errors must not include HTTP headers or gRPC metadata;
- the integration user should have least privilege;
- secret rotation must not require a code change.

## 2. REST API and SOQL

`src/salesforce_cdc/rest_client.py` reuses the authentication flow to query
Opportunity. This proves connectivity, object permissions, and field access
before testing CDC.

The validation query reads a small, ordered result set:

```sql
SELECT Id, Name, StageName, Amount, CloseDate, LastModifiedDate
FROM Opportunity
ORDER BY LastModifiedDate DESC
LIMIT 20
```

REST remains useful after adopting CDC for point lookups, metadata, controlled
enrichment, diagnostics, and small reconciliations. It should not be the main
mechanism for large snapshots; Bulk API 2.0 is more appropriate for volume.

## 3. Change Data Capture

CDC was enabled for Opportunity. Committed changes are published to:

```text
/data/OpportunityChangeEvent
```

A change event is not merely a copy of a row. Its `ChangeEventHeader` includes:

- `recordIds`;
- `changeType`, such as `CREATE`, `UPDATE`, `DELETE`, or `UNDELETE`;
- `changedFields`, encoded as a bitmap;
- `commitTimestamp`;
- `transactionKey`, `sequenceNumber`, and `commitNumber`.

## Result

The test Opportunity was queried through REST, and changing its `StageName`
produced a CDC event that was received and decoded locally. Real identifiers
and values are intentionally absent from this documentation.

## Official References

- [OAuth Client Credentials Flow](https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_client_credentials_flow.htm&type=5)
- [Change Data Capture Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_intro.htm)
- [REST API Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm)
- [Bulk API 2.0 Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0.htm)