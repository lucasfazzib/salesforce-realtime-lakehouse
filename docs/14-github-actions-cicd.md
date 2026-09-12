# 14 - GitHub Actions CI/CD

## Purpose

This phase introduces a simple promotion model for learning CI/CD with
Databricks Asset Bundles:

```text
feature/*
  -> pull request to dev
  -> Python tests and DEV bundle validation
  -> merge
  -> automatic DEV deployment

dev
  -> pull request to main
  -> Python tests and PROD-SIMULATED bundle validation
  -> merge
  -> automatic PROD-SIMULATED deployment
```

No release, hotfix, reconciliation, Gold, or pipeline-run automation is added.

## Branch Model

- `main`: stable, promoted portfolio code;
- `dev`: integration branch and source of promotion pull requests;
- `feature/*`: short-lived development branches targeting `dev`.

Bootstrap the integration branch once after this CI/CD change is reviewed and
merged:

```bash
git switch main
git pull --ff-only
git switch -c dev
git push --set-upstream origin dev
```

Normal feature work then starts from `dev`:

```bash
git switch dev
git pull --ff-only
git switch -c feature/<short-name>
```

## One Workspace, Two Logical Environments

Databricks Free Edition provides one workspace and one metastore. The targets
therefore provide logical isolation only:

| Setting | DEV | PROD-SIMULATED |
|---|---|---|
| DAB target | `dev` | `prod` |
| deployment mode | `development` | `production` |
| catalog | `salesforce_realtime_lakehouse` | same physical catalog |
| Bronze schema | `bronze` | `bronze_prod` |
| Silver schema | `silver` | `silver_prod` |
| Volume | `salesforce_cdc_landing` | `salesforce_cdc_landing_prod` |
| root path | user-scoped `/Workspace/Users/.../dev` | `/Workspace/production/.../prod` |
| Job names | Databricks development prefix | `[PROD-SIMULATED]` prefix |

DEV keeps the existing schema and Volume names to preserve the already
validated data. PROD-SIMULATED receives new names and independent DAB state, so
deployments do not overwrite DEV Jobs or tables.

This is not real production isolation. A real deployment should normally use
separate workspace/account boundaries, service principals, storage policies,
network controls, quotas, and audit controls.

## PROD-SIMULATED Data Setup

Bundle deployment creates Jobs, but the ingestion code expects the managed
Volume to exist before the pipeline is run. Create the logical production data
namespace once in Databricks SQL:

```sql
CREATE SCHEMA IF NOT EXISTS salesforce_realtime_lakehouse.bronze_prod;
CREATE SCHEMA IF NOT EXISTS salesforce_realtime_lakehouse.silver_prod;

CREATE VOLUME IF NOT EXISTS
  salesforce_realtime_lakehouse.bronze_prod.salesforce_cdc_landing_prod;
```

No PROD-SIMULATED deployment or pipeline run was performed locally in this
phase.

## Workflows

### Pull Request CI

`.github/workflows/ci.yml` runs for pull requests targeting `dev` or `main`.

The Python job always:

1. checks out the repository;
2. installs Python 3.12;
3. installs `requirements-dev.txt`;
4. runs `python -m pytest --quiet`.

The authenticated bundle-validation job runs only for branches in this same
repository. GitHub does not expose repository secrets to fork pull requests,
and the workflow deliberately skips authenticated validation for forks.

- PR to `dev`: validates `--target dev`;
- PR to `main`: validates `--target prod`.

CI never deploys resources and never runs Salesforce integration tests.

### DEV Deployment

`.github/workflows/deploy-dev.yml` runs after a push to `dev` and supports
manual dispatch from that branch. It uses the `development` GitHub Environment,
validates the DEV target, and deploys it. It does not run the Lakeflow pipeline.

To run the pipeline manually after deployment:

```bash
databricks bundle run -t dev salesforce_cdc_pipeline
```

### PROD-SIMULATED Deployment

`.github/workflows/deploy-prod.yml` runs after a push to `main` and supports
manual dispatch from that branch. It uses the `production` GitHub Environment,
validates the production-mode target, and deploys it without running data
tasks.

Configure an approval rule on the `production` Environment when the GitHub plan
supports required reviewers. The workflow references the Environment, but
GitHub does not enable approval automatically.

Both manual workflows enforce their expected branch and fail if dispatched from
the wrong ref.

## Authentication Decision

### Preferred Long-Term Model: GitHub OIDC

Databricks supports `github-oidc` when a Databricks account administrator can:

1. create a service principal;
2. create a workload identity federation policy for GitHub Actions;
3. constrain issuer, audience, repository, and GitHub Environment subject;
4. grant the service principal only the required workspace and Unity Catalog
   permissions.

The intended workflow settings are:

```yaml
permissions:
  id-token: write
  contents: read

env:
  DATABRICKS_AUTH_TYPE: github-oidc
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
```

Databricks Free Edition does not provide account-console or account-level API
access. Those capabilities are required to create the service principal
federation policy. OIDC is therefore not practically configurable in the
current account and is not faked in these workflows.

### Current Lab Fallback: PAT

The workflows use Databricks unified authentication with these settings:

| Setting | Source |
|---|---|
| `DATABRICKS_AUTH_TYPE` | literal `pat` |
| `DATABRICKS_HOST` | encrypted GitHub secret |
| `DATABRICKS_TOKEN` | encrypted GitHub secret |

Create a short-lived PAT through the Databricks UI or CLI and place it directly
in GitHub encrypted secrets. Never paste it into YAML, source code, an issue, or
documentation. Rotate or revoke it after the lab or whenever exposure is
suspected.

This is a learning fallback, not the recommended long-term production model.

## GitHub Setup

### 1. Create Environments

In repository **Settings > Environments**, create:

- `development`;
- `production`.

Optionally configure required reviewers on `production`.

### 2. Configure Repository Secrets for PR Validation

In **Settings > Secrets and variables > Actions**, create repository secrets:

- `DATABRICKS_HOST`;
- `DATABRICKS_TOKEN`.

These are used only by authenticated bundle validation for same-repository pull
requests. External fork pull requests receive neither secret.

### 3. Configure Environment Secrets for Deployments

Create the same two secrets independently in both GitHub Environments:

- `development`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`;
- `production`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.

The host is not confidential, but storing both fallback settings together as
Environment secrets keeps the workflow configuration uniform. Environment
secrets override repository secrets for deployment jobs.

No GitHub Actions variables are required by the current PAT fallback.

When OIDC becomes available, replace the token secret with the environment
variables `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID`, grant `id-token: write`,
and change `DATABRICKS_AUTH_TYPE` to `github-oidc`.

## Branch Protection and Rulesets

Recommended `main` rules:

- require a pull request before merging;
- require the CI Python and bundle-validation checks;
- block direct pushes;
- require the branch to be up to date before merge;
- optionally require review from another maintainer.

Recommended `dev` rules:

- require pull requests for `feature/*` integration;
- require CI when practical;
- allow normal merge-based integration;
- block force pushes and branch deletion.

Configure these settings manually in GitHub. They are not changed by this
repository patch.

## Local Validation

```bash
./.venv/bin/python -m pip install --requirement requirements-dev.txt
./.venv/bin/python -m pytest --quiet
databricks bundle validate -t dev
databricks bundle validate -t prod
```

Resolved target isolation can be reviewed with:

```bash
databricks bundle validate -t dev --output json
databricks bundle validate -t prod --output json
```

Do not deploy PROD-SIMULATED locally as part of ordinary validation.

## End-to-End GitHub Validation

After environments, secrets, and the `dev` branch exist:

1. create `feature/test-cicd` from `dev`;
2. open a pull request to `dev` and verify CI passes;
3. merge and verify `Deploy DEV` succeeds;
4. open a pull request from `dev` to `main` and verify CI validates `prod`;
5. merge and verify `Deploy PROD-SIMULATED` succeeds after any configured
   approval.

Confirm in Databricks that DEV Jobs retain their `[dev <user>]` prefix and the
production target uses `[PROD-SIMULATED]`, with separate bundle root paths.

## Security Notes

- no token, OAuth response, `.env`, or `.databrickscfg` belongs in Git;
- never print GitHub secrets during troubleshooting;
- use the shortest practical PAT lifetime;
- restrict the PAT owner to required workspace and Unity Catalog privileges;
- do not enable deployment workflows for untrusted branches;
- migrate to workload identity federation when using a Databricks account tier
  that supports the required account administration.