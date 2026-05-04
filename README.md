# Globant Data Engineering Challenge — GCP Implementation

End-to-end implementation of both parts of the Globant data-engineering coding
challenge on **Google Cloud Platform**. 

---

## What's in this repo

```
challange1/
├── historical_load/   one-shot CSV → BigQuery migration (SQL scripts)
├── api/               Cloud Run service: row-batch insert API + restore endpoint
└── backup/            Cloud Run job: per-table AVRO snapshots to GCS
challange2/            Cloud Run service: analytical SQL endpoints (Flask)
raw_data/              source CSVs (hired_employees, departments, jobs)
```

**Challenge 1** — historic CSV migration into BigQuery, REST API for
batch-validated row inserts, weekly AVRO backup of every table, on-demand
restore endpoint.

**Challenge 2** — two read-only analytical SQL endpoints over the same dataset
(quarterly hires by department/job; departments above the 2021 hiring mean).

---

## Prerequisites

Install on your workstation:
- **gcloud CLI** — <https://cloud.google.com/sdk/docs/install>
- **Python 3.12+** (the API uses 3.12, Challenge 2 uses 3.13; both work)
- **Docker** is *not* required — Cloud Build does the container builds.
- A GCP project with **billing enabled**.

---

## 1. One-time GCP project setup

The default project ID throughout this repo is `dataengineer-494617`. If you
fork to a new project, search-and-replace that ID in:
- `challange1/historical_load/*.sql`
- `challange1/api/bq_writer.py`, `challange1/api/restore.py`
- `challange2/service.py`

```bash
# Replace with your project / region / bucket name
export PROJECT_ID=dataengineer-494617
export REGION=us-central1
export BUCKET=hr-poc-data
export DATASET=hr_poc

# Authenticate the CLI and pick the project
gcloud auth login
gcloud config set project $PROJECT_ID

# Authenticate Python client libraries (writes Application Default Credentials)
gcloud auth application-default login

# Enable every API used by either challenge
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com
```

Create the GCS bucket that holds raw CSVs and AVRO backups:

```bash
gcloud storage buckets create gs://$BUCKET \
  --location=$REGION \
  --uniform-bucket-level-access
```

---

## 2. Historical migration (Challenge 1)

Loads the three source CSVs into BigQuery using a **stage → validate → publish**
pattern: every row lands in a STRING-typed staging table first, then SAFE_CAST
filters split valid rows into the typed final tables and bad rows into a
`load_reject` audit table.

### 2a. Upload the source CSVs

```bash
gcloud storage cp raw_data/hired_employees.csv gs://$BUCKET/raw/
gcloud storage cp raw_data/departments.csv     gs://$BUCKET/raw/
gcloud storage cp raw_data/jobs.csv            gs://$BUCKET/raw/
```

### 2b. Create the BigQuery dataset and tables

```bash
bq --location=US mk --dataset $PROJECT_ID:$DATASET

bq query --use_legacy_sql=false \
  < challange1/historical_load/bq_tables.sql
```

This creates:
- **Final** tables: `hired_employees`, `departments`, `jobs` (typed, NOT NULL).
- **Staging** tables: same names + `_staging` suffix (all STRING).
- **`load_reject`**: audit table for any row that fails validation.

### 2c. Load CSVs into staging, then validate into final

```bash
bq query --use_legacy_sql=false \
  < challange1/historical_load/load_job.sql

bq query --use_legacy_sql=false \
  < challange1/historical_load/validate.sql
```

`load_job.sql` uses `LOAD DATA OVERWRITE` to land raw CSV bytes into staging.
`validate.sql` truncates the final tables, then `SAFE_CAST`s rows from staging
— valid rows go to the final tables, invalid rows go to `load_reject` with a
human-readable rejection reason.

Sanity check:
```bash
bq query --use_legacy_sql=false \
  "SELECT 'hired_employees', COUNT(*) FROM \`$PROJECT_ID.$DATASET.hired_employees\` UNION ALL
   SELECT 'departments',     COUNT(*) FROM \`$PROJECT_ID.$DATASET.departments\` UNION ALL
   SELECT 'jobs',            COUNT(*) FROM \`$PROJECT_ID.$DATASET.jobs\` UNION ALL
   SELECT 'load_reject',     COUNT(*) FROM \`$PROJECT_ID.$DATASET.load_reject\`"
```

---

## 3. Challenge 1 — API service (batch insert + restore)

A FastAPI service on Cloud Run. Three `POST /insert/{table}` endpoints accept
1–1000 rows, validate via Pydantic, write valid rows via the **BigQuery Storage
Write API** (proto-encoded, default stream), and stream invalid rows to
`load_reject`. A `POST /restore/{table}?run_id=...` endpoint repopulates a
table from a previously-taken AVRO backup.

### 3a. Create the runtime service account

```bash
gcloud iam service-accounts create globant-api-sa \
  --display-name="Globant Challenge 1 API"

SA=globant-api-sa@$PROJECT_ID.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/storage.objectViewer"
```

`dataEditor` lets the API write to `load_reject` and the final tables;
`jobUser` lets the restore endpoint create load jobs; `objectViewer` lets the
restore endpoint read AVRO files from GCS.

### 3b. Deploy

```bash
cd challange1/api

gcloud run deploy globant-api \
  --source=. \
  --region=$REGION \
  --service-account=$SA \
  --no-allow-unauthenticated
```

Cloud Build packages the source, builds the container from the Dockerfile,
pushes to Artifact Registry, and creates a new Cloud Run revision. First deploy
takes ~3-5 minutes.

### 3c. Grant yourself permission to call it

```bash
gcloud run services add-iam-policy-binding globant-api \
  --region=$REGION \
  --member="user:YOUR_EMAIL@example.com" \
  --role="roles/run.invoker"
```

---

## 4. Challenge 1 — backup job + scheduler

Per-table BQ → AVRO export, written to `gs://$BUCKET/backups/{table}/{run_id}/`,
triggered weekly by Cloud Scheduler.

### 4a. Backup runtime service account

```bash
gcloud iam service-accounts create globant-backup-sa \
  --display-name="Globant Challenge 1 Backup"

BACKUP_SA=globant-backup-sa@$PROJECT_ID.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$BACKUP_SA" --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$BACKUP_SA" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$BACKUP_SA" --role="roles/storage.objectCreator"
```

### 4b. Deploy the Job

```bash
cd challange1/backup

gcloud run jobs deploy globant-backup \
  --source=. \
  --region=$REGION \
  --service-account=$BACKUP_SA \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET_ID=$DATASET,BACKUP_BUCKET=$BUCKET \
  --task-timeout=900
```

Manual one-off run (smoke test):
```bash
gcloud run jobs execute globant-backup --region=$REGION --wait
```

### 4c. Cloud Scheduler — weekly trigger

```bash
gcloud iam service-accounts create globant-scheduler-sa \
  --display-name="Globant Scheduler Invoker"

SCHED_SA=globant-scheduler-sa@$PROJECT_ID.iam.gserviceaccount.com

# Scope the invoker role to the backup Job ONLY
gcloud run jobs add-iam-policy-binding globant-backup \
  --region=$REGION \
  --member="serviceAccount:$SCHED_SA" \
  --role="roles/run.invoker"

gcloud scheduler jobs create http globant-backup-daily \
  --location=$REGION \
  --schedule="0 6 * * 1" \
  --time-zone="UTC" \
  --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/globant-backup:run" \
  --http-method=POST \
  --oauth-service-account-email=$SCHED_SA
```

`0 6 * * 1` = 06:00 UTC every Monday. Cloud Scheduler mints an OAuth token from
`globant-scheduler-sa` and calls the Job's `:run` endpoint.

---

## 5. Challenge 2 — analytical endpoints

A small Flask app on Cloud Run with two read-only `GET` endpoints that run
parameterless aggregations against the BigQuery dataset.

### 5a. Runtime service account (read-only)

```bash
gcloud iam service-accounts create globant-api2-sa \
  --display-name="Globant Challenge 2 API"

SA2=globant-api2-sa@$PROJECT_ID.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA2" --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA2" --role="roles/bigquery.jobUser"
```

Read-only — `dataViewer` not `dataEditor`. No GCS or other resource access.

### 5b. Deploy

```bash
cd challange2

gcloud run deploy globant-api2 \
  --source=. \
  --region=$REGION \
  --service-account=$SA2 \
  --no-allow-unauthenticated
```

### 5c. Grant yourself invoker

```bash
gcloud run services add-iam-policy-binding globant-api2 \
  --region=$REGION \
  --member="user:YOUR_EMAIL@example.com" \
  --role="roles/run.invoker"
```

---

## 6. Calling the deployed services

Both services are IAM-protected (no public access). Every request needs a
short-lived identity token in the `Authorization` header.

```bash
TOKEN=$(gcloud auth print-identity-token)

# Service URLs
API1_URL=$(gcloud run services describe globant-api  --region=$REGION --format='value(status.url)')
API2_URL=$(gcloud run services describe globant-api2 --region=$REGION --format='value(status.url)')
```

### Challenge 1 — insert a batch of rows

```bash
curl -X POST "$API1_URL/insert/departments" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"rows":[{"id":999,"department":"Test Department"}]}'
```

Response:
```json
{ "accepted": 1, "rejected": 0, "rejects": [] }
```

Same shape for `/insert/hired_employees` and `/insert/jobs`. Batches up to 1000
rows. Validation failures appear in the `rejects` array AND are persisted to
`hr_poc.load_reject` for auditing.

### Challenge 1 — restore a table from backup

```bash
# Restore using the latest backup
curl -X POST "$API1_URL/restore/departments" \
  -H "Authorization: Bearer $TOKEN"

# Or pin a specific run_id
curl -X POST "$API1_URL/restore/departments?run_id=2026-04-30T06-00-00Z" \
  -H "Authorization: Bearer $TOKEN"
```

`run_id` corresponds to a folder under `gs://$BUCKET/backups/{table}/`.
The restore performs `WRITE_TRUNCATE` — the live table is replaced atomically
with the AVRO contents.

### Challenge 2 — analytical queries

```bash
curl -H "Authorization: Bearer $TOKEN" "$API2_URL/hires-by-quarter"
curl -H "Authorization: Bearer $TOKEN" "$API2_URL/depts-above-mean"
```

Returns JSON arrays:
- **`/hires-by-quarter`** → one row per `(department, job)` with q1–q4 hire counts for 2021, sorted alphabetically.
- **`/depts-above-mean`** → departments whose 2021 hire count exceeded the mean across all departments, sorted descending by count.

---

## Operational notes

- **IAM propagation** — bindings can take 1–2 minutes to apply globally.
  If you get a 403 immediately after granting a role, wait and retry.
- **Cold starts** — the first request after idle takes 2–5 seconds while
  Cloud Run spins a container instance. Subsequent calls are fast.
- **Costs at idle** — Cloud Run scales to zero. No traffic = no cost.
  Cloud Scheduler charges flat $0.10/job/month. BigQuery storage is the only
  other recurring cost (~$0.02/GB/month for active storage).
- **Logs** — `gcloud run services logs tail globant-api2 --region=$REGION`
  for live tailing, or use the Cloud Console (Cloud Run → service → Logs tab).
- **Local dev** — see `GCP_Commands_Reference.pdf` for Flask dev server and
  gunicorn invocations that mirror the production runtime.
