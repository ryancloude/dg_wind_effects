```md
# DG Wind Effects (`dg_wind_effects`)

Production-style AWS data pipeline for disc golf analytics and wind-impact modeling.

## Current status

Implemented:
- Bronze ingestion:
  - `ingest_pdga_event_pages`
  - `ingest_pdga_live_results`
- Silver normalization:
  - `silver_pdga_live_results`
  - outputs `player_rounds` and `player_holes` parquet tables

Planned:
- Gold features and wind-effect modeling outputs

## Repository structure

```text
dg_wind_effects/
  ingest_pdga_event_pages/
  ingest_pdga_live_results/
  silver_pdga_live_results/
  tests/
  docs/
  Dockerfile.event_pages
  Dockerfile.live_results
  Dockerfile.silver_live_results
  pyproject.toml
  requirements.lock
```

## Environment variables

Required:
- `PDGA_S3_BUCKET`
- `PDGA_DDB_TABLE`

Optional:
- `AWS_REGION`
- `PDGA_DDB_STATUS_END_DATE_GSI` (default `gsi_status_end_date`)

Example `.env`:

```dotenv
PDGA_S3_BUCKET=my-pdga-bucket
PDGA_DDB_TABLE=pdga-event-index
AWS_REGION=us-east-2
PDGA_DDB_STATUS_END_DATE_GSI=gsi_status_end_date
```

## Local setup (PowerShell)

```powershell
cd C:\Users\ryanc\dg_wind_effects
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
pip install -r .\requirements.lock
```

## Bronze runners

Event pages:

```powershell
python -m ingest_pdga_event_pages.runner --incremental
python -m ingest_pdga_event_pages.runner --ids 100001,100002
python -m ingest_pdga_event_pages.runner --range 100000-100050
python -m ingest_pdga_event_pages.runner --backfill-start-id 90000
```

Live results:

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill
python -m ingest_pdga_live_results.runner --event-ids 92608,92612
python -m ingest_pdga_live_results.runner --historical-backfill --dry-run
python -m ingest_pdga_live_results.backfill_live_results_ingested_flag
```

## Silver runner (`silver_pdga_live_results`)

### What Silver does
- Selects finalized events with `live_results_ingested=true`
- Resolves Bronze round JSON payloads per event/division/round
- Normalizes to:
  - `player_rounds` (player-round grain)
  - `player_holes` (player-hole grain)
- Dedups deterministically by logical keys
- Enforces DQ checks and writes quarantine reports on DQ failure
- Writes event-level deterministic parquet outputs
- Tracks event checkpoints + run summaries in DynamoDB
- Computes deterministic `round_date_interp` fields for both tables

### Incremental selection and reruns
`--run-mode pending_only` (default):
- includes events with missing checkpoint
- includes events with `failed`
- excludes `success` with fingerprint
- excludes `dq_failed` by default
- include `dq_failed` explicitly with `--include-dq-failed-in-pending`

`--run-mode full_check`:
- evaluates all candidate events

`--force-events`:
- disables unchanged-fingerprint skip for selected events

### Silver run examples

Dry run:

```powershell
python -m silver_pdga_live_results.runner --dry-run --log-level INFO
```

Default incremental run:

```powershell
python -m silver_pdga_live_results.runner --run-mode pending_only --progress-every 25
```

Retry including prior DQ failures:

```powershell
python -m silver_pdga_live_results.runner --run-mode pending_only --include-dq-failed-in-pending --progress-every 25
```

Full reprocess (historical Silver re-backfill):

```powershell
python -m silver_pdga_live_results.runner --run-mode full_check --force-events --progress-every 25 --log-level INFO
```

Specific events:

```powershell
python -m silver_pdga_live_results.runner --event-ids 90008,90009 --force-events --log-level INFO
```

Console entrypoint equivalent:

```powershell
plan-silver-live-results --run-mode full_check --force-events --progress-every 25
```

## Docker usage

Build:

```powershell
docker build -f Dockerfile.event_pages -t dg_event_pages:dev .
docker build -f Dockerfile.live_results -t dg_live_results:dev .
docker build -f Dockerfile.silver_live_results -t dg_silver_live_results:dev .
```

Run:

```powershell
docker run --rm --env-file .env dg_event_pages:dev --incremental
docker run --rm --env-file .env dg_live_results:dev --historical-backfill
docker run --rm --env-file .env dg_silver_live_results:dev --run-mode pending_only --progress-every 25
```

## Storage contracts

Bronze S3:

```text
bronze/pdga/event_page/event_id=<event_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.html.gz
bronze/pdga/event_page/event_id=<event_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.meta.json
bronze/pdga/live_results/event_id=<event_id>/division=<division>/round=<round>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.json
bronze/pdga/live_results/event_id=<event_id>/division=<division>/round=<round>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.meta.json
```

Silver S3:

```text
silver/pdga/live_results/player_rounds/event_year=<YYYY>/tourn_id=<event_id>/player_rounds.parquet
silver/pdga/live_results/player_holes/event_year=<YYYY>/tourn_id=<event_id>/player_holes.parquet
silver/pdga/live_results/quarantine/event_id=<event_id>/run_id=<run_id>/dq_errors.json
```

## Testing

All tests:

```powershell
python -m pytest -v
```
## Documentation index
- `docs/silver_live_results_schema.md`
- `docs/silver_live_results_code_walkthrough.md`
- `docs/dynamodb_data_model.md`
- `docs/ingest_pdga_event_pages_code_walkthrough.md`
- `docs/ingest_pdga_live_results_code_walkthrough.md`
```