1. Replace the current `README.md` with a clean project-level README (the current one still contains generated wrapper text and an outdated “Bronze only” scope statement).
2. Include current Bronze + Silver behavior, run commands, storage contracts, and links to the docs you just added.
3. Keep commands copy/paste-ready for local and Docker execution.

### Full `README.md`
```md
# DG Wind Effects

Production-style data pipeline project for estimating the effect of wind on disc golf scoring.

Current implemented layers:
- Bronze:
  - `ingest_pdga_event_pages`
  - `ingest_pdga_live_results`
- Silver:
  - `silver_pdga_live_results` (player-round and player-hole tables)

Planned next layers:
- Gold analytics features and wind-impact modeling

## Repository Structure

```text
dg_wind_effects/
  ingest_pdga_event_pages/
  ingest_pdga_live_results/
  silver_pdga_live_results/
  tests/
    ingest_pdga_event_pages/
    ingest_pdga_live_results/
    silver_pdga_live_results/
  docs/
  docker/
  infra/
  pyproject.toml
  requirements.lock
  Dockerfile.event_pages
  Dockerfile.live_results
  Dockerfile.silver_live_results
```

## Environment Variables

Required:
- `PDGA_S3_BUCKET`
- `PDGA_DDB_TABLE`

Optional:
- `AWS_REGION`
- `PDGA_DDB_STATUS_END_DATE_GSI` (default: `gsi_status_end_date`)

Example `.env`:
```dotenv
PDGA_S3_BUCKET=my-pdga-bucket
PDGA_DDB_TABLE=pdga-event-index
AWS_REGION=us-east-2
PDGA_DDB_STATUS_END_DATE_GSI=gsi_status_end_date
```

## Local Setup

```powershell
cd C:\Users\ryanc\dg_wind_effects
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
pip install -r .\requirements.lock
```

## Bronze: Event Page Ingest (`ingest_pdga_event_pages`)

### Run modes

Explicit IDs:
```powershell
python -m ingest_pdga_event_pages.runner --ids 100001,100002
```

Inclusive range:
```powershell
python -m ingest_pdga_event_pages.runner --range 100000-100050
```

Forward backfill:
```powershell
python -m ingest_pdga_event_pages.runner --backfill-start-id 90000
```

Incremental mode:
```powershell
python -m ingest_pdga_event_pages.runner --incremental
```

Dry run:
```powershell
python -m ingest_pdga_event_pages.runner --incremental --dry-run --log-level INFO
```

### Incremental controls

Override statuses:
```powershell
python -m ingest_pdga_event_pages.runner --incremental --incremental-statuses "Sanctioned,Errata pending."
```

Override window:
```powershell
python -m ingest_pdga_event_pages.runner --incremental --incremental-window-days 183
```

Forward-scan stop controls:
```powershell
python -m ingest_pdga_event_pages.runner --incremental --backfill-stop-after-unscheduled 5 --backfill-max-event-id 102500
```

## Bronze: Live Results Ingest (`ingest_pdga_live_results`)

### Run modes

Explicit IDs:
```powershell
python -m ingest_pdga_live_results.runner --event-ids 92608,92612
```

Event IDs from S3 file:
```powershell
python -m ingest_pdga_live_results.runner --event-ids-s3-uri s3://my-bucket/path/event_ids.csv
```

Historical backfill:
```powershell
python -m ingest_pdga_live_results.runner --historical-backfill
```

Dry run:
```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --dry-run
```

Throughput/progress controls:
```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --sleep-base 0.7 --sleep-jitter 0.5 --progress-every 50
```

Override excluded statuses:
```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --historical-excluded-statuses "Sanctioned,In progress."
```

Backfill existing `live_results_ingested` markers:
```powershell
python -m ingest_pdga_live_results.backfill_live_results_ingested_flag
```

## Silver: Live Results Normalization (`silver_pdga_live_results`)

Builds:
- `player_rounds` (grain: player-round-event)
- `player_holes` (grain: player-hole-round-event)

Key properties:
- Incremental via event fingerprint checkpoints
- Default fast resume mode (`--run-mode pending_only`)
- Idempotent event-level overwrite
- Deterministic player key fallback (`PDGA -> ResultID -> NAMEHASH`)
- DQ gates with quarantine on failure
- Supports round-only events (no hole detail available)

### Run

Dry run:
```powershell
python -m silver_pdga_live_results.runner --dry-run --log-level INFO
```

Specific events:
```powershell
python -m silver_pdga_live_results.runner --event-ids 90008,90009 --log-level INFO
```

Force reprocess:
```powershell
python -m silver_pdga_live_results.runner --event-ids 90008 --force-events --log-level INFO
```

Console script equivalent:
```powershell
plan-silver-live-results --dry-run --log-level INFO
```

## Docker Runs

Build images:
```powershell
docker build -f Dockerfile.event_pages -t dg_event_pages:dev .
docker build -f Dockerfile.live_results -t dg_live_results:dev .
docker build -f Dockerfile.silver_live_results -t dg_silver_live_results:dev .
```

Run:
```powershell
docker run --rm --env-file .env dg_event_pages:dev --incremental
docker run --rm --env-file .env dg_live_results:dev --historical-backfill
docker run --rm --env-file .env dg_silver_live_results:dev --dry-run --log-level INFO
```

## Storage Contracts

### S3 Bronze

Event pages:
```text
bronze/pdga/event_page/event_id=<event_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.html.gz
bronze/pdga/event_page/event_id=<event_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.meta.json
```

Live results:
```text
bronze/pdga/live_results/event_id=<event_id>/division=<division>/round=<round>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.json
bronze/pdga/live_results/event_id=<event_id>/division=<division>/round=<round>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.meta.json
```

### S3 Silver

Final deterministic event keys:
```text
silver/pdga/live_results/player_rounds/event_year=<YYYY>/tourn_id=<event_id>/player_rounds.parquet
silver/pdga/live_results/player_holes/event_year=<YYYY>/tourn_id=<event_id>/player_holes.parquet
```

Quarantine:
```text
silver/pdga/live_results/quarantine/event_id=<event_id>/run_id=<run_id>/dq_errors.json
```

### DynamoDB (single table model)

See full reference in:
- `docs/dynamodb_data_model.md`

Core item families:
- Event metadata:
  - `pk=EVENT#<event_id>`, `sk=METADATA`
- Live results state:
  - `pk=EVENT#<event_id>`, `sk=LIVE_RESULTS#DIV#<division>#ROUND#<round>`
- Bronze run summary:
  - `pk=RUN#<run_id>`, `sk=LIVE_RESULTS#SUMMARY`
- Silver event checkpoint:
  - `pk=PIPELINE#SILVER_LIVE_RESULTS`, `sk=EVENT#<event_id>`
- Silver run summary:
  - `pk=RUN#<run_id>`, `sk=SILVER_LIVE_RESULTS#SUMMARY`

## Testing

All tests:
```powershell
python -m pytest -v
```

By layer:
```powershell
python -m pytest tests/ingest_pdga_event_pages -v
python -m pytest tests/ingest_pdga_live_results -v
python -m pytest tests/silver_pdga_live_results -v
```

Focused regression:
```powershell
python -m pytest tests/silver_pdga_live_results/test_round_only_events.py -v
```

## Documentation Index

- Silver schema:
  - `docs/silver_live_results_schema.md`
- Silver code walkthrough:
  - `docs/silver_live_results_code_walkthrough.md`
- Event pages walkthrough:
  - `docs/ingest_pdga_event_pages_code_walkthrough.md`
- Live results walkthrough:
  - `docs/ingest_pdga_live_results_code_walkthrough.md`
- DynamoDB data model:
  - `docs/dynamodb_data_model.md`
- Backfill completion checklist:
  - `docs/silver_backfill_completion_checklist.md`

## Notes

- Rebuild Docker images after code changes to avoid stale installed package behavior.
- Date comparisons in DynamoDB workflows assume `YYYY-MM-DD` ISO strings.
- Silver backfill progress can be monitored via logs and Silver checkpoint items in DynamoDB.
```

### What changed and why

- Removed generated wrapper text and code-fenced README artifact.
- Updated scope to include Silver (current repo reality).
- Added Silver commands, storage paths, and DynamoDB checkpoint/run-summary item types.
- Linked your new docs so onboarding and operations are centralized.

### How to apply and validate

```powershell
# replace README.md content with the file above
git add README.md
git commit -m "docs: refresh README for bronze+silver pipelines and runbook links"
```