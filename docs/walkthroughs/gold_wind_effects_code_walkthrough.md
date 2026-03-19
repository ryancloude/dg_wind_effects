# Gold Wind Effects Code Walkthrough

## Module Overview
Package: `gold_wind_effects`

Core modules:
- `config.py`: loads env config (`PDGA_S3_BUCKET`, `PDGA_DDB_TABLE`, optional `AWS_REGION`)
- `dynamo_io.py`: candidate discovery + checkpoints + run summaries
- `silver_io.py`: reads Gold inputs (`silver_weather_enriched` parquet files)
- `transform.py`: appends Gold feature fields and deterministic row hashes
- `quality.py`: event-level DQ checks
- `parquet_io.py`: deterministic S3 output/quarantine keys + parquet writes
- `runner.py`: orchestration, incremental skip, DQ handling, writes, summary

## End-to-End Flow
1. Runner loads candidate events from successful `PIPELINE#SILVER_WEATHER_ENRICHED` checkpoint items.
2. Runner filters by run mode:
- `pending_only`: process only pending/failed events
- `full_check`: evaluate all candidates
3. For each event:
- load round/hole enriched rows from S3
- compute deterministic event fingerprint
- skip if unchanged success checkpoint exists
- transform rows with Gold feature columns
- run DQ checks
- on success: write Gold parquet + checkpoint
- on DQ failure: write quarantine + `dq_failed` checkpoint
4. At end, write run summary:
- `pk = RUN#<run_id>`
- `sk = GOLD_WIND_EFFECTS#SUMMARY`

## Why This Design
- Preserves event-level overwrite semantics for idempotency.
- Keeps deterministic incremental behavior via fingerprinting.
- Uses same runner pattern as Bronze/Silver for operational consistency.
- Produces model-friendly fields while retaining raw lineage columns.

## Typical Commands

Run tests:
```powershell
python -m pytest tests/gold_wind_effects -v

Dry Run:
python -m gold_wind_effects.runner --dry-run --run-mode pending_only --progress-every 25 --log-level INFO

Incremental run:
python -m gold_wind_effects.runner --run-mode pending_only --progress-every 25 --log-level INFO

Force specific events:
python -m gold_wind_effects.runner --event-ids 90008,90009 --force-events --progress-every 10