# Gold Wind Model Inputs Code Walkthrough

## Module Overview
Package: `gold_wind_model_inputs`

Core modules:
- `config.py`: runtime config
- `dynamo_io.py`: candidate loading from `PIPELINE#GOLD_WIND_EFFECTS`, checkpoint/run summary writes
- `gold_io.py`: reads Gold feature parquet
- `transform.py`: builds curated model-input rows and deterministic row hashes
- `quality.py`: model-input DQ validation
- `parquet_io.py`: output/quarantine S3 key builders + parquet writes
- `runner.py`: orchestration and incremental behavior

## End-to-End Flow
1. Runner discovers candidate events from successful Gold feature checkpoints.
2. Runner selects events via:
- `pending_only` + optional inclusion of `dq_failed`
- `full_check`
3. For each event:
- load hole (required) and round (optional) feature rows
- compute event fingerprint
- skip unchanged success checkpoint
- build hole/round model-input rows
- run DQ validation
- write outputs and success checkpoint on pass
- write quarantine and `dq_failed` checkpoint on DQ failure
4. Write run summary item.

## Why This Layer Exists
- Stabilizes model training/inference contract.
- Prevents accidental leakage or schema drift from upstream tables.
- Keeps preprocessing deterministic and versioned.
- Improves reproducibility of experiments and production runs.

## Typical Commands

Run tests:
```powershell
python -m pytest tests/gold_wind_model_inputs -v

Dry run:
python -m gold_wind_model_inputs.runner --dry-run --run-mode pending_only --progress-every 25 --log-level INFO

Incremental run:
python -m gold_wind_model_inputs.runner --run-mode pending_only --progress-every 25 --log-level INFO

Force specific events:
python -m gold_wind_model_inputs.runner --event-ids 90008,90009 --force-events