# Report Round Weather Impacts Code Walkthrough

## Module Overview

Package:
- `report_round_weather_impacts`

Core modules:
- `config.py`
  - loads S3 bucket, DynamoDB table, and AWS region config
- `models.py`
  - defines pipeline constants, report table names, prefixes, unit conversion constants, and report policy version
- `scored_io.py`
  - lists scored round event parquet files and loads event-level dataframes
- `dimensions.py`
  - derives dashboard-friendly dimensions and converts weather units to mph / Fahrenheit
- `aggregations.py`
  - builds event-level intermediate contribution tables for each report dataset
- `publish.py`
  - rebuilds published dashboard tables from the intermediate contribution files
- `parquet_io.py`
  - writes deterministic intermediate and published parquet outputs
- `dynamo_io.py`
  - handles event-level checkpoints and run summaries in DynamoDB
- `runner.py`
  - orchestrates the end-to-end reporting refresh

---

## Why This Step Exists

`score_round_wind_model` gives the project row-level scored outputs, which are perfect for:
- detail exploration
- event drill-down
- QA

But dashboards typically need:
- aggregated tables
- stable semantic layers
- smaller, filter-friendly datasets
- consistent user-facing units

`report_round_weather_impacts` exists to build that semantic layer.

This package is the bridge between:
- scored round rows
and
- dashboard-ready reporting tables

---

## Core Reporting Architecture

A major design choice in this package is the use of two reporting layers.

## Layer 1: event-level intermediate contributions
For every event, the reporting step writes deterministic contribution files for each report table.

Example:
- one event’s contribution to `weather_by_state`
- one event’s contribution to `weather_by_division`
- one event’s contribution to `weather_by_event_round`

These files are:
- incremental
- idempotent
- easy to overwrite safely

## Layer 2: published dashboard tables
After event contributions are refreshed, the step rebuilds final dashboard tables by reading all intermediate contributions for each report dataset.

These published tables are:
- global dashboard inputs
- already aggregated
- already standardized to mph / Fahrenheit
- easy for the dashboard to load directly

This two-layer design is what makes the step both incremental and dashboard-friendly.

---

## End-to-End Flow

## 1. Load config
The runner reads:
- S3 bucket
- DynamoDB table
- AWS region

from environment-driven config.

## 2. List scored event inputs
`scored_io.py` lists scored event parquet files from:

- `gold/pdga/wind_effects/scored_rounds/...`

Each scored event file is one reporting refresh unit.

This mirrors the event-level pattern used elsewhere in the pipeline.

## 3. Compute an event input fingerprint
For each scored event file, the runner computes a deterministic fingerprint from object metadata such as:
- key
- etag
- size
- last modified

This fingerprint answers:
- did the scored input for this event change?

## 4. Check DynamoDB for an existing success
The runner checks for an event-level checkpoint with:
- `pk = PIPELINE#REPORT_ROUND_WEATHER_IMPACTS`
- `sk = EVENT#<event_id>#REPORT_POLICY#<version>`

If the fingerprint is unchanged and the prior run succeeded, the event is skipped unless:
- `--force-events`

This keeps the step incremental.

## 5. Load the scored event dataframe
For events that need processing, `scored_io.py` loads the event’s scored round parquet rows into a pandas dataframe.

## 6. Prepare reporting dimensions and units
`dimensions.py`:
- validates required scored columns
- drops rows missing critical fields needed for reporting
- converts observed wind from `mps` to `mph`
- converts observed temperature from `C` to `F`
- derives:
  - `rating_band`
  - `temperature_band_f`
  - `precip_flag`
  - `round_year`
  - `round_month`
  - `round_month_label`

This is where the reporting layer becomes dashboard-facing.

## 7. Build per-event contribution tables
`aggregations.py` groups the prepared event dataframe in several ways to produce event-level contribution tables for:

- overview
- wind bucket
- temperature band
- month
- event geo
- state
- division
- rating band
- course/layout
- event
- event round

Each contribution table stores:
- dimensions
- counts
- additive sums

This makes later recombination straightforward.

## 8. Write deterministic intermediate files
`parquet_io.py` writes each event’s contribution table to deterministic S3 paths such as:

- `gold/pdga/wind_effects/reports/intermediate/weather_by_state/event_year=<YYYY>/tourn_id=<ID>/part.parquet`

These writes are event-level overwrites, which keeps them idempotent.

## 9. Write success checkpoint
After all of an event’s contribution tables are written, the runner records:
- the event fingerprint
- row counts
- success status

in DynamoDB.

## 10. Publish dashboard tables
After event processing finishes, `publish.py` rebuilds each published report table by:
- listing all intermediate files for that table
- loading them into pandas
- grouping them at the dashboard dataset grain
- summing additive metrics
- computing final averages

The result is written to:

- `gold/pdga/wind_effects/reports/published/<table_name>/report.parquet`

## 11. Write run summary
Finally, the runner writes a run summary with:
- events attempted
- events processed
- events skipped
- events failed
- published tables count
- input and retained row totals

---

## Core Design Choices

## Event-level incrementality for an aggregate step
A key design question in reporting is whether aggregated outputs can still be incremental.

This package answers yes by using:
- event-level intermediate contributions
- final published tables rebuilt from those contributions

This is the cleanest production-style compromise between:
- correctness
- incrementality
- understandable code

## Dashboard-first semantic layer
The reporting outputs are not generic warehouse aggregates.
They are intentionally designed around the dashboard’s pages:
- Overview
- Geography
- Where It Matters
- Event Explorer

This is why the report table set is shaped the way it is.

## mph / Fahrenheit in reporting
Upstream scoring remains in the units produced by model-input data.
But reporting converts weather values to:
- mph
- Fahrenheit

This makes the dashboard U.S.-friendly and more natural for portfolio presentation.

## Sums and counts in intermediate tables
Intermediate tables store additive metrics rather than final averages only.

Why this is important:
- additive metrics combine cleanly across events
- published tables can compute consistent averages
- event contributions remain stable and composable

This also avoids more complex incremental median logic in v1.

---

## Module Walkthrough

## `models.py`
Defines:
- pipeline name
- report checkpoint PK
- report policy version
- base prefixes
- list of published report tables
- conversion constants
- required scored input columns

This is the policy anchor for the reporting step.

## `scored_io.py`
Responsible for:
- listing scored event parquet files
- filtering by event IDs
- loading one scored event dataframe

This keeps S3 scoring-input access separate from aggregation logic.

## `dimensions.py`
This module standardizes reporting-ready fields.

Responsibilities:
- validate required scored columns
- numeric coercion
- row retention filtering
- wind conversion to mph
- temperature conversion to Fahrenheit
- rating band derivation
- temperature band derivation
- month derivation
- state normalization

This is where the dashboard-facing semantic layer begins.

## `aggregations.py`
This module builds event-level contributions.

Responsibilities:
- group by dashboard dimensions
- calculate counts
- calculate additive sums
- produce one dataframe per report table

It owns the event-level contribution logic.

## `publish.py`
This module rebuilds final dashboard tables.

Responsibilities:
- list intermediate files for a report table
- read and concatenate them
- group by final dashboard keys
- sum additive metrics
- compute final averages

This keeps publish logic isolated from event refresh logic.

## `parquet_io.py`
Responsible for:
- deterministic key construction
- writing intermediate contribution files
- writing published tables

This module owns S3 path construction for reporting outputs.

## `dynamo_io.py`
Responsible for:
- `get_report_checkpoint(...)`
- `put_report_checkpoint(...)`
- `put_report_run_summary(...)`

This module isolates reporting checkpoint concerns from orchestration logic.

## `runner.py`
This is the orchestration entrypoint.

Responsibilities:
- parse CLI args
- list scored inputs
- compute event fingerprints
- decide event-level skip vs process
- prepare reporting dataframe
- build/write intermediate contributions
- publish final dashboard tables
- write checkpoints and run summary
- emit timing logs

---

## Report Table Set

The report table set matches the planned dashboard.

## Overview page support
- `weather_overview`
- `weather_by_wind_bucket`
- `weather_by_temperature_band`
- `weather_by_month`

## Geography page support
- `weather_by_event_geo`
- `weather_by_state`

## Where It Matters page support
- `weather_by_division`
- `weather_by_rating_band`
- `weather_by_course_layout`

## Event Explorer support
- `weather_by_event`
- `weather_by_event_round`

This is a dashboard-first reporting design.

---

## Output Contracts

## Intermediate outputs
Written under:

- `gold/pdga/wind_effects/reports/intermediate/...`

These are not meant to be the dashboard’s primary read path.
They are the incremental processing layer.

## Published outputs
Written under:

- `gold/pdga/wind_effects/reports/published/...`

These are the dashboard’s main semantic-layer inputs.

---

## Incremental Behavior

This step is incremental at the event level.

### Skip rule
An event is skipped when:
- its scored source fingerprint is unchanged
- its report policy version is unchanged
- the existing checkpoint is successful
- `--force-events` was not used

### Why this works
Because the event-level contribution files are deterministic and overwrite-safe.

### Publish phase behavior
Published tables are rebuilt from all intermediate event contributions.
So they always reflect:
- old unchanged events
- newly updated events
- forced refreshes

This is correct and production-friendly.

---

## Typical Commands

Run unit tests:
```powershell
python -m pytest tests/report_round_weather_impacts -v
