# Silver Live Results Schema

## Purpose
This document defines the Silver-layer schema for finalized PDGA live results in `dg_wind_effects`.

Design goals:
- Idempotent reruns with deterministic keys
- Incremental event-level processing
- Denormalized analytics-friendly tables
- Strong lineage and data-quality guarantees
- No Silver snapshots (Bronze remains replayable source of truth)

## Final Event Eligibility
An event is eligible for Silver processing only when:
- `status_text` is one of:
  - `Event complete; official ratings processed.`
  - `Event complete; unofficial ratings processed.`
- `live_results_ingested = true` on `EVENT#<event_id> / METADATA`

## Logical Keys

### player_rounds
- Logical PK: `(tourn_id, round_number, player_key)`

### player_holes
- Logical PK: `(tourn_id, round_number, hole_number, player_key)`

## Player Identity (`player_key`)
Deterministic derivation order:
1. `PDGA#<pdga_num>` when `PDGANum > 0`
2. `RESULT#<result_id>` when PDGA number missing and `ResultID > 0`
3. `NAMEHASH#<sha256>` from normalized:
   - `tourn_id|name|first_name|last_name|city|state_prov|country`

`player_key_type` values:
- `pdga_num`
- `result_id`
- `namehash`

## Event Location Enrichment (from METADATA)
Both Silver tables include:
- `event_location_raw`
- `event_city`
- `event_state`
- `event_country`

Mapping:
- `event_location_raw = location_raw if present else raw_location`
- `event_city = city`
- `event_state = state`
- `event_country = country`

## Round Date Interpolation Contract
Both tables include deterministic round-date interpolation fields:
- `round_date_interp`
- `round_date_interp_method`
- `round_date_interp_confidence`

Behavior:
- For multi-day events (`event_start_date < event_end_date`), interpolation is always applied.
- For single-day events or no span, `event_start_date` is used.
- If `event_start_date` is missing/invalid, interpolation output is blank and method indicates fallback.

Formula:
- `span_days = max(0, end_date - start_date)`
- If `max_round_number > 1` and `span_days > 0`:
  - `offset_days = floor((round_number - 1) * span_days / (max_round_number - 1))`
  - `round_date_interp = start_date + offset_days`
- Else:
  - `round_date_interp = start_date` (or blank if start date missing)

## Table: player_rounds

### Grain
One row per player per round per event.

### Columns

| Column | Type | Required | Description |
|---|---|---|---|
| event_year | INT | yes | Partition column |
| tourn_id | BIGINT | yes | Event/Tournament ID |
| round_number | SMALLINT | yes | Round number |
| player_key | STRING | yes | Deterministic player key |
| player_key_type | STRING | yes | `pdga_num` / `result_id` / `namehash` |
| pdga_num | BIGINT | no | PDGA number |
| result_id | BIGINT | no | Result ID |
| score_id | BIGINT | no | Score row ID |
| round_id | BIGINT | no | Round ID |
| division | STRING | yes | Division (non-key) |
| player_name | STRING | yes | Display name |
| first_name | STRING | no | First name |
| last_name | STRING | no | Last name |
| short_name | STRING | no | Short name |
| profile_url | STRING | no | Player profile URL |
| player_city | STRING | no | Player city |
| player_state_prov | STRING | no | Player state/province |
| player_country | STRING | no | Player country |
| player_full_location | STRING | no | Player full location text |
| event_name | STRING | no | Event name |
| event_status_text | STRING | no | Event status |
| event_start_date | STRING | no | Event start date (`YYYY-MM-DD`) |
| event_end_date | STRING | no | Event end date (`YYYY-MM-DD`) |
| round_date_interp | STRING | no | Interpolated round date (`YYYY-MM-DD`) |
| round_date_interp_method | STRING | no | Method used to produce `round_date_interp` |
| round_date_interp_confidence | DOUBLE | no | Confidence score for interpolation |
| event_location_raw | STRING | no | Event location raw text |
| event_city | STRING | no | Event city |
| event_state | STRING | no | Event state |
| event_country | STRING | no | Event country |
| layout_id | BIGINT | no | Layout ID |
| layout_name | STRING | no | Layout name |
| course_id | BIGINT | no | Course ID |
| course_name | STRING | no | Course name |
| layout_holes | SMALLINT | no | Number of holes in layout |
| layout_par | SMALLINT | no | Layout par |
| layout_length | INT | no | Layout length |
| layout_units | STRING | no | Units |
| pool | STRING | no | Pool |
| round_pool | STRING | no | Round pool |
| card_num | INT | no | Card number |
| tee_start | STRING | no | Tee start |
| tee_time_raw | STRING | no | Tee time raw string |
| tee_time_sort | STRING | no | Tee time sort key |
| tee_time_est_ts | STRING | no | Estimated Tee Time |
| tee_time_est_method | STRING | no | Method used to estimate Tee time |
| tee_time_est_confidence | Double | no | Confidence in Estimated Tee Time |
| lag_bucket_used | INT | no | Bucket used to find difference between scorecard updated and tee time |
| lag_sample_size | INT | no | sample size of lag bucket |
| round_duration_est_minutes | INT | no | Etimated duration of the round |
| played_holes | SMALLINT | no | Holes played |
| round_score | INT | no | Round strokes |
| round_to_par | INT | no | Round to par |
| round_rating | INT | no | Round rating |
| grand_total | INT | no | Event running total |
| to_par_total | INT | no | Event running to-par |
| prev_rnd_total | INT | no | Prior rounds total |
| prev_rounds | SMALLINT | no | Prior rounds count |
| running_place | INT | no | Running place |
| previous_place | INT | no | Previous place |
| round_status | STRING | no | Round status |
| completed_flag | BOOLEAN | no | Completed |
| round_started_flag | BOOLEAN | no | Round started |
| has_round_score_flag | BOOLEAN | no | Has round score |
| authoritative_flag | BOOLEAN | no | Authoritative |
| tied_flag | BOOLEAN | no | Tied |
| won_playoff_flag | BOOLEAN | no | Won playoff |
| scorecard_updated_at_raw | STRING | no | Raw scorecard timestamp |
| update_date_raw | STRING | no | Raw update timestamp |
| scorecard_updated_at_ts | STRING | no | Parsed scorecard timestamp |
| update_date_ts | STRING | no | Parsed update timestamp |
| source_json_key | STRING | yes | Bronze JSON key |
| source_meta_key | STRING | no | Bronze meta key |
| source_content_sha256 | STRING | yes | Bronze payload hash |
| source_fetched_at_utc | STRING | yes | Bronze fetch time |
| silver_run_id | STRING | yes | Silver run ID |
| silver_processed_at_utc | STRING | yes | Silver processing time |
| event_source_fingerprint | STRING | yes | Event fingerprint used for incremental skip |
| row_hash_sha256 | STRING | yes | Deterministic row hash |

## Table: player_holes

### Grain
One row per player per hole per round per event.

### Columns

| Column | Type | Required | Description |
|---|---|---|---|
| event_year | INT | yes | Partition column |
| tourn_id | BIGINT | yes | Event/Tournament ID |
| round_number | SMALLINT | yes | Round number |
| hole_number | SMALLINT | yes | Hole ordinal |
| player_key | STRING | yes | Deterministic player key |
| player_key_type | STRING | yes | `pdga_num` / `result_id` / `namehash` |
| pdga_num | BIGINT | no | PDGA number |
| result_id | BIGINT | no | Result ID |
| score_id | BIGINT | no | Score row ID |
| round_id | BIGINT | no | Round ID |
| division | STRING | yes | Division (non-key) |
| player_name | STRING | yes | Display name |
| event_location_raw | STRING | no | Event location raw text |
| event_city | STRING | no | Event city |
| event_state | STRING | no | Event state |
| event_country | STRING | no | Event country |
| event_start_date | STRING | no | Event start date |
| event_end_date | STRING | no | Event end date |
| round_date_interp | STRING | no | Interpolated round date (`YYYY-MM-DD`) |
| round_date_interp_method | STRING | no | Method used to produce `round_date_interp` |
| round_date_interp_confidence | DOUBLE | no | Confidence score for interpolation |
| layout_id | BIGINT | no | Layout ID |
| layout_name | STRING | no | Layout name |
| course_id | BIGINT | no | Course ID |
| course_name | STRING | no | Course name |
| layout_holes | SMALLINT | no | Layout hole count |
| tee_start | STRING | no | Tee start |
| tee_time_raw | STRING | no | Tee time raw string |
| tee_time_sort | STRING | no | Tee time sort key |
| tee_time_est_ts | STRING | no | Estimated Tee Time |
| tee_time_est_method | STRING | no | Method used to estimate Tee time |
| tee_time_est_confidence | Double | no | Confidence in Estimated Tee Time |
| lag_bucket_used | INT | no | Bucket used to find difference between scorecard updated and tee time |
| lag_sample_size | INT | no | sample size of lag bucket |
| round_duration_est_minutes | INT | no | Etimated duration of the round |
| hole_start_est_ts | STRING | no | estimated hole start time |
| hole_end_est_ts | STRING | no | estimate time for hole end |
| lhole_time_est_method | STRING | no | method used to estimate the hole time |
| hole_time_est_confidence | DOUBLE | no | Confidence in hole time estimation |
| hole_code | STRING | no | Source hole code (`H1`) |
| hole_label | STRING | no | Hole label |
| hole_ordinal | SMALLINT | no | Hole ordinal from source |
| hole_par | SMALLINT | no | Hole par |
| hole_length | INT | no | Hole length |
| hole_score | SMALLINT | no | Player strokes on hole |
| hole_to_par | SMALLINT | no | `hole_score - hole_par` |
| played_holes | SMALLINT | no | Round-level denorm |
| round_score | INT | no | Round-level denorm |
| round_to_par | INT | no | Round-level denorm |
| completed_flag | BOOLEAN | no | Round-level denorm |
| round_status | STRING | no | Round-level denorm |
| scorecard_updated_at_ts | STRING | no | Parsed scorecard timestamp |
| update_date_ts | STRING | no | Parsed update timestamp |
| source_json_key | STRING | yes | Bronze JSON key |
| source_meta_key | STRING | no | Bronze meta key |
| source_content_sha256 | STRING | yes | Bronze payload hash |
| source_fetched_at_utc | STRING | yes | Bronze fetch time |
| silver_run_id | STRING | yes | Silver run ID |
| silver_processed_at_utc | STRING | yes | Silver processing time |
| event_source_fingerprint | STRING | yes | Event fingerprint used for incremental skip |
| row_hash_sha256 | STRING | yes | Deterministic row hash |

## S3 Partitioning and Keys

Final deterministic keys:
- `silver/pdga/live_results/player_rounds/event_year=<YYYY>/tourn_id=<TournID>/player_rounds.parquet`
- `silver/pdga/live_results/player_holes/event_year=<YYYY>/tourn_id=<TournID>/player_holes.parquet`

Temporary keys during write:
- `silver/pdga/live_results/_tmp/run_id=<run_id>/event_id=<TournID>/player_rounds.parquet`
- `silver/pdga/live_results/_tmp/run_id=<run_id>/event_id=<TournID>/player_holes.parquet`

Quarantine keys:
- `silver/pdga/live_results/quarantine/event_id=<TournID>/run_id=<run_id>/dq_errors.json`

## Round-Only Event Behavior
Some finalized events do not include hole-by-hole detail.
Behavior:
- Event is still considered successful if `player_rounds` is non-empty.
- `player_holes` may be empty.
- `events_without_hole_detail` counter increments.
- Existing stale `player_holes.parquet` for the event is removed.

## Dedup and Tie-Break Rules

### player_rounds
- Dedup key: `(tourn_id, round_number, player_key)`

### player_holes
- Dedup key: `(tourn_id, round_number, hole_number, player_key)`

Tie-break order for both:
1. `source_fetched_at_utc`
2. `scorecard_updated_at_ts`
3. `update_date_ts`
4. `source_json_key`

## Tee Time Estimation (v1)
For rows missing `tee_time_raw`:
- if `scorecard_updated_at_ts` exists, estimate:
  - `tee_time_est_ts = scorecard_updated_at_ts - 449 minutes`
  - method: `score_minus_global_median_lag`
  - confidence: `0.55`
- if `tee_time_raw` exists and score timestamp exists:
  - align tee time to same/previous day so tee is never after score timestamp
  - method: `raw_tee_time`
  - confidence: `1.00`
- if both are missing:
  - method: `missing_inputs`
  - confidence: `0.00`

## Data Quality Constraints
- Uniqueness of logical PK in each table
- Referential integrity: every `player_holes` row has a parent `player_rounds` row on `(tourn_id, round_number, player_key)`
- Required lineage fields present:
  - `source_json_key`
  - `source_content_sha256`
  - `source_fetched_at_utc`
  - `silver_run_id`
- Domain checks:
  - `round_number >= 1`
  - `hole_number >= 1`
  - `hole_number <= layout_holes` when `layout_holes` is known
- Division collision guard:
  - for each `(tourn_id, player_key)`, count distinct `division` must be <= 1

If any DQ check fails:
- Event is quarantined
- Event status checkpoint is `dq_failed`
- Final partition is not updated for that run

## Incremental and Idempotent Contract
- Process finalized + ingested events only
- Build `event_source_fingerprint` from per-division/per-round Bronze source keys and hashes
- Selection behavior:
  - `pending_only` (default): excludes successful fingerprinted events and excludes `dq_failed` unless `--include-dq-failed-in-pending` is set
  - `full_check`: evaluates all candidates
- Skip unchanged events via checkpoint comparison (unless `--force-events`)
- Overwrite only target event partition when changed
- Bronze remains source of truth and replay layer