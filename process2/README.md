# process2 — VaR Calculation Pipeline

Processes position feeds from one or more data sources, calculates VaR per account, and stores results in the database.
`proc_positions` is a shared staging table — each feed source is identified by a `feed_source` column so pipelines do not interfere with each other.

## Data Flow

```
Raw MSSB Feed
    └── process_mssb_positions.py ──► proc_positions (DB, feed_source='mssb')
                                              │
                                    preprocess_var.py
                                      ├── update_security_info.py  (enrich with security metadata)
                                      └── update_position_price.py (fetch latest market prices)
                                              │
                                    calculate_var.py ──► VaR_engine.calc_VaR()
                                              │
                                    db_position_var.py ──────► position_var (DB)
```

## Files

### Pipeline

| File | Purpose |
|------|---------|
| `calculate_var.py` | **Main entry point.** Orchestrates VaR calculation per account for a given `feed_source`, re-attaches excluded positions (VaR columns = NULL), calls insert per account. |
| `preprocess_var.py` | Fetches `proc_positions` filtered by `feed_source`, maps column names to VaR engine conventions, enriches with security info and market prices. Returns `(params, positions)`. |
| `db_position_var.py` | Database layer for `position_var`. Reads from `proc_positions` (filtered by `feed_source`), writes to `position_var` (delete + re-insert per account to ensure clean re-runs). |

### Feed processors

| File | Purpose |
|------|---------|
| `process_mssb_positions.py` | Processes raw `mssb_posit` feed rows. Resolves accounts and securities (creating new ones as needed), archives old data to `proc_positions_hist`, inserts into `proc_positions` with `feed_source='mssb'`. |

### Enrichment utilities

| File | Purpose |
|------|---------|
| `update_security_info.py` | Merges security metadata (AssetClass, AssetType, attributes) onto positions. Excludes unknown, unmodeled, and matured securities. |
| `update_position_price.py` | Updates position prices from the market database. Handles cash pricing and recalculates market values. |

### Other

| File | Purpose |
|------|---------|
| `run_mssb_process.bat` | Runs the full MSSB pipeline (Step 1: `process_mssb_positions.py`, Step 2: `calculate_var.py mssb`) for a given `as_of_date`. |
| `sql/` | DDL scripts for tables used by this pipeline (`position_var`, `proc_positions`, `broker_asset_class_map`, etc.). |

## Usage

```bash
# Run the full MSSB pipeline for a specific date
run_mssb_process.bat 2026-03-19

# Run calculate_var only — auto-detects latest as_of_date for feed_source
python calculate_var.py mssb

# Run calculate_var for a specific date
python calculate_var.py mssb 2025-09-30

# Run the MSSB position processor only
python process_mssb_positions.py 2025-01-15
```

## feed_source

`proc_positions` and `proc_positions_hist` are shared tables. The `feed_source` column identifies which pipeline owns each row.

| feed_source | Pipeline |
|-------------|----------|
| `mssb` | `process_mssb_positions.py` → `calculate_var.py mssb` |

All reads and writes (including archiving to `proc_positions_hist`) are scoped by `feed_source`, so adding a new feed source does not affect existing pipelines.

**Required DB migration when this change is first deployed:**
```sql
ALTER TABLE proc_positions      ADD COLUMN feed_source TEXT;
ALTER TABLE proc_positions_hist ADD COLUMN feed_source TEXT;
```

## Regression Tests

Tests live in `tests/` and are written with pytest.

| File | What it tests |
|------|---------------|
| `tests/test_db_position_var.py` | `insert_results`: idempotency, stale row removal, type coercion (numeric/date/bool/NaN). `fetch_proc_positions` and `fetch_latest_as_of_date` with `feed_source` (integration). |
| `tests/test_preprocess_var.py` | `_map_columns`: renames, drops, passthrough. `build_params`: required keys, types, values. `preprocess_var` end-to-end (integration). |
| `tests/test_calculate_var.py` | `build_results`: merge correctness, no duplicate columns. Excluded row re-attachment: NULL VaR columns, position data preserved, row count. `calculate_var` end-to-end (integration). |

```bash
# Unit tests only (fast, no DB required)
cd trg_app && python -m pytest process2/tests/ -v -m "not integration"

# All tests including integration (require live DB + VaR model)
cd trg_app && python -m pytest process2/tests/ -v -m integration
```

## Key Design Notes

- `proc_positions` is a **shared table** — `feed_source` scopes all reads, writes, and archiving so multiple feed pipelines can coexist safely.
- VaR is calculated **per account** inside a loop; failed accounts are logged and skipped.
- `insert_results` deletes existing `(as_of_date, account_id)` rows before inserting, so re-runs are safe.
- Excluded positions are re-attached to the results with VaR columns set to NULL.
- `as_of_date` defaults to `MAX(as_of_date)` from `proc_positions` for the given `feed_source` if not provided.
- `PortfolioName` in `build_params` is currently hardcoded to `'MSSB'` — to be derived from `feed_source` when additional feed sources are added.
