# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`trg_app` is the production Python backend for Tail Risk Global: a Flask API plus a set of batch/pipeline scripts that compute portfolio VaR, P&L, stress tests and dashboard data against a shared PostgreSQL (AWS RDS) database. There is no packaging (`setup.py`/`pyproject.toml`) and no requirements file; dependencies are installed on servers by `install.sh` (Amazon Linux, `.venv`).

## Running

```bash
python api_main.py                      # dev: port 5050, Flask debug on
APP_ENV=production python api_main.py   # prod: port 8000
python api_main.py -port 5051 -debug    # override port / debug
```

Swagger UI is served at `/api/apidocs/` (spec template in `api/apidoc.yml`). Logs go to `../log/api.<timestamp>.log` (file only, not stdout).

Most modules outside `api/` are standalone scripts meant to be run directly from their own directory; they put the repo root on `sys.path` themselves (`sys.path.insert(0, <parent>)`), so imports are always repo-root absolute (`from database2 import pg_connection`, `from dashboard.positions_calc import ...`). Keep that pattern when adding new runnable scripts. Examples:

```bash
python dashboard/dashboard_process.py --date 2026-03-29 --account-id 5
python process2/calculate_var.py mssb [as_of_date]
python process2/process_mssb_positions.py 2025-01-15
python database2/create_tables.py        # applies database2/tables.sql
python process_scheduler/register.py [job_id]   # push jobs.json to the remote scheduler
```

### Tests

`tests/` is mostly ad-hoc scripts, not a real test suite: many files have no `test_` functions, hardcode credentials/hosts (e.g. `http://localhost:5050`), hit the live DB, and some use `xlwings` (Excel on Windows). There is no pytest config or conftest. Run a single file/function with `python -m pytest tests/test_var.py::test_name` only where functions actually exist; otherwise run the script directly. `process2/README.md` describes `process2/tests/` with `-m "not integration"` markers, but that directory does not currently exist.

### Deploy

`deploy_dev2.bat` / `deploy_prod2.bat` robocopy the tree to a staging folder (excluding `.venv`, `__pycache__`, `.git`, `test_data`, `log`), scp to the EC2 host at `/home/ec2-user/api/trgapp`, then `sudo supervisorctl restart api`. Several subfolders have their own `deploy_prod2.bat` for pushing just that folder. These deploy to real servers — don't run them unless asked.

## Configuration

`trg_config.py` builds a global `config` dict imported almost everywhere (`from trg_config import config`). Importing it has side effects:
- Reads secrets from `../config/app_config.json` (or `$TRG_CONFIG_FILE`) and exports `DB_*`, `PRS_*`, `EMAIL_*` into `os.environ`.
- Merges `../config/config.json`, `../config/support_team.json`, and `../config/config_override.json` (or `$TRG_OVERRIDE`; values for existing `Path` keys are converted to `Path`).
- Creates all `*_DIR` folders under `../data/` (market data, models, securities, portfolios, VaR, YH/YF/BB feeds, etc.).

So `config/`, `data/`, and `log/` are **siblings of `trg_app/`**, not inside it, and are not in git. `maintenance/_paths.py` similarly points at `../data/maintenance/{CSV,Excel}`.

## Architecture

### Two database layers
- `database/` — legacy layer: Flask-SQLAlchemy `db` (bound to the app in `api/__init__.py`), ORM models in `database/models.py`, helpers in `db_utils.py` / `model_aux.py`, plus MS SQL Server and SQLite remnants. Used by auth/user/account code.
- `database2/` — newer layer used by `process2/`, `dashboard/`, and most recent code: raw `psycopg2` via `with pg_connection() as conn:`, credentials read directly from `../config/app_config.json` (does not import `trg_config`). DDL lives in `database2/tables.sql` / `views.sql`; incremental migrations are loose `.sql` files in `dashboard/sql/`, `process2/sql/`, `maintenance/sql/` and are applied manually.

Both point at the same Postgres RDS instance. Prefer `database2` for new code.

### API (`api/`)
`api/__init__.py` creates the Flask `app` (CORS on `/api/*`, ProxyFix, Swagger, SQLAlchemy) and then imports `routes.py` and `ops_routes.py`, which register routes directly on `app` (no blueprints).
- `routes.py` — client-facing endpoints (register/login/password, portfolio upload/download, dashboard, what-if, broker settings). Auth via `@token_required` (JWT) from `api/auth.py`, which injects `username` as the first arg.
- `ops_routes.py` — internal ops-portal endpoints gated by `@ops_role_required` (roles `admin`/`superadmin`/`support`).
- `dispatch.py` — `api_request(route_name, username, request)` forwards to the big `if/elif` router in `request_handler.get_response()`; many endpoints are thin wrappers around this. `api_request_ft` is the restricted variant for "FT" users (`request_handler_ft.py`).
- Newer dashboard endpoints bypass the dispatcher and call functions in `dashboard/` directly (e.g. `dashboard.portfolios_page`, `dashboard.whatif`).

### Risk computation
- `engine/VaR_engine.py` — core VaR calculation (`calc_VaR`) over a positions DataFrame; uses `security/` for security attributes, `models/` for risk factor distributions, `utils/` helpers.
- `models/` — builds the risk-factor distributions the engine consumes (equity, credit/spread, IR/UST curve, FX, vol, options, private/alternative asset proxies, structured notes). Workflow per `models/readme.txt`: load raw timeseries into the market-data HDF file → build distributions → update `risk_factors`.
- `mkt_data/`, `detl/` — market data extraction (Yahoo via RapidAPI `YH_API`, yfinance, Bloomberg, FRED/Treasury, FIGI) into the HDF store (`config['mkt_file']`) and DB.
- `security/` — security master: lookup, xref (TRG_ID/ISIN/CUSIP/BB_GLOBAL/Ticker), sector/region/rating classification, new-security creation.

### Batch pipelines
- `process2/` — account/feed-level pipeline (see `process2/README.md`): raw broker feed (e.g. MSSB) → `proc_positions` (scoped by `feed_source`) → `preprocess_var` (security info + prices) → VaR engine → `position_var`. Also per-asset-class P&L calculators (`calc_*_pnl.py`), stress tests, benchmark, beta. Writes are delete-then-insert per `(as_of_date, account_id)` so reruns are idempotent.
- `dashboard/` — reads `position_var` and materializes dashboard tables (`db_mv_history`, `db_portfolio_summary`, `db_positions`, `db_concentrations`, `db_portfolio_breakdown`) via `dashboard_process.py`; also user-uploaded portfolio processing (`process_uploaded_portfolio.py` → `port_position_var`), settings/limits/presets, what-if, and `backfill_*` scripts. Modules generally split into `*_calc.py` (pure pandas) and `*_db.py` (SQL I/O).
- `process_scheduler/` — `jobs.json` declares the daily job DAG (id, script_path relative to `/home/ec2-user`, dependencies, retries) for an external scheduler service at `engine.tailriskglobal.com/scheduler`; `register.py` pushes it. Jobs key off the `as_of_date` stored in `proc_asof_date` (`database2.get_proc_asof_date()`).
- `maintenance/` — one-off operational scripts (security setup, reruns, dumps, remote commands/scp). Not imported by the app.

### Other
- `templates/` — email HTML/text templates used by `api/email.py`.
- `account/`, `clients/` — account management, limits, and per-account parameters.
- `report/`, `workbook/`, `calculator/`, `projects/`, `statement/`, `preprocess/`, `process/` — older/adjacent tooling (reports, Excel workbooks via xlwings, statement parsing); less central to the current API.
- `.claude/worktrees/` contains full copies of the repo — exclude it when searching.
