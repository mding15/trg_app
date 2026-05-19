"""
dump_scheduler.py — Dump scheduler tables for a given run_date to Excel.

Sheets written (in order):
    sch_config       — scheduler key-value config   (all rows, no date filter)
    sch_processes    — process definitions           (all rows, no date filter)
    sch_daily_runs   — daily run status              (filtered by run_date)
    sch_run_attempts — full attempt log              (filtered by run_date; stdout/stderr truncated to 1000 chars)

Each sheet is capped at 1000 rows.

Output:
    maintenance/Excel/dump_scheduler_{run_date}.xlsx

Usage:
    python maintenance/dump_scheduler.py --date 2026-04-28
    python maintenance/dump_scheduler.py              # defaults to today
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

EXCEL_DIR = Path(__file__).resolve().parent / "Excel"
ROW_LIMIT = 1000
STDOUT_STDERR_LIMIT = 1000


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_scheduler")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _fetch(sql: str, params: tuple) -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _fetch_all(table: str) -> pd.DataFrame:
    return _fetch(f"SELECT * FROM {table} LIMIT {ROW_LIMIT}", ())


def _fetch_by_run_date(table: str, run_date: date) -> pd.DataFrame:
    return _fetch(
        f"SELECT * FROM {table} WHERE run_date = %s LIMIT {ROW_LIMIT}",
        (run_date,),
    )


def _truncate_text_cols(df: pd.DataFrame, cols: list[str], limit: int) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: v[:limit] + "…" if isinstance(v, str) and len(v) > limit else v
            )
    return df


# ── Core ──────────────────────────────────────────────────────────────────────

def run(run_date: date) -> None:
    log = _setup_logger()
    log.info(f"dump_scheduler  run_date={run_date}")

    EXCEL_DIR.mkdir(parents=True, exist_ok=True)

    sheets: dict[str, pd.DataFrame] = {}

    log.info("Fetching sch_config …")
    sheets["sch_config"] = _fetch_all("sch_config")

    log.info("Fetching sch_processes …")
    sheets["sch_processes"] = _fetch_all("sch_processes")

    log.info("Fetching sch_daily_runs …")
    sheets["sch_daily_runs"] = _fetch_by_run_date("sch_daily_runs", run_date)

    log.info("Fetching sch_run_attempts …")
    df_attempts = _fetch_by_run_date("sch_run_attempts", run_date)
    df_attempts = _truncate_text_cols(df_attempts, ["stdout", "stderr"], STDOUT_STDERR_LIMIT)
    sheets["sch_run_attempts"] = df_attempts

    # ── Write Excel ───────────────────────────────────────────────────────────

    date_str = run_date.strftime("%Y%m%d")
    out_path = EXCEL_DIR / f"dump_scheduler_{date_str}.xlsx"

    log.info(f"Writing {out_path} …")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            log.info(
                f"  Sheet '{sheet_name}': {len(df)} row(s) x {len(df.columns)} col(s)"
                + (" [capped at 1000]" if len(df) == ROW_LIMIT else "")
            )

    log.info("─" * 60)
    log.info(f"Done.  Written to {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    def _parse_date(s: str) -> date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser = argparse.ArgumentParser(
        description="Dump scheduler tables for a given run_date to Excel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/dump_scheduler.py --date 2026-04-28\n"
            "  python maintenance/dump_scheduler.py\n"
        ),
    )
    parser.add_argument("--date", dest="run_date", type=_parse_date, default=None,
                        metavar="YYYY-MM-DD", help="Run date to dump (default: today)")
    args = parser.parse_args()

    run_date = args.run_date or date.today()
    run(run_date=run_date)
