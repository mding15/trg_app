"""
dump_account.py — Dump pipeline tables for a given account_id and as_of_date to Excel.

Sheets written (in order):
    mssb_posit          — raw broker feed     (filtered by feed_date = as_of_date)
    proc_positions      — processed positions (filtered by account_id + as_of_date)
    position_var        — VaR positions       (filtered by account_id + as_of_date)
    db_positions        — dashboard positions (filtered by account_id + as_of_date)
    security_info_view  — security master     (security_ids from proc_positions)
    sec_beta_view       — betas               (security_ids from proc_positions)
    security_attribute  — security attributes (security_ids from proc_positions)

Each sheet is capped at 1000 rows.

Parent account handling:
    If the given account_id is a parent (i.e. other accounts have parent_account_id = account_id
    in the "account" table), mssb_posit and proc_positions are queried using the child
    account_ids instead — because broker feed rows and processed positions are stored under
    children, not the parent.  position_var is always queried by the supplied account_id
    (parent-level data is already aggregated there).

Output:
    maintenance/Excel/dump_{account_id}_{as_of_date}.xlsx

Usage:
    python maintenance/dump_account.py --account-id 1003 --date 2026-03-02
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


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_account")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _fetch(sql: str, params: tuple) -> pd.DataFrame:
    """Execute *sql* with *params* and return results as a DataFrame."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _fetch_by_date(table: str, where: str, params: tuple) -> pd.DataFrame:
    """Fetch up to ROW_LIMIT rows from *table* filtered by *where*."""
    sql = f"SELECT * FROM {table} WHERE {where} LIMIT {ROW_LIMIT}"
    return _fetch(sql, params)


def _fetch_by_security_ids(table: str, sec_col: str, sec_ids: list) -> pd.DataFrame:
    """
    Fetch up to ROW_LIMIT rows from *table* where *sec_col* is in *sec_ids*.
    *sec_col* is used verbatim in the SQL — pass quoted names (e.g. '"SecurityID"')
    for mixed-case columns.
    """
    if not sec_ids:
        return pd.DataFrame()
    sql = f"SELECT * FROM {table} WHERE {sec_col} = ANY(%s) LIMIT {ROW_LIMIT}"
    return _fetch(sql, (sec_ids,))


# ── Core ──────────────────────────────────────────────────────────────────────

def _get_child_ids(account_id: int) -> list[int]:
    """Return child account_ids for *account_id*, or empty list if not a parent."""
    df = _fetch(
        "SELECT account_id FROM account WHERE parent_account_id = %s",
        (account_id,),
    )
    if df.empty:
        return []
    return df["account_id"].tolist()


def run(account_id: int, as_of_date: date) -> None:
    log = _setup_logger()
    log.info(f"dump_account  account_id={account_id}  as_of_date={as_of_date}")

    EXCEL_DIR.mkdir(parents=True, exist_ok=True)

    sheets: dict[str, pd.DataFrame] = {}

    # ── Resolve child accounts (parent account handling) ──────────────────────

    child_ids = _get_child_ids(account_id)
    if child_ids:
        log.info(f"Parent account detected — child account_ids: {child_ids}")
        posit_ids = child_ids      # mssb_posit and proc_positions use children
    else:
        posit_ids = [account_id]   # leaf account — use as-is

    # ── Position tables ───────────────────────────────────────────────────────

    log.info("Fetching mssb_posit …")
    sheets["mssb_posit"] = _fetch(
        """
        SELECT m.*
        FROM mssb_posit m
        JOIN broker_account ba ON ba.broker_account = m.account
        WHERE ba.account_id = ANY(%s) AND m.feed_date = %s
        LIMIT %s
        """,
        (posit_ids, as_of_date, ROW_LIMIT),
    )

    log.info("Fetching proc_positions …")
    sheets["proc_positions"] = _fetch_by_date(
        "proc_positions",
        "account_id = ANY(%s) AND as_of_date = %s",
        (posit_ids, as_of_date),
    )

    log.info("Fetching position_var …")
    sheets["position_var"] = _fetch_by_date(
        "position_var",
        "account_id = %s AND as_of_date = %s",
        (account_id, as_of_date),
    )

    log.info("Fetching db_positions …")
    sheets["db_positions"] = _fetch_by_date(
        "db_positions",
        "account_id = %s AND as_of_date = %s",
        (account_id, as_of_date),
    )

    # ── Derive security_ids from proc_positions ───────────────────────────────

    proc_df = sheets["proc_positions"]
    if proc_df.empty or "security_id" not in proc_df.columns:
        log.warning("proc_positions returned no rows — security tables will be empty.")
        sec_ids: list = []
    else:
        sec_ids = proc_df["security_id"].dropna().unique().tolist()
        log.info(f"  {len(sec_ids)} unique security_ids from proc_positions")

    # ── Security tables ───────────────────────────────────────────────────────

    log.info("Fetching security_info_view …")
    sheets["security_info_view"] = _fetch_by_security_ids(
        "security_info_view", '"SecurityID"', sec_ids
    )

    log.info("Fetching sec_beta_view …")
    sheets["sec_beta_view"] = _fetch_by_security_ids(
        "sec_beta_view", '"SecurityID"', sec_ids
    )

    log.info("Fetching security_attribute …")
    sheets["security_attribute"] = _fetch_by_security_ids(
        "security_attribute", "security_id", sec_ids
    )

    # ── Write Excel ───────────────────────────────────────────────────────────

    date_str = as_of_date.strftime("%Y%m%d")
    out_path = EXCEL_DIR / f"dump_{account_id}_{date_str}.xlsx"

    log.info(f"Writing {out_path} …")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            # Excel does not support timezone-aware datetimes — strip tz from all datetime cols
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
        description="Dump pipeline tables for a given account_id and date to Excel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/dump_account.py --account-id 1003 --date 2026-03-02\n"
        ),
    )
    parser.add_argument("--account-id", dest="account_id", type=int, required=True,
                        help="Account ID to dump")
    parser.add_argument("--date", dest="as_of_date", type=_parse_date, required=True,
                        metavar="YYYY-MM-DD", help="Position date (as_of_date / feed_date)")
    args = parser.parse_args()

    run(account_id=args.account_id, as_of_date=args.as_of_date)
