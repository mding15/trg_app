"""
upload_option_iv.py — Upload implied vol data from a CSV into option_iv.

Upserts on the table key (as_of_date, underlying_sec_id, tenor_years,
moneyness): new keys are inserted, existing keys get iv, underlying,
underlying_price and source overwritten (created_at keeps its original value).
The file is all-or-nothing — if any row fails validation, nothing is written.

CSV columns:
    required  as_of_date (YYYY-MM-DD or M/D/YYYY), underlying_sec_id,
              tenor_years (> 0), moneyness (K/S, > 0), iv (> 0, 0.25 = 25%)
    optional  underlying (ticker), underlying_price, source
    ignored   created_at, and any other column

Usage:
    python maintenance/upload_option_iv.py --dry-run          # latest option_iv*.csv
    python maintenance/upload_option_iv.py --file option_iv_20260929.csv
    python maintenance/upload_option_iv.py --file option_iv_20260929.csv --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection
from _paths import CSV_DIR

DEFAULT_PATTERN = "option_iv*.csv"

KEY_COLS = ["as_of_date", "underlying_sec_id", "tenor_years", "moneyness"]
REQUIRED_COLS = KEY_COLS + ["iv"]
OPTIONAL_COLS = ["underlying", "underlying_price", "source"]
TABLE_COLS = REQUIRED_COLS + OPTIONAL_COLS
_POSITIVE_COLS = ["tenor_years", "moneyness", "iv"]


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("upload_option_iv")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _resolve_path(file: str) -> Path:
    p = Path(file)
    if p.exists():
        return p
    return CSV_DIR / file


def _latest_csv() -> Path | None:
    """Latest option_iv*.csv by name — name files option_iv_<YYYYMMDD...>.csv so
    the newest sorts last (mtime would change whenever an older file is edited)."""
    files = sorted(CSV_DIR.glob(DEFAULT_PATTERN), key=lambda f: f.name)
    return files[-1] if files else None


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Return (normalized frame with TABLE_COLS, list of error messages).

    Row numbers in messages are CSV line numbers (header = line 1).
    """
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        return df, [f"missing required column(s): {', '.join(missing)}"]

    df = df.copy()
    for c in OPTIONAL_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[TABLE_COLS]
    line = pd.Series(df.index + 2, index=df.index)
    errors: list[str] = []

    raw_date = df["as_of_date"]
    df["as_of_date"] = pd.to_datetime(raw_date, format="mixed", errors="coerce").dt.date
    for i in df.index[df["as_of_date"].isna()]:
        errors.append(f"line {line[i]}: bad or missing as_of_date {raw_date[i]!r}")

    df["underlying_sec_id"] = df["underlying_sec_id"].map(
        lambda s: str(s).strip() if pd.notna(s) and str(s).strip() else None)
    for i in df.index[df["underlying_sec_id"].isna()]:
        errors.append(f"line {line[i]}: missing underlying_sec_id")

    for c in _POSITIVE_COLS + ["underlying_price"]:
        raw = df[c]
        df[c] = pd.to_numeric(raw, errors="coerce")
        if c in _POSITIVE_COLS:
            bad = df.index[df[c].isna() | (df[c] <= 0)]
        else:
            bad = df.index[raw.notna() & (df[c].isna() | (df[c] <= 0))]
        for i in bad:
            errors.append(f"line {line[i]}: {c} must be a number > 0, got '{raw[i]}'")

    if not errors:
        dup = df[df.duplicated(KEY_COLS, keep=False)]
        for _, g in dup.groupby(KEY_COLS, sort=False):
            errors.append(f"lines {', '.join(str(line[i]) for i in g.index)}: duplicate key "
                          + ", ".join(f"{c}={g.iloc[0][c]}" for c in KEY_COLS))

    return df, errors


def _records(df: pd.DataFrame) -> list[tuple]:
    return [tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False)]


def count_existing(cur, df: pd.DataFrame) -> int:
    """How many of df's keys are already in option_iv."""
    cur.execute("CREATE TEMP TABLE _iv_keys (as_of_date date, underlying_sec_id varchar(20), "
                "tenor_years numeric, moneyness numeric) ON COMMIT DROP")
    execute_values(cur, "INSERT INTO _iv_keys VALUES %s", _records(df[KEY_COLS]))
    cur.execute(f"SELECT count(*) FROM _iv_keys k JOIN option_iv o USING ({', '.join(KEY_COLS)})")
    return cur.fetchone()[0]


def upsert(cur, df: pd.DataFrame) -> tuple[int, int]:
    """Upsert df into option_iv. Returns (inserted, updated)."""
    cols = ", ".join(f'"{c}"' for c in TABLE_COLS)
    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in TABLE_COLS if c not in KEY_COLS)
    rows = execute_values(
        cur,
        f"""
        INSERT INTO option_iv ({cols}) VALUES %s
        ON CONFLICT ({', '.join(KEY_COLS)}) DO UPDATE SET {updates}
        RETURNING (xmax = 0) AS inserted
        """,
        _records(df),
        fetch=True,
    )
    inserted = sum(1 for (ins,) in rows if ins)
    return inserted, len(rows) - inserted


def run(file: str | None, dry_run: bool) -> None:
    log = _setup_logger()

    if file is None:
        path = _latest_csv()
        if path is None:
            log.error(f"No {DEFAULT_PATTERN} found in {CSV_DIR}")
            sys.exit(1)
        log.info(f"No --file given; using latest {DEFAULT_PATTERN}: {path.name}")
    else:
        path = _resolve_path(file)
    if not path.exists():
        log.error(f"File not found: {path}")
        sys.exit(1)

    raw = pd.read_csv(path, encoding="utf-8-sig", dtype={"underlying_sec_id": str})
    log.info(f"Loaded {len(raw)} row(s) from {path}")
    if raw.empty:
        return

    df, errors = validate(raw)
    if errors:
        log.error(f"{len(errors)} problem(s) — nothing written:")
        for e in errors:
            log.error(f"  {e}")
        sys.exit(1)

    dates = sorted(df["as_of_date"].unique())
    log.info(f"  as_of_date(s): {', '.join(str(d) for d in dates)}")
    for sec_id, g in df.groupby("underlying_sec_id"):
        name = g["underlying"].dropna().iloc[0] if g["underlying"].notna().any() else ""
        log.info(f"  {sec_id:<12} {name:<8} {len(g):>4} point(s)  iv {g['iv'].min():.4f} - {g['iv'].max():.4f}")

    log.info("─" * 60)
    with pg_connection() as conn:
        with conn.cursor() as cur:
            if dry_run:
                n_existing = count_existing(cur, df)
                conn.rollback()
                log.info("DRY RUN — no data written")
                log.info(f"  would insert  {len(df) - n_existing}")
                log.info(f"  would update  {n_existing}")
                return
            inserted, updated = upsert(cur, df)
        conn.commit()
    log.info(f"Done.  inserted {inserted}, updated {updated}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload implied vol data from a CSV into option_iv (upsert).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/upload_option_iv.py --dry-run\n"
            "  python maintenance/upload_option_iv.py --file option_iv_20260929.csv\n"
        ),
    )
    parser.add_argument(
        "--file", default=None, metavar="FILENAME",
        help=f"CSV filename inside data/maintenance/CSV/ (or a full path); default: latest {DEFAULT_PATTERN}",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate and show what would be inserted/updated without writing",
    )
    args = parser.parse_args()

    run(args.file, args.dry_run)


if __name__ == "__main__":
    main()
