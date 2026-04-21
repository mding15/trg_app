"""
calc_benchmark.py — Calculate benchmark index values for a given date and upsert
into benchmark_hist.

Steps:
  1. Load all active benchmarks (is_active = TRUE) from the benchmark table.
  2. For each benchmark:
       proxy    — single security return: (close_today - close_prev) / close_prev
       internal — weighted-average return across benchmark_weights securities
  3. Close prices are fill-forwarded within a 5-business-day window per security.
     Benchmarks with any still-missing price are skipped with a warning.
  4. new_value = prev_index_value * (1 + return)
     If no prior index value exists, seed 100 on the previous business day
     and insert it into benchmark_hist.
  5. Upsert (delete + insert) the new value into benchmark_hist.

Usage:
    python process2/calc_benchmark.py --date 2026-04-20
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

FILL_WINDOW = 5  # business days for fill-forward price lookup


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("calc_benchmark")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Date helpers ──────────────────────────────────────────────────────────────

def _prev_bday(d: date) -> date:
    return (pd.Timestamp(d) - pd.offsets.BDay(1)).date()

def _bday_range_end(end: date, periods: int) -> list[date]:
    return list(pd.bdate_range(end=end, periods=periods).date)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _fetch(sql: str, params: tuple) -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _get_active_benchmarks() -> pd.DataFrame:
    return _fetch(
        "SELECT benchmark_id, benchmark_name, source_provider, security_id "
        "FROM benchmark WHERE is_active = TRUE",
        (),
    )


def _get_benchmark_weights(benchmark_id: int) -> pd.DataFrame:
    return _fetch(
        "SELECT security_id, weight FROM benchmark_weights "
        "WHERE benchmark_id = %s AND security_id IS NOT NULL",
        (benchmark_id,),
    )


def _fetch_prices(security_ids: list[str], from_date: date, to_date: date) -> dict[str, dict[date, float]]:
    """Return {security_id: {date: close}} for all securities in the date window."""
    if not security_ids:
        return {}
    df = _fetch(
        'SELECT "SecurityID", "Date", "Close" FROM current_price '
        'WHERE "SecurityID" = ANY(%s) AND "Date" BETWEEN %s AND %s',
        (security_ids, from_date, to_date),
    )
    price_map: dict[str, dict[date, float]] = {}
    for _, row in df.iterrows():
        sid = row["SecurityID"]
        d   = row["Date"] if isinstance(row["Date"], date) else row["Date"].date()
        if row["Close"] is not None:
            price_map.setdefault(sid, {})[d] = float(row["Close"])
    return price_map


def _get_ffill_price(price_map: dict, security_id: str, target_date: date) -> float | None:
    """Return the most recent Close at or before target_date within FILL_WINDOW business days."""
    sec_prices = price_map.get(security_id, {})
    for d in reversed(_bday_range_end(target_date, FILL_WINDOW)):
        if d in sec_prices:
            return sec_prices[d]
    return None


def _get_prev_index_value(benchmark_id: int, calc_date: date) -> tuple[float | None, date | None]:
    df = _fetch(
        "SELECT date, value FROM benchmark_hist "
        "WHERE benchmark_id = %s AND date < %s ORDER BY date DESC LIMIT 1",
        (benchmark_id, calc_date),
    )
    if df.empty:
        return None, None
    row = df.iloc[0]
    d = row["date"] if isinstance(row["date"], date) else row["date"].date()
    return float(row["value"]), d


def _upsert_hist(benchmark_id: int, d: date, value: float) -> None:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM benchmark_hist WHERE benchmark_id = %s AND date = %s",
                (benchmark_id, d),
            )
            cur.execute(
                "INSERT INTO benchmark_hist (benchmark_id, date, value) VALUES (%s, %s, %s)",
                (benchmark_id, d, value),
            )
        conn.commit()


# ── Return calculators ────────────────────────────────────────────────────────

def _proxy_return(
    bm_name: str,
    security_id: str,
    price_map: dict,
    calc_date: date,
    prev_bday: date,
    log: logging.Logger,
) -> float | None:
    p_today = _get_ffill_price(price_map, security_id, calc_date)
    p_prev  = _get_ffill_price(price_map, security_id, prev_bday)
    if p_today is None or p_prev is None:
        log.warning(
            f"  [{bm_name}] missing price for {security_id} "
            f"(today={p_today}, prev={p_prev}) — skipping"
        )
        return None
    return (p_today - p_prev) / p_prev


def _internal_return(
    bm_name: str,
    weights_df: pd.DataFrame,
    price_map: dict,
    calc_date: date,
    prev_bday: date,
    log: logging.Logger,
) -> float | None:
    weighted_sum = 0.0
    total_weight = 0.0
    for _, row in weights_df.iterrows():
        sid = row["security_id"]
        w   = float(row["weight"])
        p_today = _get_ffill_price(price_map, sid, calc_date)
        p_prev  = _get_ffill_price(price_map, sid, prev_bday)
        if p_today is None or p_prev is None:
            log.warning(
                f"  [{bm_name}] missing price for {sid} "
                f"(today={p_today}, prev={p_prev}) — skipping benchmark"
            )
            return None
        weighted_sum += w * (p_today - p_prev) / p_prev
        total_weight += w
    if total_weight == 0:
        log.warning(f"  [{bm_name}] total weight is zero — skipping")
        return None
    return weighted_sum / total_weight


# ── Core ──────────────────────────────────────────────────────────────────────

def run(calc_date: date, log: logging.Logger) -> None:
    prev_bday = _prev_bday(calc_date)
    # fetch window covers FILL_WINDOW days ending at both prev_bday and calc_date
    fetch_from = (pd.Timestamp(prev_bday) - pd.offsets.BDay(FILL_WINDOW - 1)).date()

    log.info(f"calc_date={calc_date}  prev_bday={prev_bday}  price_window=[{fetch_from}, {calc_date}]")

    benchmarks = _get_active_benchmarks()
    log.info(f"Found {len(benchmarks)} active benchmark(s)")

    # ── Collect all security_ids and weights up front ─────────────────────────
    all_sec_ids: set[str] = set()
    weights_cache: dict[int, pd.DataFrame] = {}

    for _, bm in benchmarks.iterrows():
        sp = (bm["source_provider"] or "").lower()
        if sp == "proxy":
            if bm["security_id"]:
                all_sec_ids.add(bm["security_id"])
        elif sp == "internal":
            wdf = _get_benchmark_weights(bm["benchmark_id"])
            weights_cache[bm["benchmark_id"]] = wdf
            all_sec_ids.update(wdf["security_id"].dropna().tolist())

    # ── Fetch all prices in a single query ────────────────────────────────────
    price_map = _fetch_prices(list(all_sec_ids), fetch_from, calc_date)
    log.info(f"Fetched prices for {len(price_map)} security(ies)")

    # ── Process each benchmark ────────────────────────────────────────────────
    ok = skipped = 0

    for _, bm in benchmarks.iterrows():
        bid  = int(bm["benchmark_id"])
        name = bm["benchmark_name"]
        sp   = (bm["source_provider"] or "").lower()

        log.info(f"Processing benchmark_id={bid}  '{name}'  source={sp}")

        # -- calculate index return -------------------------------------------
        if sp == "proxy":
            sid = bm["security_id"]
            if not sid:
                log.warning(f"  [{name}] proxy benchmark has no security_id — skipping")
                skipped += 1
                continue
            index_return = _proxy_return(name, sid, price_map, calc_date, prev_bday, log)

        elif sp == "internal":
            wdf = weights_cache.get(bid, pd.DataFrame())
            if wdf.empty:
                log.warning(f"  [{name}] no weights found — skipping")
                skipped += 1
                continue
            index_return = _internal_return(name, wdf, price_map, calc_date, prev_bday, log)

        else:
            log.warning(f"  [{name}] unknown source_provider '{sp}' — skipping")
            skipped += 1
            continue

        if index_return is None:
            skipped += 1
            continue

        # -- get / seed previous index value ----------------------------------
        prev_value, _ = _get_prev_index_value(bid, calc_date)
        if prev_value is None:
            seed_date = _prev_bday(calc_date)
            log.info(f"  [{name}] no prior index value — seeding 100 on {seed_date}")
            _upsert_hist(bid, seed_date, 100.0)
            prev_value = 100.0

        # -- compute and store new value --------------------------------------
        new_value = prev_value * (1.0 + index_return)
        log.info(
            f"  [{name}] return={index_return:+.6f}  "
            f"prev={prev_value:.4f}  new={new_value:.4f}"
        )
        _upsert_hist(bid, calc_date, new_value)
        ok += 1

    log.info("─" * 60)
    log.info(f"Done.  calculated={ok}  skipped={skipped}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    def _parse_date(s: str) -> date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser = argparse.ArgumentParser(
        description="Calculate benchmark index values for a given date.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python process2/calc_benchmark.py --date 2026-04-20\n",
    )
    parser.add_argument(
        "--date", dest="calc_date", type=_parse_date, required=True,
        metavar="YYYY-MM-DD", help="Calculation date",
    )
    args = parser.parse_args()

    log = _setup_logger()
    run(calc_date=args.calc_date, log=log)
