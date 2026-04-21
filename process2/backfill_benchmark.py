"""
backfill_benchmark.py — Backfill benchmark index values for a date range.

Iterates over every business day in [from_date, to_date] and upserts
calculated values into benchmark_hist.

All price data is fetched in one query up front; daily logic is identical
to calc_benchmark.py (fill-forward within 5 business days, seed 100 if no
prior index value exists).

Usage:
    python process2/backfill_benchmark.py --benchmark-id 3 --from-date 2025-01-01 --to-date 2026-04-20
    python process2/backfill_benchmark.py --all             --from-date 2025-01-01 --to-date 2026-04-20
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from process2.calc_benchmark import (
    _get_active_benchmarks,
    _get_benchmark_weights,
    _get_prev_index_value,
    _internal_return,
    _prev_bday,
    _proxy_return,
    _setup_logger,
    _upsert_hist,
    _fetch,
    _fetch_prices,
    FILL_WINDOW,
)


# ── Fetch single benchmark ────────────────────────────────────────────────────

def _get_benchmark(benchmark_id: int) -> dict | None:
    df = _fetch(
        "SELECT benchmark_id, benchmark_name, source_provider, security_id "
        "FROM benchmark WHERE benchmark_id = %s",
        (benchmark_id,),
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


# ── Per-benchmark backfill ────────────────────────────────────────────────────

def _backfill_one(
    bm: dict,
    weights_df: pd.DataFrame,
    price_map: dict,
    bdays: list[date],
    log: logging.Logger,
) -> tuple[int, int]:
    """Process all business days for a single benchmark. Returns (ok, skipped)."""
    bid  = int(bm["benchmark_id"])
    name = bm["benchmark_name"]
    sp   = (bm["source_provider"] or "").lower()

    ok = skipped = 0

    for calc_date in bdays:
        prev_bday = _prev_bday(calc_date)

        if sp == "proxy":
            index_return = _proxy_return(
                name, bm["security_id"], price_map, calc_date, prev_bday, log
            )
        else:
            index_return = _internal_return(
                name, weights_df, price_map, calc_date, prev_bday, log
            )

        if index_return is None:
            skipped += 1
            continue

        prev_value, _ = _get_prev_index_value(bid, calc_date)
        if prev_value is None:
            seed_date = _prev_bday(calc_date)
            log.info(f"  [{name}][{calc_date}] no prior index value — seeding 100 on {seed_date}")
            _upsert_hist(bid, seed_date, 100.0)
            prev_value = 100.0

        new_value = prev_value * (1.0 + index_return)
        log.info(
            f"  [{name}][{calc_date}] return={index_return:+.6f}  "
            f"prev={prev_value:.4f}  new={new_value:.4f}"
        )
        _upsert_hist(bid, calc_date, new_value)
        ok += 1

    return ok, skipped


# ── Core ──────────────────────────────────────────────────────────────────────

def run(
    from_date: date,
    to_date: date,
    log: logging.Logger,
    benchmark_id: int | None = None,
) -> None:
    bdays      = list(pd.bdate_range(start=from_date, end=to_date).date)
    fetch_from = (pd.Timestamp(from_date) - pd.offsets.BDay(FILL_WINDOW)).date()

    log.info(f"Date range: [{from_date}, {to_date}]  ({len(bdays)} business day(s))")

    # ── Resolve benchmark list ────────────────────────────────────────────────
    if benchmark_id is not None:
        bm = _get_benchmark(benchmark_id)
        if bm is None:
            log.error(f"benchmark_id={benchmark_id} not found — aborting")
            sys.exit(1)
        benchmarks = pd.DataFrame([bm])
    else:
        benchmarks = _get_active_benchmarks()
        log.info(f"Found {len(benchmarks)} active benchmark(s)")

    # ── Collect all security_ids and weights up front ─────────────────────────
    all_sec_ids: set[str] = set()
    weights_cache: dict[int, pd.DataFrame] = {}
    valid_benchmarks = []

    for _, bm in benchmarks.iterrows():
        bid = int(bm["benchmark_id"])
        sp  = (bm["source_provider"] or "").lower()

        if sp == "proxy":
            sid = bm["security_id"]
            if not sid:
                log.warning(f"  [{bm['benchmark_name']}] proxy has no security_id — skipping")
                continue
            all_sec_ids.add(sid)

        elif sp == "internal":
            wdf = _get_benchmark_weights(bid)
            if wdf.empty:
                log.warning(f"  [{bm['benchmark_name']}] no weights found — skipping")
                continue
            weights_cache[bid] = wdf
            all_sec_ids.update(wdf["security_id"].dropna().tolist())

        else:
            log.warning(f"  [{bm['benchmark_name']}] unknown source_provider '{sp}' — skipping")
            continue

        valid_benchmarks.append(bm.to_dict())

    if not valid_benchmarks:
        log.error("No valid benchmarks to process — aborting")
        sys.exit(1)

    # ── Fetch all prices in one query ─────────────────────────────────────────
    price_map = _fetch_prices(list(all_sec_ids), fetch_from, to_date)
    log.info(f"Fetched prices for {len(price_map)} security(ies) from {fetch_from} to {to_date}")

    # ── Backfill each benchmark ───────────────────────────────────────────────
    total_ok = total_skipped = 0

    for bm in valid_benchmarks:
        bid = int(bm["benchmark_id"])
        log.info(f"─ benchmark_id={bid}  '{bm['benchmark_name']}'  source={bm['source_provider']}")
        wdf = weights_cache.get(bid, pd.DataFrame())
        ok, skipped = _backfill_one(bm, wdf, price_map, bdays, log)
        total_ok      += ok
        total_skipped += skipped

    log.info("─" * 60)
    log.info(f"Done.  calculated={total_ok}  skipped={total_skipped}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    def _parse_date(s: str) -> date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser = argparse.ArgumentParser(
        description="Backfill benchmark index values for a date range.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python process2/backfill_benchmark.py --benchmark-id 3 "
            "--from-date 2025-01-01 --to-date 2026-04-20\n"
            "  python process2/backfill_benchmark.py --all "
            "--from-date 2025-01-01 --to-date 2026-04-20\n"
        ),
    )

    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--benchmark-id", dest="benchmark_id", type=int,
                        help="Single benchmark ID to backfill")
    target.add_argument("--all", action="store_true",
                        help="Backfill all active benchmarks")

    parser.add_argument("--from-date", dest="from_date", type=_parse_date, required=True,
                        metavar="YYYY-MM-DD", help="Start date (inclusive)")
    parser.add_argument("--to-date", dest="to_date", type=_parse_date, required=True,
                        metavar="YYYY-MM-DD", help="End date (inclusive)")
    args = parser.parse_args()

    if args.from_date > args.to_date:
        parser.error("--from-date must be on or before --to-date")

    log = _setup_logger()
    run(
        from_date=args.from_date,
        to_date=args.to_date,
        log=log,
        benchmark_id=args.benchmark_id,  # None when --all
    )
