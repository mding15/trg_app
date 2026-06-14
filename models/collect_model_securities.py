# -*- coding: utf-8 -*-
"""
collect_model_securities.py — Collect distribution statistics across all model types.

For a given model_id, reads distribution CSVs from matching subfolders, computes
per-security statistics via stat_utils.dist_stat(), and writes one combined CSV
and inserts results into the model_security_stat database table.

For repeated security_id within the same model, folders are sorted ascending and
the last occurrence is kept.

Model / category / folder / file mapping
-----------------------------------------
    Equity          PRICE    Equity.*            simulated_dist.csv
    IR              IR       IR.*                ir_dist.csv
    Spread          SPREAD   SpreadGeneric.*     dist.csv
    Spread          SPREAD   Spread.*            dist.csv
    UF              YIELD    UF_Model.*          dist.csv
    PrivateEquity   PRICE    PrivateEquity.*     dist.csv
    PrivateCredit   PRICE    PrivateCredit.*     dist.csv
    RealEstate      PRICE    RealEstate.*        dist.csv
    FX              FX       FX.*                dist.csv
    Proxy           PRICE    Proxy.*             dist.csv
    VIX             VIX      VIX                 dist.csv

Usage
-----
    python models/collect_model_securities.py
    python models/collect_model_securities.py M_20251231

Output
------
    models/test_output/model_securities.{model_id}.csv   (overwritten each run)
    DB table: model_security_stat                        (upserted each run)
"""

import sys
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trg_config import config
from database2 import pg_connection
from utils import stat_utils

TEST_OUTPUT_DIR = Path(__file__).resolve().parent / 'test_output'
DEFAULT_MODEL_ID = 'M_20251231'

# (model_name, category, folder_prefix, filename)
# Spread is listed twice — SpreadGeneric first, then Spread — so that when
# deduplicating by ascending folder-name sort, a security in both keeps its
# Spread.* entry (alphabetically later = kept by keep='last').
MODEL_SPECS: list[tuple[str, str, str, str]] = [
    ('Equity',        'PRICE',  'Equity.',        'simulated_dist.csv'),
    ('IR',            'IR',     'IR.',             'ir_dist.csv'),
    ('Spread',        'SPREAD', 'SpreadGeneric.',  'dist.csv'),
    ('Spread',        'SPREAD', 'Spread.',         'dist.csv'),
    ('UF',            'YIELD',  'UF_Model.',       'dist.csv'),
    ('PrivateEquity', 'PRICE',  'PrivateEquity.',  'dist.csv'),
    ('PrivateCredit', 'PRICE',  'PrivateCredit.',  'dist.csv'),
    ('RealEstate',    'PRICE',  'RealEstate.',     'dist.csv'),
    ('FX',            'FX',     'FX.',             'dist.csv'),
    ('Proxy',         'PRICE',  'Proxy.',          'dist.csv'),
    ('VIX',           'VIX',    'VIX',             'dist.csv'),
]

# Rename dist_stat() column names to DB-safe names
_STAT_COL_RENAME = {
    'q-1%':  'q_1pct',
    'q-5%':  'q_5pct',
    'q-50%': 'q_50pct',
    'q-95%': 'q_95pct',
    'q-99%': 'q_99pct',
    'es-5%': 'es_5pct',
    'es-1%': 'es_1pct',
}

_STAT_COLS = ['min', 'max', 'mean', 'std', 'q_1pct', 'q_5pct', 'q_50pct', 'q_95pct', 'q_99pct', 'es_5pct', 'es_1pct']
_TABLE_COLS = ['model_id', 'model', 'category', 'folder', 'security_id'] + _STAT_COLS


# ── DB ────────────────────────────────────────────────────────────────────────

def insert_to_db(df: pd.DataFrame, model_id: str) -> int:
    """Upsert rows into model_security_stat. Returns number of rows processed."""
    db_df = df.copy()
    db_df.insert(0, 'model_id', model_id)
    db_df = db_df[[c for c in _TABLE_COLS if c in db_df.columns]]
    db_df = db_df.replace({np.nan: None})

    col_sql      = ', '.join(f'"{c}"' for c in _TABLE_COLS)
    placeholders = ', '.join(f'%({c})s' for c in _TABLE_COLS)
    update_sql   = ', '.join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in _TABLE_COLS if c not in ('model_id', 'model', 'security_id')
    )
    sql = f"""
        INSERT INTO model_security_stat ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT (model_id, model, security_id) DO UPDATE SET {update_sql}
    """

    rows = db_df.to_dict(orient='records')
    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows)
        conn.commit()
    return len(rows)


# ── Collection ────────────────────────────────────────────────────────────────

def collect_stats(
    model_dir: Path,
    model_name: str,
    category: str,
    folder_prefix: str,
    filename: str,
) -> list[pd.DataFrame]:
    """
    Find all subfolders matching folder_prefix, read filename from each,
    compute dist_stat(), and return a list of DataFrames with model/category/folder columns.
    """
    matched = sorted(
        [p for p in model_dir.iterdir()
         if p.is_dir() and p.name.lower().startswith(folder_prefix.lower())],
        key=lambda p: p.name,
    )

    if not matched:
        return []

    frames = []
    for folder in matched:
        csv_path = folder / filename
        if not csv_path.exists():
            print(f'  WARNING: {filename} not found in {folder.name}, skipping')
            continue

        try:
            dist = pd.read_csv(csv_path, index_col=0)
        except Exception as e:
            print(f'  WARNING: could not read {csv_path.name} in {folder.name}: {e}')
            continue

        if dist.empty or dist.shape[1] == 0:
            print(f'  WARNING: {folder.name}/{filename} is empty, skipping')
            continue

        stats = stat_utils.dist_stat(dist)
        stats = stats.reset_index().rename(columns={'SecurityID': 'security_id'})
        stats = stats.rename(columns=_STAT_COL_RENAME)
        stats.insert(0, 'model',    model_name)
        stats.insert(1, 'category', category)
        stats.insert(2, 'folder',   folder.name)
        frames.append(stats)
        print(f'  {folder.name}: {len(stats)} securities')

    return frames


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Collect distribution statistics across all model types.'
    )
    parser.add_argument(
        'model_id', nargs='?', default=DEFAULT_MODEL_ID,
        help=f'Model ID (default: {DEFAULT_MODEL_ID})',
    )
    args = parser.parse_args()
    model_id = args.model_id

    model_dir = config['MODEL_DIR'] / model_id
    if not model_dir.exists():
        print(f'Error: model directory not found: {model_dir}')
        sys.exit(1)

    print(f'Model: {model_id}')
    print(f'Directory: {model_dir}')

    # Collect all frames grouped by model name
    frames_by_model: dict[str, list[pd.DataFrame]] = {}
    for model_name, category, folder_prefix, filename in MODEL_SPECS:
        print(f'\n[{model_name}]  category={category!r}  prefix={folder_prefix!r}  file={filename!r}')
        frames = collect_stats(model_dir, model_name, category, folder_prefix, filename)
        if frames:
            frames_by_model.setdefault(model_name, []).extend(frames)
        else:
            print('  No matching folders found.')

    if not frames_by_model:
        print('\nNo data collected.')
        sys.exit(1)

    # Deduplicate within each model: sort by folder ascending, keep last
    deduped: list[pd.DataFrame] = []
    for model_name, frames in frames_by_model.items():
        df = pd.concat(frames, ignore_index=True)
        df = df.sort_values('folder', ascending=True)
        before = len(df)
        df = df.drop_duplicates(subset=['model', 'security_id'], keep='last')
        dropped = before - len(df)
        if dropped:
            print(f'\n[{model_name}] {dropped} duplicate security_id(s) removed (kept last folder).')
        deduped.append(df)

    result = pd.concat(deduped, ignore_index=True)

    # ── CSV ───────────────────────────────────────────────────────────────────
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = TEST_OUTPUT_DIR / f'model_securities.{model_id}.csv'
    result.to_csv(out_path, index=False)
    print(f'\nTotal: {len(result)} securities across {result["model"].nunique()} model(s)')
    print(f'CSV: {out_path}')

    # ── DB ────────────────────────────────────────────────────────────────────
    n = insert_to_db(result, model_id)
    print(f'DB:  {n} rows upserted into model_security_stat')


if __name__ == '__main__':
    main()
