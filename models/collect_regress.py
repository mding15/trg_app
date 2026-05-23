# -*- coding: utf-8 -*-
"""
collect_regress.py — Collect regression results across model types.

For a given model_id, reads regression CSVs from matching subfolders under
MODEL_DIR/model_id, concatenates them, and writes one output CSV per type.
Columns are merged with an outer join (missing values filled with NaN).
A 'submodel_id' column identifies the source submodel for each row.
Duplicate SecurityIDs across submodels are reported to stdout.

Collections
-----------
    equity       — equity.* folders,                              regress_df.csv     → regress_df.equity.csv
    spread       — spread.* / spreadgeneric.* folders,            betas.csv          → regress_df.spread.csv
    alternatives — privateequity.* / privatecredit.* / realestate.* folders,
                   private_equity.csv / betas.csv / betas.csv    → regress_df.alternatives.csv

Usage
-----
    python models/collect_regress.py
    python models/collect_regress.py M_20251231

Output
------
    models/test_output/regress_df.equity.csv         (overwritten each run)
    models/test_output/regress_df.spread.csv         (overwritten each run)
    models/test_output/regress_df.alternatives.csv   (overwritten each run)
"""

import sys
import argparse
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trg_config import config

TEST_OUTPUT_DIR = Path(__file__).resolve().parent / 'test_output'
DEFAULT_MODEL_ID = 'M_20251231'


def collect(model_id: str, sources: list[tuple[str, str]], id_col: str = 'SecurityID') -> pd.DataFrame | None:
    """Concatenate CSVs from matching subfolders.

    sources: list of (folder_prefix, filename) pairs. Each subfolder is matched
             against exactly one prefix; its filename is read independently.
    id_col:  name of the security ID column in source files; renamed to 'SecurityID'.
    """
    model_dir = config['MODEL_DIR'] / model_id

    if not model_dir.exists():
        print(f'  Error: model directory not found: {model_dir}')
        return None

    # Build a map from each subfolder to the filename it should use
    prefix_file: dict[str, str] = {pfx: fname for pfx, fname in sources}

    folder_file: list[tuple[Path, str]] = sorted([
        (p, next(fname for pfx, fname in sources if p.name.lower().startswith(pfx)))
        for p in model_dir.iterdir()
        if p.is_dir() and any(p.name.lower().startswith(pfx) for pfx, _ in sources)
    ], key=lambda x: x[0].name)

    if not folder_file:
        print(f'  No matching subfolders found (prefixes: {[pfx for pfx, _ in sources]})')
        return None

    print(f'  Found {len(folder_file)} submodel(s): {[f.name for f, _ in folder_file]}')

    frames = []
    for folder, filename in folder_file:
        csv_path = folder / filename
        if not csv_path.exists():
            print(f'  WARNING: {filename} not found in {folder.name}, skipping')
            continue

        df = pd.read_csv(csv_path)
        if id_col != 'SecurityID' and id_col in df.columns:
            df = df.rename(columns={id_col: 'SecurityID'})
        df.insert(0, 'submodel_id', folder.name)
        frames.append(df)
        print(f'  {folder.name}: {len(df)} securities')

    if not frames:
        print('  No data loaded.')
        return None

    combined = pd.concat(frames, ignore_index=True, join='outer')

    dup_mask = combined.duplicated(subset='SecurityID', keep=False)
    dupes = combined.loc[dup_mask, ['submodel_id', 'SecurityID']]
    if not dupes.empty:
        dup_ids = sorted(dupes['SecurityID'].unique().tolist())
        print(f'  Duplicate SecurityIDs ({len(dup_ids)}):')
        for sid in dup_ids:
            sources = dupes.loc[dupes['SecurityID'] == sid, 'submodel_id'].tolist()
            print(f'    {sid}: {sources}')
    else:
        print('  No duplicate SecurityIDs.')

    return combined


def main():
    parser = argparse.ArgumentParser(description='Collect regression results across model types.')
    parser.add_argument('model_id', nargs='?', default=DEFAULT_MODEL_ID,
                        help=f'Model ID (default: {DEFAULT_MODEL_ID})')
    args = parser.parse_args()

    model_id = args.model_id
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    any_error = False

    print(f'Model: {model_id}')

    # Equity
    print('\n[equity]')
    equity_df = collect(model_id, [('equity.', 'regress_df.csv')])
    if equity_df is not None:
        out_path = TEST_OUTPUT_DIR / 'regress_df.equity.csv'
        equity_df.to_csv(out_path, index=False)
        print(f'  Output: {out_path}  ({len(equity_df)} rows)')
    else:
        any_error = True

    # Spread
    print('\n[spread]')
    spread_df = collect(model_id, [('spread.', 'betas.csv'), ('spreadgeneric.', 'betas.csv')])
    if spread_df is not None:
        out_path = TEST_OUTPUT_DIR / 'regress_df.spread.csv'
        spread_df.to_csv(out_path, index=False)
        print(f'  Output: {out_path}  ({len(spread_df)} rows)')
    else:
        any_error = True

    # Alternatives
    print('\n[alternatives]')
    alt_df = collect(
        model_id,
        [('privateequity.', 'private_equity.csv'), ('privatecredit.', 'betas.csv'), ('realestate.', 'betas.csv')],
        id_col='security_id',
    )
    if alt_df is not None:
        out_path = TEST_OUTPUT_DIR / 'regress_df.alternatives.csv'
        alt_df.to_csv(out_path, index=False)
        print(f'  Output: {out_path}  ({len(alt_df)} rows)')
    else:
        any_error = True

    if any_error:
        sys.exit(1)


if __name__ == '__main__':
    main()
