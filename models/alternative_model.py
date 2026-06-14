"""
alternative_model.py — Alternative investment risk model.
"""
import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

log = logging.getLogger(__name__)

from database2 import pg_connection
from utils import var_utils, stat_utils

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "test_output"

DATA = {}


# ── DB queries ────────────────────────────────────────────────────────────────

def get_model_info() -> pd.DataFrame:
    """Fetch all rows from alternative_model."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM alternative_model ORDER BY security_id")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_model_info_for(security_ids: list) -> pd.DataFrame:
    """Fetch alternative_model rows for the given security_ids only."""
    if not security_ids:
        return pd.DataFrame()
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM alternative_model WHERE security_id = ANY(%s) ORDER BY security_id",
                (security_ids,),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def corefactor_dist(model_info: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch corefactor price distributions for all proxy securities in the model.

    The proxy_id column in model_info contains the security IDs to look up.
    """
    proxy_ids = model_info["proxy_id"].dropna().unique().tolist()
    dist = var_utils.get_dist(proxy_ids, category="PRICE")
    return dist


# ── Model ─────────────────────────────────────────────────────────────────────

def alternative_model(model_info: pd.DataFrame, corefactor_dist: pd.DataFrame) -> pd.DataFrame:
    """
    Generate return distributions for each alternative security.

    dist[security_id] = beta * corefactor_dist[proxy_id] + N(0, sigma)

    required columns in model_info: security_id, proxy_id, beta, sigma
    """
    N = len(corefactor_dist)
    series = {}

    for row in model_info.itertuples(index=False):
        sec_id  = row.security_id
        proxy   = row.proxy_id
        beta    = row.beta
        sigma   = row.sigma

        if pd.isna(beta):
            log.warning("Skipping %s — beta is null", sec_id)
            continue
        if proxy not in corefactor_dist.columns:
            log.warning("Skipping %s — proxy_id '%s' not in corefactor_dist", sec_id, proxy)
            continue

        systematic   = corefactor_dist[proxy].values * float(beta)
        idiosyncratic = np.random.normal(0, float(sigma) if not pd.isna(sigma) else 0.0, N)
        series[sec_id] = systematic + idiosyncratic

    return pd.DataFrame(series, index=corefactor_dist.index)


def alternative_model_unadj(model_info: pd.DataFrame, corefactor_dist: pd.DataFrame) -> pd.DataFrame:
    """
    Generate return distributions for each alternative security using unadjusted volatilities.

    dist[security_id] = beta * corefactor_dist[proxy_id] + N(0, sigma)
    """
    # N = len(corefactor_dist)
    # series = {}

    mi_unadj = model_info.copy()
    mi_unadj["beta"] = mi_unadj["proxy_correl"] * mi_unadj["unadj_vol"] / mi_unadj["proxy_vol"]
    mi_unadj["sigma"] = mi_unadj["unadj_vol"] * np.sqrt(1 - mi_unadj["proxy_correl"] ** 2)

    return alternative_model(mi_unadj, corefactor_dist)

    # for row in model_info.itertuples(index=False):
    #     sec_id    = row.security_id
    #     proxy     = row.proxy_id
    #     unadj_vol = row.unadj_vol
    #     rho       = row.proxy_correl
    #     proxy_vol = row.proxy_vol
        
    #     if pd.isna(unadj_vol):
    #         log.warning("Skipping %s — unadjusted volatility is null", sec_id)
    #         continue
    #     if proxy not in corefactor_dist.columns:
    #         log.warning("Skipping %s — proxy_id '%s' not in corefactor_dist", sec_id, proxy)
    #         continue

    #     beta  = (rho * unadj_vol) / proxy_vol if proxy_vol != 0 else 0.0
    #     sigma = unadj_vol * np.sqrt(1 - rho**2)

    #     systematic   = corefactor_dist[proxy].values * float(beta)
    #     idiosyncratic = np.random.normal(0, float(sigma) if not pd.isna(sigma) else 0.0, N)
    #     series[sec_id] = systematic + idiosyncratic

    # return pd.DataFrame(series, index=corefactor_dist.index)

def alternative_model_adhoc(correl: dict) -> pd.DataFrame:
    """
    Generate return distributions for a subset of alternatives with overridden correlations.

    correl: {security_id: proxy_correl_value}
    Securities not present in correl are dropped. beta and sigma are recomputed
    from the supplied proxy_correl using the same formula as alternative_model_unadj.
    """
    model_info = get_model_info_for(list(correl.keys()))
    if model_info.empty:
        return pd.DataFrame()

    model_info["proxy_correl"] = model_info["security_id"].map(correl)
    model_info["beta"]         = model_info["proxy_correl"] * model_info["unadj_vol"] / model_info["proxy_vol"]
    model_info["sigma"]        = model_info["unadj_vol"] * np.sqrt(1 - model_info["proxy_correl"] ** 2)

    cf_dist = corefactor_dist(model_info)
    return alternative_model(model_info, cf_dist)


# ── DB ───────────────────────────────────────────────────────────────────────

_STAT_COL_RENAME = {
    'q-1%':  'q_1pct',
    'q-5%':  'q_5pct',
    'q-50%': 'q_50pct',
    'q-95%': 'q_95pct',
    'q-99%': 'q_99pct',
    'es-5%': 'es_5pct',
    'es-1%': 'es_1pct',
}

_TABLE_COLS = [
    'model_id', 'model', 'category', 'folder', 'security_id',
    'min', 'max', 'mean', 'std',
    'q_1pct', 'q_5pct', 'q_50pct', 'q_95pct', 'q_99pct',
    'es_5pct', 'es_1pct',
]


def _save_stat_to_db(dist: pd.DataFrame, model_info: pd.DataFrame, model_id: str) -> None:
    """Compute dist_stat() and upsert results into model_security_stat."""
    stats = stat_utils.dist_stat(dist)
    stats = stats.reset_index().rename(columns={'SecurityID': 'security_id'})
    stats = stats.rename(columns=_STAT_COL_RENAME)

    subclass_map = model_info.set_index('security_id')['asset_subclass']
    stats['model']    = stats['security_id'].map(subclass_map)
    stats['category'] = 'PRICE'
    stats['folder']   = 'None'
    stats['model_id'] = model_id

    db_df = stats[[c for c in _TABLE_COLS if c in stats.columns]].replace({float('nan'): None})

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

    log.info("  %d rows upserted into model_security_stat", len(rows))


# ── Run model ────────────────────────────────────────────────────────────────

def run_model() -> None:
    """Fetch inputs, generate distributions, and persist to the VaR store."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    log.info("Loading model info …")
    model_info = get_model_info()
    DATA["model_info"] = model_info
    log.info("  %d securities, proxies: %s", len(model_info), model_info["proxy_id"].unique().tolist())

    log.info("Fetching corefactor distributions …")
    cf_dist = corefactor_dist(model_info)
    DATA["corefactor_dist"] = cf_dist
    log.info("  corefactor_dist shape: %s", cf_dist.shape)

    log.info("Generating security distributions …")
    dist = alternative_model(model_info, cf_dist)
    DATA["dist"] = dist
    log.info("  dist shape: %s", dist.shape)

    log.info("Saving distributions to VaR store …")
    var_utils.save_dist(dist, category="PRICE")

    log.info("Saving distribution statistics to model_security_stat …")
    model_id = var_utils.get_model_id()
    _save_stat_to_db(dist, model_info, model_id)

    log.info("Generating security distributions using unadjusted volatilities …")
    dist = alternative_model_unadj(model_info, cf_dist)
    DATA["dist_unadj"] = dist
    log.info("  dist shape: %s", dist.shape)

    log.info("Saving unadjusted distributions category=ALT to VaR store …")
    var_utils.save_dist(dist, category="ALT")


    log.info("Done.")


# ── CSV output ────────────────────────────────────────────────────────────────

def _save_csv(df: pd.DataFrame, name: str, index: bool = False) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(path, index=index)
    print(f"Saved {len(df)} rows → {path}")


# ── Main ─────────────────────────────────────────────────────────────────────

def test():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    model_info = get_model_info()
    DATA["model_info"] = model_info
    _save_csv(model_info, "alt_model_info")
    print(f"model_info: {len(model_info)} securities, proxies: {model_info['proxy_id'].unique().tolist()}")

    cf_dist = corefactor_dist(model_info)
    DATA["corefactor_dist"] = cf_dist
    _save_csv(cf_dist, "alt_corefactor_dist", index=True)
    print(f"corefactor_dist: {cf_dist.shape}")

    dist = alternative_model(model_info, cf_dist)
    DATA["dist"] = dist
    _save_csv(dist, "alt_dist", index=True)
    print(f"dist: {dist.shape}")

    dist = alternative_model_unadj(model_info, cf_dist)
    DATA["dist"] = dist
    _save_csv(dist, "alt_dist_unadj", index=True)
    print(f"dist_unadj: {dist.shape}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Alternative investment risk model")
    parser.add_argument("--test", action="store_true", help="Run test() and save CSV output instead of persisting to VaR store")
    args = parser.parse_args()

    if args.test:
        test()
    else:
        run_model()
