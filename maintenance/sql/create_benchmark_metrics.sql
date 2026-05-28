-- benchmark_metrics: daily risk metrics per benchmark, calculated by calc_benchmark.py
CREATE TABLE IF NOT EXISTS benchmark_metrics (
    benchmark_id  INTEGER   NOT NULL,
    date          DATE      NOT NULL,
    volatility    NUMERIC,
    var_1d_95     NUMERIC,
    es_1d_95      NUMERIC,
    var_1d_99     NUMERIC,
    es_1d_99      NUMERIC,
    sharpe_vol    NUMERIC,
    sharpe_var    NUMERIC,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (benchmark_id, date)
);
