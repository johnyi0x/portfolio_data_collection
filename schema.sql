-- Applied automatically on collector start. Kept here for the Neon SQL editor.

CREATE TABLE IF NOT EXISTS collector_runs (
    cycle_ts        timestamptz NOT NULL,
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    status          text        NOT NULL,
    listed          integer     NOT NULL DEFAULT 0,
    snapped_ok      integer     NOT NULL DEFAULT 0,
    snapped_err     integer     NOT NULL DEFAULT 0,
    empty_books     integer     NOT NULL DEFAULT 0,
    coverage        numeric,
    leaderboard_refreshed boolean NOT NULL DEFAULT false,
    error           text,
    duration_s      numeric,
    cohort          text,
    PRIMARY KEY (cycle_ts, venue)
);

CREATE TABLE IF NOT EXISTS accounts (
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    address         text        NOT NULL,
    display_name    text,
    first_seen_at   timestamptz NOT NULL,
    last_seen_at    timestamptz NOT NULL,
    PRIMARY KEY (venue, address)
);

CREATE TABLE IF NOT EXISTS cohort_members (
    cycle_ts        timestamptz NOT NULL,
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    cohort          text        NOT NULL,
    rank            integer     NOT NULL,
    address         text        NOT NULL,
    score           numeric,
    account_value   numeric,
    pnl             numeric,
    roi             numeric,
    volume          numeric,
    PRIMARY KEY (cycle_ts, venue, cohort, address)
);

CREATE TABLE IF NOT EXISTS wallet_books (
    cycle_ts        timestamptz NOT NULL,
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    address         text        NOT NULL,
    account_value   numeric     NOT NULL,
    fingerprint     text,
    n_positions     integer     NOT NULL DEFAULT 0,
    ok              boolean     NOT NULL,
    error           text,
    fetched_at      timestamptz NOT NULL,
    PRIMARY KEY (cycle_ts, venue, address)
);

CREATE TABLE IF NOT EXISTS wallet_positions (
    cycle_ts        timestamptz NOT NULL,
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    address         text        NOT NULL,
    coin            text        NOT NULL,
    side            text        NOT NULL,
    size            numeric     NOT NULL,
    notional        numeric     NOT NULL,
    entry_px        numeric,
    leverage        integer     NOT NULL,
    isolated        boolean     NOT NULL DEFAULT false,
    conviction      numeric     NOT NULL,
    PRIMARY KEY (cycle_ts, venue, address, coin)
);

CREATE TABLE IF NOT EXISTS meta_index (
    cycle_ts        timestamptz NOT NULL,
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    coin            text        NOT NULL,
    side            text        NOT NULL,
    wallets         integer     NOT NULL,
    hold_pct        numeric     NOT NULL,
    agreement       numeric     NOT NULL,
    long_n          integer     NOT NULL,
    short_n         integer     NOT NULL,
    median_leverage integer,
    mean_leverage   numeric,
    avg_conviction  numeric,
    notional_usd    numeric,
    rank            integer     NOT NULL,
    PRIMARY KEY (cycle_ts, venue, coin)
);
