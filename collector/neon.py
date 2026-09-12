"""Neon schema + idempotent hourly writes."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

SCHEMA = """
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

CREATE INDEX IF NOT EXISTS cohort_members_addr_idx
    ON cohort_members (venue, address, cycle_ts DESC);

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

CREATE INDEX IF NOT EXISTS wallet_positions_coin_idx
    ON wallet_positions (venue, coin, cycle_ts DESC);

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

CREATE INDEX IF NOT EXISTS meta_index_coin_idx
    ON meta_index (venue, coin, cycle_ts DESC);

CREATE TABLE IF NOT EXISTS coin_prices (
    cycle_ts        timestamptz NOT NULL,
    venue           text        NOT NULL DEFAULT 'hyperliquid',
    coin            text        NOT NULL,
    mark_px         numeric,
    mid_px          numeric,
    oracle_px       numeric,
    funding         numeric,
    open_interest   numeric,
    prev_day_px     numeric,
    day_ntl_vlm     numeric,
    premium         numeric,
    ohlc_open       numeric,
    ohlc_high       numeric,
    ohlc_low        numeric,
    ohlc_close      numeric,
    ohlc_volume     numeric,
    ohlc_trades     integer,
    ohlc_start_ts   timestamptz,
    ohlc_closed     boolean     NOT NULL DEFAULT false,
    delisted        boolean     NOT NULL DEFAULT false,
    source          text,
    error           text,
    fetched_at      timestamptz NOT NULL,
    PRIMARY KEY (cycle_ts, venue, coin)
);

CREATE INDEX IF NOT EXISTS coin_prices_coin_idx
    ON coin_prices (venue, coin, cycle_ts DESC);
"""


def _ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"cannot parse timestamp: {value!r}")


class NeonStore:
    def __init__(self, dsn: str, logger: logging.Logger) -> None:
        self.dsn = dsn
        self.log = logger

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, row_factory=dict_row, autocommit=False)

    def apply_schema(self) -> None:
        with self.connect() as conn:
            for stmt in SCHEMA.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)
            conn.commit()
        self.log.info("Neon schema ready")

    def run_status(self, cycle_ts: datetime, venue: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT status FROM collector_runs WHERE cycle_ts = %s AND venue = %s",
                (cycle_ts, venue),
            ).fetchone()
            conn.commit()
        if not row:
            return None
        return str(row["status"])

    def push_cycle(self, payload: dict[str, Any]) -> None:
        """Replace this hour's rows in one transaction. Safe to retry."""
        cycle_ts = _ts(payload["cycle_ts"])
        venue = str(payload["venue"])
        cohort = str(payload["cohort"])
        members = list(payload.get("cohort_members") or [])
        books = list(payload.get("books") or [])
        index_rows = list(payload.get("meta_index") or [])
        started_at = _ts(payload["started_at"])
        finished_at = _ts(payload["finished_at"])

        with self.connect() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO collector_runs (
                        cycle_ts, venue, started_at, finished_at, status,
                        listed, snapped_ok, snapped_err, empty_books, coverage,
                        leaderboard_refreshed, error, duration_s, cohort
                    ) VALUES (
                        %(cycle_ts)s, %(venue)s, %(started_at)s, %(finished_at)s,
                        %(status)s, %(listed)s, %(snapped_ok)s, %(snapped_err)s,
                        %(empty_books)s, %(coverage)s, %(leaderboard_refreshed)s,
                        %(error)s, %(duration_s)s, %(cohort)s
                    )
                    ON CONFLICT (cycle_ts, venue) DO UPDATE SET
                        started_at = EXCLUDED.started_at,
                        finished_at = EXCLUDED.finished_at,
                        status = EXCLUDED.status,
                        listed = EXCLUDED.listed,
                        snapped_ok = EXCLUDED.snapped_ok,
                        snapped_err = EXCLUDED.snapped_err,
                        empty_books = EXCLUDED.empty_books,
                        coverage = EXCLUDED.coverage,
                        leaderboard_refreshed = EXCLUDED.leaderboard_refreshed,
                        error = EXCLUDED.error,
                        duration_s = EXCLUDED.duration_s,
                        cohort = EXCLUDED.cohort
                    """,
                    {
                        "cycle_ts": cycle_ts,
                        "venue": venue,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "status": payload["status"],
                        "listed": int(payload.get("listed") or 0),
                        "snapped_ok": int(payload.get("snapped_ok") or 0),
                        "snapped_err": int(payload.get("snapped_err") or 0),
                        "empty_books": int(payload.get("empty_books") or 0),
                        "coverage": payload.get("coverage"),
                        "leaderboard_refreshed": bool(payload.get("leaderboard_refreshed")),
                        "error": payload.get("error") or None,
                        "duration_s": payload.get("duration_s"),
                        "cohort": cohort,
                    },
                )

                for m in members:
                    addr = str(m["address"]).lower()
                    conn.execute(
                        """
                        INSERT INTO accounts (venue, address, display_name, first_seen_at, last_seen_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (venue, address) DO UPDATE SET
                            display_name = COALESCE(EXCLUDED.display_name, accounts.display_name),
                            last_seen_at = EXCLUDED.last_seen_at
                        """,
                        (
                            venue,
                            addr,
                            m.get("display_name"),
                            cycle_ts,
                            cycle_ts,
                        ),
                    )

                conn.execute(
                    "DELETE FROM cohort_members WHERE cycle_ts = %s AND venue = %s AND cohort = %s",
                    (cycle_ts, venue, cohort),
                )
                for m in members:
                    conn.execute(
                        """
                        INSERT INTO cohort_members (
                            cycle_ts, venue, cohort, rank, address, score,
                            account_value, pnl, roi, volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            cycle_ts,
                            venue,
                            cohort,
                            int(m["rank"]),
                            str(m["address"]).lower(),
                            m.get("score"),
                            m.get("account_value"),
                            m.get("pnl"),
                            m.get("roi"),
                            m.get("volume"),
                        ),
                    )

                conn.execute(
                    "DELETE FROM wallet_positions WHERE cycle_ts = %s AND venue = %s",
                    (cycle_ts, venue),
                )
                conn.execute(
                    "DELETE FROM wallet_books WHERE cycle_ts = %s AND venue = %s",
                    (cycle_ts, venue),
                )
                for book in books:
                    addr = str(book["address"]).lower()
                    fetched = _ts(book.get("fetched_at") or finished_at)
                    positions = list(book.get("positions") or [])
                    conn.execute(
                        """
                        INSERT INTO wallet_books (
                            cycle_ts, venue, address, account_value, fingerprint,
                            n_positions, ok, error, fetched_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            cycle_ts,
                            venue,
                            addr,
                            float(book.get("account_value") or 0),
                            book.get("fingerprint") or "",
                            len(positions),
                            bool(book.get("ok")),
                            book.get("error") or None,
                            fetched,
                        ),
                    )
                    for p in positions:
                        conn.execute(
                            """
                            INSERT INTO wallet_positions (
                                cycle_ts, venue, address, coin, side, size, notional,
                                entry_px, leverage, isolated, conviction
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                cycle_ts,
                                venue,
                                addr,
                                str(p["coin"]),
                                str(p["side"]),
                                float(p.get("size") or 0),
                                float(p.get("notional") or 0),
                                p.get("entry_px"),
                                int(p.get("leverage") or 1),
                                bool(p.get("isolated")),
                                float(p.get("conviction") or 0),
                            ),
                        )

                conn.execute(
                    "DELETE FROM meta_index WHERE cycle_ts = %s AND venue = %s",
                    (cycle_ts, venue),
                )
                for row in index_rows:
                    conn.execute(
                        """
                        INSERT INTO meta_index (
                            cycle_ts, venue, coin, side, wallets, hold_pct, agreement,
                            long_n, short_n, median_leverage, mean_leverage,
                            avg_conviction, notional_usd, rank
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            cycle_ts,
                            venue,
                            str(row["coin"]),
                            str(row["side"]),
                            int(row["wallets"]),
                            float(row["hold_pct"]),
                            float(row["agreement"]),
                            int(row["long_n"]),
                            int(row["short_n"]),
                            int(row.get("median_leverage") or 1),
                            float(row.get("mean_leverage") or 1),
                            float(row.get("avg_conviction") or 0),
                            float(row.get("notional_usd") or 0),
                            int(row["rank"]),
                        ),
                    )

                # Never DELETE coin_prices here. Old WAL payloads have no prices;
                # wiping would throw away a backfill. Upsert only rows we have.
                price_rows = list(payload.get("coin_prices") or [])
                if price_rows:
                    _upsert_price_rows(conn, venue, price_rows, default_cycle=cycle_ts)

        self.log.info(
            "Neon wrote cycle %s  books=%s  coins=%s  prices=%s  status=%s",
            cycle_ts.isoformat(),
            len(books),
            len(index_rows),
            len(payload.get("coin_prices") or []),
            payload.get("status"),
        )

    def upsert_coin_prices(self, venue: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.connect() as conn:
            with conn.transaction():
                _upsert_price_rows(conn, venue, rows)
        self.log.info("Neon upserted %s coin_prices rows", len(rows))

    def price_backfill_plan(
        self,
        venue: str,
        now: datetime,
        *,
        skip_cycle: datetime | None = None,
    ) -> list[tuple[datetime, str]]:
        """Hours that already have a board but are missing a closed 1h bar."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                WITH wanted AS (
                    SELECT DISTINCT cycle_ts, coin
                    FROM meta_index
                    WHERE venue = %s
                    UNION
                    SELECT DISTINCT cycle_ts, coin
                    FROM wallet_positions
                    WHERE venue = %s
                )
                SELECT w.cycle_ts, w.coin
                FROM wanted w
                JOIN collector_runs r
                  ON r.cycle_ts = w.cycle_ts
                 AND r.venue = %s
                 AND r.status IN ('ok', 'partial')
                LEFT JOIN coin_prices p
                  ON p.cycle_ts = w.cycle_ts
                 AND p.venue = %s
                 AND p.coin = w.coin
                WHERE (%s::timestamptz IS NULL OR w.cycle_ts <> %s)
                  AND (
                        p.coin IS NULL
                        OR p.ohlc_close IS NULL
                        OR (
                            p.ohlc_closed = false
                            AND w.cycle_ts + interval '1 hour' <= %s
                        )
                  )
                ORDER BY w.coin, w.cycle_ts
                """,
                (venue, venue, venue, venue, skip_cycle, skip_cycle, now),
            ).fetchall()
            conn.commit()
        out: list[tuple[datetime, str]] = []
        for row in rows:
            out.append((_ts(row["cycle_ts"]), str(row["coin"])))
        return out


_UPSERT_PRICES = """
INSERT INTO coin_prices (
    cycle_ts, venue, coin,
    mark_px, mid_px, oracle_px, funding, open_interest,
    prev_day_px, day_ntl_vlm, premium,
    ohlc_open, ohlc_high, ohlc_low, ohlc_close, ohlc_volume, ohlc_trades,
    ohlc_start_ts, ohlc_closed, delisted, source, error, fetched_at
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (cycle_ts, venue, coin) DO UPDATE SET
    mark_px = COALESCE(coin_prices.mark_px, EXCLUDED.mark_px),
    mid_px = COALESCE(coin_prices.mid_px, EXCLUDED.mid_px),
    oracle_px = COALESCE(coin_prices.oracle_px, EXCLUDED.oracle_px),
    funding = COALESCE(coin_prices.funding, EXCLUDED.funding),
    open_interest = COALESCE(coin_prices.open_interest, EXCLUDED.open_interest),
    prev_day_px = COALESCE(coin_prices.prev_day_px, EXCLUDED.prev_day_px),
    day_ntl_vlm = COALESCE(coin_prices.day_ntl_vlm, EXCLUDED.day_ntl_vlm),
    premium = COALESCE(coin_prices.premium, EXCLUDED.premium),
    ohlc_open = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_open
        ELSE COALESCE(EXCLUDED.ohlc_open, coin_prices.ohlc_open)
    END,
    ohlc_high = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_high
        ELSE COALESCE(EXCLUDED.ohlc_high, coin_prices.ohlc_high)
    END,
    ohlc_low = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_low
        ELSE COALESCE(EXCLUDED.ohlc_low, coin_prices.ohlc_low)
    END,
    ohlc_close = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_close
        ELSE COALESCE(EXCLUDED.ohlc_close, coin_prices.ohlc_close)
    END,
    ohlc_volume = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_volume
        ELSE COALESCE(EXCLUDED.ohlc_volume, coin_prices.ohlc_volume)
    END,
    ohlc_trades = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_trades
        ELSE COALESCE(EXCLUDED.ohlc_trades, coin_prices.ohlc_trades)
    END,
    ohlc_start_ts = CASE
        WHEN coin_prices.ohlc_closed THEN coin_prices.ohlc_start_ts
        ELSE COALESCE(EXCLUDED.ohlc_start_ts, coin_prices.ohlc_start_ts)
    END,
    ohlc_closed = coin_prices.ohlc_closed OR EXCLUDED.ohlc_closed,
    delisted = coin_prices.delisted OR EXCLUDED.delisted,
    source = CASE
        WHEN coin_prices.source LIKE '%ctx%' THEN coin_prices.source
        ELSE COALESCE(EXCLUDED.source, coin_prices.source)
    END,
    error = CASE
        WHEN EXCLUDED.ohlc_close IS NOT NULL THEN NULL
        ELSE COALESCE(EXCLUDED.error, coin_prices.error)
    END,
    fetched_at = EXCLUDED.fetched_at
"""


def _opt_ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    return _ts(value)


def _upsert_price_rows(
    conn: psycopg.Connection,
    venue: str,
    rows: list[dict[str, Any]],
    *,
    default_cycle: datetime | None = None,
) -> None:
    for row in rows:
        coin = str(row.get("coin") or "").strip()
        if not coin:
            continue
        cycle = row.get("cycle_ts")
        cycle_ts = _ts(cycle) if cycle is not None else default_cycle
        if cycle_ts is None:
            continue
        conn.execute(
            _UPSERT_PRICES,
            (
                cycle_ts,
                venue,
                coin,
                row.get("mark_px"),
                row.get("mid_px"),
                row.get("oracle_px"),
                row.get("funding"),
                row.get("open_interest"),
                row.get("prev_day_px"),
                row.get("day_ntl_vlm"),
                row.get("premium"),
                row.get("ohlc_open"),
                row.get("ohlc_high"),
                row.get("ohlc_low"),
                row.get("ohlc_close"),
                row.get("ohlc_volume"),
                row.get("ohlc_trades"),
                _opt_ts(row.get("ohlc_start_ts")),
                bool(row.get("ohlc_closed")),
                bool(row.get("delisted")),
                row.get("source") or None,
                (row.get("error") or None),
                _ts(row.get("fetched_at") or datetime.now(timezone.utc)),
            ),
        )
