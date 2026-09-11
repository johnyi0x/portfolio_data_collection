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

        self.log.info(
            "Neon wrote cycle %s  books=%s  coins=%s  status=%s",
            cycle_ts.isoformat(),
            len(books),
            len(index_rows),
            payload.get("status"),
        )
