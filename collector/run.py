"""Hourly collect loop: top 7d ROI wallets → books → majority index → Neon."""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import Settings, load_settings
from .hl import HyperliquidPublic, load_leaderboard, shortlist_top_roi, snapshot_wallet
from .index import tally_holds
from .neon import NeonStore
from .wal import CycleWal

log = logging.getLogger("collector")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
    )
    logging.Formatter.converter = time.gmtime


def cycle_bucket(now: datetime, interval_hours: float) -> datetime:
    now = now.astimezone(timezone.utc)
    step = max(1, int(round(interval_hours)))
    hour = (now.hour // step) * step
    return now.replace(hour=hour, minute=0, second=0, microsecond=0)


def cycle_key(ts: datetime, venue: str) -> str:
    return f"{venue}_{ts.strftime('%Y%m%dT%H%M')}Z"


def seconds_until_next(now: datetime, interval_hours: float) -> float:
    current = cycle_bucket(now, interval_hours)
    nxt = current + timedelta(hours=max(1, int(round(interval_hours))))
    return max(5.0, (nxt - now).total_seconds())


def gather_cycle(
    cfg: Settings,
    client: HyperliquidPublic,
    now: datetime,
) -> dict[str, Any]:
    started = time.time()
    cycle_ts = cycle_bucket(now, cfg.snapshot_interval_hours)
    cache_path = cfg.data_dir / "leaderboard_cache.json"
    log.info("Refreshing 7d ROI wallet list before snapshots")
    rows, refreshed = load_leaderboard(
        client, cache_path, cfg.leaderboard_refresh_hours
    )
    members = shortlist_top_roi(
        rows, rank_window=cfg.rank_window, limit=cfg.basket_size
    )
    if not members:
        raise RuntimeError("Leaderboard shortlist was empty")
    log.info(
        "Wallet list ready n=%s refreshed=%s — snapshotting books",
        len(members),
        refreshed,
    )

    books: list[dict[str, Any]] = []
    for i, m in enumerate(members, start=1):
        snap_now = time.time()
        book = snapshot_wallet(client, m["address"], snap_now)
        books.append(book)
        if not book["ok"]:
            log.warning("Snapshot fail %s %s — %s", i, m["address"][:10], book["error"])
        elif i % 25 == 0 or i == len(members):
            log.info("Snapped %s/%s", i, len(members))

    index_rows, stats = tally_holds(
        books,
        dex_scope=cfg.dex_scope,
        min_notional_usd=cfg.min_notional_usd,
    )
    listed = len(members)
    ok = int(stats["ok"])
    coverage = (ok / listed) if listed else 0.0
    if coverage + 1e-12 >= cfg.min_coverage:
        status = "ok"
        err = ""
    elif ok > 0:
        status = "partial"
        err = f"coverage {coverage:.2%} below {cfg.min_coverage:.0%}"
        log.warning(err)
    else:
        status = "failed"
        err = "every wallet snapshot failed"
        log.error(err)

    finished = time.time()
    return {
        "cycle_ts": cycle_ts.isoformat(),
        "venue": cfg.venue,
        "cohort": cfg.cohort,
        "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "finished_at": datetime.fromtimestamp(finished, tz=timezone.utc).isoformat(),
        "status": status,
        "listed": listed,
        "snapped_ok": ok,
        "snapped_err": int(stats["errors"]),
        "empty_books": int(stats["empty"]),
        "coverage": round(coverage, 6),
        "leaderboard_refreshed": refreshed,
        "error": err,
        "duration_s": round(finished - started, 2),
        "cohort_members": members,
        "books": books,
        "meta_index": index_rows,
    }


def push_with_retry(store: NeonStore, payload: dict[str, Any], attempts: int = 5) -> None:
    last: Exception | None = None
    for i in range(attempts):
        try:
            store.push_cycle(payload)
            return
        except Exception as exc:
            last = exc
            delay = min(60.0, 4.0 * (2 ** i))
            log.warning("Neon push failed (%s/%s): %s — retry in %.0fs", i + 1, attempts, exc, delay)
            time.sleep(delay)
    raise RuntimeError(f"Neon push failed after retries: {last}")


def replay_wal(wal: CycleWal, store: NeonStore) -> None:
    for key in wal.unsynced_keys():
        payload = wal.load(key)
        if not payload:
            continue
        log.info("Replaying unsynced WAL %s", key)
        try:
            push_with_retry(store, payload)
            wal.mark_synced(key)
        except Exception:
            log.exception("WAL replay still failing for %s — will retry next loop", key)


def run_forever(cfg: Settings | None = None) -> None:
    setup_logging()
    cfg = cfg or load_settings()
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    wal = CycleWal(cfg.data_dir)
    store = NeonStore(cfg.database_url, log)
    store.apply_schema()
    client = HyperliquidPublic(
        info_url=cfg.hl_info_url,
        timeout_s=cfg.request_timeout_s,
        leaderboard_timeout_s=cfg.leaderboard_timeout_s,
        gap_s=cfg.request_gap_s,
        retries=cfg.snapshot_retries,
        logger=log,
    )
    log.info(
        "Collector start venue=%s basket=%s window=%s every=%.1fh leaderboard=%.1fh",
        cfg.venue,
        cfg.basket_size,
        cfg.rank_window,
        cfg.snapshot_interval_hours,
        cfg.leaderboard_refresh_hours,
    )
    replay_wal(wal, store)

    while True:
        try:
            replay_wal(wal, store)
            now = datetime.now(timezone.utc)
            bucket = cycle_bucket(now, cfg.snapshot_interval_hours)
            key = cycle_key(bucket, cfg.venue)
            neon_status = store.run_status(bucket, cfg.venue)
            if neon_status == "ok":
                sleep_s = seconds_until_next(now, cfg.snapshot_interval_hours)
                log.info("Hour %s already stored — sleep %.0fs", bucket.isoformat(), sleep_s)
                time.sleep(sleep_s)
                continue

            existing = wal.load(key)
            if existing and not wal.is_synced(key):
                log.info("Pushing local WAL for %s (no re-snapshot)", key)
                push_with_retry(store, existing)
                wal.mark_synced(key)
                continue

            log.info("Gathering cycle %s", bucket.isoformat())
            payload = gather_cycle(cfg, client, now)
            wal.write(key, payload)
            push_with_retry(store, payload)
            wal.mark_synced(key)
            wal.prune_synced(keep_days=7)
            sleep_s = seconds_until_next(datetime.now(timezone.utc), cfg.snapshot_interval_hours)
            log.info(
                "Cycle done status=%s coverage=%.0f%% coins=%s — sleep %.0fs",
                payload["status"],
                float(payload["coverage"]) * 100.0,
                len(payload["meta_index"]),
                sleep_s,
            )
            time.sleep(sleep_s)
        except KeyboardInterrupt:
            log.info("Stopped")
            return
        except Exception:
            log.exception("Cycle error — retry in 60s")
            time.sleep(60.0)
