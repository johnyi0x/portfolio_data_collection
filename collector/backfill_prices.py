"""Fill coin_prices for hours already in Neon.

Past hours cannot recover the live mark. This writes the 1h candle (and
fundingHistory when HL still has it) and tags source so later PnL knows
mark vs candle-close. Safe to re-run; upserts never overwrite a live mark.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from .config import Settings, load_settings
from .hl import HyperliquidPublic, fetch_all_mids_map, fetch_asset_ctx_map
from .neon import NeonStore
from .prices import (
    collect_price_range,
    ctx_dexes_for_coins,
    hour_still_open,
    lookup_mid,
)

log = logging.getLogger("collector")


def backfill_prices(
    cfg: Settings,
    client: HyperliquidPublic,
    store: NeonStore,
    *,
    skip_cycle: datetime | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    plan = store.price_backfill_plan(cfg.venue, now, skip_cycle=skip_cycle)
    if not plan:
        log.info("Price backfill: nothing missing")
        return 0

    by_coin: dict[str, list[datetime]] = defaultdict(list)
    for cycle_ts, coin in plan:
        by_coin[coin].append(cycle_ts)

    live_cycle = None
    for cycle_ts, _coin in plan:
        if hour_still_open(cycle_ts, now):
            live_cycle = cycle_ts
            break

    dexes = ctx_dexes_for_coins(list(by_coin))
    ctx_map: dict[str, dict[str, Any]] = {}
    if live_cycle is not None:
        try:
            ctx_map = fetch_asset_ctx_map(client, dexes)
        except Exception:
            log.exception("Backfill live ctx failed")
        if ctx_map:
            try:
                mids = fetch_all_mids_map(client, dexes)
            except Exception:
                mids = {}
            for coin, ctx in list(ctx_map.items()):
                if ctx.get("mark_px") is None:
                    mid = lookup_mid(mids, coin)
                    if mid is not None:
                        ctx["mark_px"] = mid
                        ctx["mid_px"] = ctx.get("mid_px") or mid

    log.info(
        "Price backfill: %s coins, %s hour-rows%s",
        len(by_coin),
        len(plan),
        f" (skip {skip_cycle.isoformat()})" if skip_cycle else "",
    )
    written = 0
    failed = 0
    batch: list[dict[str, Any]] = []
    for i, (coin, cycles) in enumerate(sorted(by_coin.items()), start=1):
        try:
            rows = collect_price_range(
                client,
                coin=coin,
                cycles=sorted(set(cycles)),
                now=now,
                ctx_map=ctx_map,
                live_cycle=live_cycle,
            )
            batch.extend(rows)
        except Exception:
            failed += 1
            log.exception("Price backfill failed for %s", coin)
        if len(batch) >= 200 or i == len(by_coin):
            if batch:
                store.upsert_coin_prices(cfg.venue, batch)
                written += len(batch)
                batch = []
        if i % 25 == 0 or i == len(by_coin):
            log.info("Price backfill %s/%s coins", i, len(by_coin))

    log.info("Price backfill done rows=%s coin-failures=%s", written, failed)
    return written


def run_backfill() -> None:
    from .run import setup_logging

    setup_logging()
    cfg = load_settings()
    store = NeonStore(cfg.database_url, log)
    store.apply_schema()
    client = HyperliquidPublic(
        info_url=cfg.hl_info_url,
        timeout_s=cfg.request_timeout_s,
        leaderboard_timeout_s=cfg.leaderboard_timeout_s,
        gap_s=cfg.request_gap_s,
        retries=cfg.snapshot_retries,
        logger=log,
        ip_reserve=cfg.ip_weight_reserve,
    )
    n = backfill_prices(cfg, client, store)
    log.info("Standalone price backfill wrote %s rows", n)


if __name__ == "__main__":
    run_backfill()
