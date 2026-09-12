"""Mark / funding / OI + 1h OHLC for every coin on the hour's board.

Live hours store the snapshot mark from metaAndAssetCtxs. Past hours cannot
get that mark back; backfill uses the 1h candle close and tags source=candle.
HIP-3 names try xyz:TICKER then the bare ticker so a rename does not skip OHLC.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import Settings
from .hl import (
    HyperliquidPublic,
    fetch_all_mids_map,
    fetch_asset_ctx_map,
    fetch_candles,
    fetch_funding_history,
    fopt,
)

log = logging.getLogger("collector")

HOUR_MS = 3_600_000
_candle_alias: dict[str, str] = {}


def ctx_dexes(cfg: Settings) -> list[str]:
    fallback = [d.strip() for d in cfg.dex_fallback.split(",") if d.strip()]
    scope = (cfg.dex_scope or "include").strip().lower()
    if scope == "native":
        return [""]
    if scope == "xyz_only":
        return fallback or ["xyz"]
    return [""] + fallback


def coins_from_books_and_index(
    books: list[dict[str, Any]],
    index_rows: list[dict[str, Any]],
) -> list[str]:
    coins: set[str] = set()
    for row in index_rows:
        coin = str(row.get("coin") or "").strip()
        if coin:
            coins.add(coin)
    for book in books:
        for pos in book.get("positions") or []:
            coin = str(pos.get("coin") or "").strip()
            if coin:
                coins.add(coin)
    return sorted(coins)


def candle_name_candidates(coin: str) -> list[str]:
    raw = str(coin or "").strip()
    out: list[str] = []
    for name in (raw,):
        if name and name not in out:
            out.append(name)
    if ":" in raw:
        dex, sym = raw.split(":", 1)
        dex, sym = dex.strip(), sym.strip()
        for name in (sym, f"{dex}:{sym}" if dex else ""):
            if name and name not in out:
                out.append(name)
    else:
        xyz = f"xyz:{raw}"
        if xyz not in out:
            out.append(xyz)
    return out


def lookup_ctx(ctx_map: dict[str, dict[str, Any]], coin: str) -> dict[str, Any] | None:
    if coin in ctx_map:
        return ctx_map[coin]
    if ":" in coin:
        _dex, sym = coin.split(":", 1)
        if sym in ctx_map:
            return ctx_map[sym]
        return None
    return ctx_map.get(f"xyz:{coin}")


def lookup_mid(mids: dict[str, float], coin: str) -> float | None:
    if coin in mids:
        return mids[coin]
    if ":" in coin:
        _dex, sym = coin.split(":", 1)
        return mids.get(sym)
    return mids.get(f"xyz:{coin}")


def parse_candle(raw: dict[str, Any]) -> dict[str, Any] | None:
    t = fopt(raw.get("t"))
    if t is None:
        return None
    open_ms = int(t)
    close_ms = int(fopt(raw.get("T")) or (open_ms + HOUR_MS - 1))
    o = fopt(raw.get("o"))
    h = fopt(raw.get("h"))
    low = fopt(raw.get("l"))
    c = fopt(raw.get("c"))
    if o is None or h is None or low is None or c is None:
        return None
    trades = raw.get("n")
    try:
        n_trades = int(trades) if trades is not None and trades != "" else None
    except (TypeError, ValueError):
        n_trades = None
    return {
        "open_ms": open_ms,
        "close_ms": close_ms,
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "volume": fopt(raw.get("v")),
        "trades": n_trades,
    }


def bar_for_hour(candles: list[dict[str, Any]], start_ms: int) -> dict[str, Any] | None:
    matched: list[dict[str, Any]] = []
    for raw in candles:
        bar = parse_candle(raw)
        if bar is None:
            continue
        if bar["open_ms"] == start_ms or start_ms <= bar["open_ms"] < start_ms + HOUR_MS:
            matched.append(bar)
    if not matched:
        return None
    exact = [b for b in matched if b["open_ms"] == start_ms]
    return exact[0] if exact else matched[0]


def candle_closed(bar: dict[str, Any], now: datetime) -> bool:
    now_ms = int(now.timestamp() * 1000)
    return int(bar["close_ms"]) < now_ms or int(bar["open_ms"]) + HOUR_MS <= now_ms


def resolve_candle_coin(
    client: HyperliquidPublic,
    coin: str,
    *,
    start_ms: int,
    end_ms: int,
) -> tuple[str | None, list[dict[str, Any]], str]:
    cached = _candle_alias.get(coin)
    names = [cached] if cached else []
    for name in candle_name_candidates(coin):
        if name not in names:
            names.append(name)
    last_err = ""
    empty_ok: tuple[str, list[dict[str, Any]]] | None = None
    for name in names:
        if not name:
            continue
        try:
            raw = fetch_candles(client, name, start_ms=start_ms, end_ms=end_ms)
        except Exception as exc:
            last_err = str(exc)[:240]
            continue
        if raw:
            _candle_alias[coin] = name
            return name, raw, ""
        empty_ok = (name, raw)
    if empty_ok:
        _candle_alias[coin] = empty_ok[0]
        return empty_ok[0], empty_ok[1], ""
    return None, [], last_err or "no 1h candle"


def apply_bar(row: dict[str, Any], bar: dict[str, Any], now: datetime) -> None:
    row["ohlc_open"] = bar["open"]
    row["ohlc_high"] = bar["high"]
    row["ohlc_low"] = bar["low"]
    row["ohlc_close"] = bar["close"]
    row["ohlc_volume"] = bar["volume"]
    row["ohlc_trades"] = bar["trades"]
    row["ohlc_start_ts"] = datetime.fromtimestamp(bar["open_ms"] / 1000.0, tz=timezone.utc).isoformat()
    row["ohlc_closed"] = candle_closed(bar, now)
    if row.get("mark_px") is None:
        row["mark_px"] = bar["close"]


def apply_ctx(row: dict[str, Any], ctx: dict[str, Any] | None) -> None:
    if not ctx:
        return
    for key in (
        "mark_px",
        "mid_px",
        "oracle_px",
        "funding",
        "open_interest",
        "prev_day_px",
        "day_ntl_vlm",
        "premium",
    ):
        if row.get(key) is None and ctx.get(key) is not None:
            row[key] = ctx[key]
    if ctx.get("delisted"):
        row["delisted"] = True


def empty_price_row(cycle_ts: datetime, coin: str, fetched_at: datetime) -> dict[str, Any]:
    return {
        "cycle_ts": cycle_ts.isoformat(),
        "coin": coin,
        "mark_px": None,
        "mid_px": None,
        "oracle_px": None,
        "funding": None,
        "open_interest": None,
        "prev_day_px": None,
        "day_ntl_vlm": None,
        "premium": None,
        "ohlc_open": None,
        "ohlc_high": None,
        "ohlc_low": None,
        "ohlc_close": None,
        "ohlc_volume": None,
        "ohlc_trades": None,
        "ohlc_start_ts": None,
        "ohlc_closed": False,
        "delisted": False,
        "source": None,
        "error": None,
        "fetched_at": fetched_at.isoformat(),
    }


def source_tag(row: dict[str, Any], *, used_ctx: bool, used_candle: bool, used_funding: bool) -> str:
    parts: list[str] = []
    if used_ctx and row.get("mark_px") is not None:
        parts.append("ctx")
    if used_candle and row.get("ohlc_close") is not None:
        parts.append("candle")
    if used_funding and row.get("funding") is not None:
        parts.append("funding")
    return "+".join(parts) if parts else "missing"


def funding_at(history: list[dict[str, Any]], cycle_ts: datetime) -> float | None:
    start_ms = int(cycle_ts.timestamp() * 1000)
    end_ms = start_ms + HOUR_MS
    best: tuple[int, float] | None = None
    for item in history:
        t = fopt(item.get("time"))
        rate = fopt(item.get("fundingRate") if "fundingRate" in item else item.get("funding"))
        if t is None or rate is None:
            continue
        ts = int(t)
        if ts < start_ms - HOUR_MS or ts >= end_ms + HOUR_MS:
            continue
        dist = abs(ts - start_ms)
        if best is None or dist < best[0]:
            best = (dist, rate)
    return best[1] if best else None


def collect_cycle_prices(
    client: HyperliquidPublic,
    *,
    cycle_ts: datetime,
    coins: list[str],
    dexes: list[str],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    fetched_at = now
    if not coins:
        return []
    start_ms = int(cycle_ts.timestamp() * 1000)
    end_ms = start_ms + HOUR_MS

    ctx_map: dict[str, dict[str, Any]] = {}
    try:
        ctx_map = fetch_asset_ctx_map(client, dexes)
    except Exception:
        log.exception("asset ctx fetch failed — will still try 1h candles")

    mids: dict[str, float] = {}
    missing_mark = [c for c in coins if lookup_ctx(ctx_map, c) is None]
    if missing_mark:
        try:
            mids = fetch_all_mids_map(client, dexes)
        except Exception:
            log.exception("allMids fallback failed")

    rows: list[dict[str, Any]] = []
    ok = 0
    for coin in coins:
        row = empty_price_row(cycle_ts, coin, fetched_at)
        ctx = lookup_ctx(ctx_map, coin)
        apply_ctx(row, ctx)
        used_ctx = ctx is not None and row.get("mark_px") is not None
        if row.get("mark_px") is None:
            mid = lookup_mid(mids, coin)
            if mid is not None:
                row["mark_px"] = mid
                row["mid_px"] = mid if row.get("mid_px") is None else row["mid_px"]
                used_ctx = True
        _name, candles, err = resolve_candle_coin(
            client, coin, start_ms=start_ms, end_ms=end_ms
        )
        bar = bar_for_hour(candles, start_ms)
        used_candle = bar is not None
        if bar:
            apply_bar(row, bar, now)
        elif err:
            row["error"] = err
        elif not candles:
            row["error"] = "no 1h candle"
        row["source"] = source_tag(row, used_ctx=used_ctx, used_candle=used_candle, used_funding=False)
        if row.get("ohlc_close") is not None or row.get("mark_px") is not None:
            ok += 1
        rows.append(row)

    log.info(
        "Prices for %s: %s/%s coins with mark or 1h close",
        cycle_ts.isoformat(),
        ok,
        len(coins),
    )
    return rows


def collect_price_range(
    client: HyperliquidPublic,
    *,
    coin: str,
    cycles: list[datetime],
    now: datetime,
    ctx_map: dict[str, dict[str, Any]],
    live_cycle: datetime | None,
) -> list[dict[str, Any]]:
    if not cycles:
        return []
    start_ms = int(min(cycles).timestamp() * 1000)
    end_ms = int(max(cycles).timestamp() * 1000) + HOUR_MS
    api_name, candles, err = resolve_candle_coin(
        client, coin, start_ms=start_ms, end_ms=end_ms
    )
    history: list[dict[str, Any]] = []
    fund_name = api_name or candle_name_candidates(coin)[0]
    try:
        history = fetch_funding_history(
            client, fund_name, start_ms=start_ms, end_ms=end_ms
        )
    except Exception as exc:
        log.warning("fundingHistory %s: %s", coin, exc)

    ctx = lookup_ctx(ctx_map, coin)
    rows: list[dict[str, Any]] = []
    for cycle_ts in cycles:
        row = empty_price_row(cycle_ts, coin, now)
        hour_ms = int(cycle_ts.timestamp() * 1000)
        is_live = live_cycle is not None and cycle_ts == live_cycle
        used_ctx = False
        if is_live:
            apply_ctx(row, ctx)
            used_ctx = ctx is not None and row.get("mark_px") is not None
        bar = bar_for_hour(candles, hour_ms)
        used_candle = bar is not None
        if bar:
            apply_bar(row, bar, now)
        used_funding = False
        if row.get("funding") is None:
            rate = funding_at(history, cycle_ts)
            if rate is not None:
                row["funding"] = rate
                used_funding = True
        if not used_candle:
            row["error"] = err or "no 1h candle"
        row["source"] = source_tag(
            row, used_ctx=used_ctx, used_candle=used_candle, used_funding=used_funding
        )
        rows.append(row)
    return rows


def hour_still_open(cycle_ts: datetime, now: datetime) -> bool:
    return cycle_ts + timedelta(hours=1) > now
