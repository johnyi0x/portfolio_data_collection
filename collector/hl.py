"""Hyperliquid public info: leaderboard + wallet books (no trading keys)."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import requests

from .wal import atomic_write_json, read_json

LEADERBOARD_URLS = (
    "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard",
    "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard.json",
)

WINDOW_ALIAS = {
    "month": "perpMonth",
    "week": "perpWeek",
    "day": "perpDay",
}

_CLEARINGHOUSE_META_KEYS = {
    "assetPositions",
    "marginSummary",
    "crossMarginSummary",
    "withdrawable",
    "time",
    "agentAddress",
    "cumLedger",
    "perpDexStates",
}


def fnum(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def coin_key(coin: str, dex: str | None = None) -> str:
    raw = str(coin or "").strip()
    if ":" in raw:
        left, right = raw.split(":", 1)
        dex_s = left.strip()
        sym = right.strip()
        return f"{dex_s}:{sym}" if dex_s else sym
    dex_s = (dex or "").strip()
    return f"{dex_s}:{raw}" if dex_s else raw


def dex_of(api_coin: str) -> str:
    raw = str(api_coin or "").strip()
    if ":" in raw:
        return raw.split(":", 1)[0].strip()
    return ""


def in_scope(coin: str, dex_scope: str) -> bool:
    scope = (dex_scope or "include").strip().lower()
    dex = dex_of(coin)
    if scope == "native" and dex:
        return False
    if scope == "xyz_only" and not dex:
        return False
    return True


def rank_window_key(name: str) -> str:
    n = str(name or "week").strip()
    return WINDOW_ALIAS.get(n, n)


class HyperliquidPublic:
    def __init__(
        self,
        *,
        info_url: str,
        timeout_s: float,
        leaderboard_timeout_s: float,
        gap_s: float,
        retries: int,
        logger: logging.Logger,
    ) -> None:
        self.info_url = info_url
        self.timeout_s = timeout_s
        self.leaderboard_timeout_s = leaderboard_timeout_s
        self.gap_s = gap_s
        self.retries = retries
        self.log = logger
        self._last_call = 0.0
        self.session = requests.Session()

    def _pace(self) -> None:
        wait = self.gap_s - (time.time() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.time()

    def _sleep_backoff(self, attempt: int, retry_after: float | None = None) -> None:
        if retry_after and retry_after > 0:
            time.sleep(min(30.0, retry_after))
            return
        time.sleep(min(20.0, 2.0 * (2 ** max(0, attempt))))

    def post_info(self, body: dict[str, Any]) -> Any:
        last_err: Exception | None = None
        for attempt in range(self.retries):
            self._pace()
            try:
                resp = self.session.post(
                    self.info_url,
                    json=body,
                    timeout=self.timeout_s,
                )
                if resp.status_code == 429:
                    ra = None
                    try:
                        ra = float(resp.headers.get("Retry-After") or 0)
                    except (TypeError, ValueError):
                        ra = None
                    self.log.warning("HL 429 on %s — backoff", body.get("type"))
                    self._sleep_backoff(attempt, ra)
                    last_err = RuntimeError("429")
                    continue
                if resp.status_code >= 500:
                    self.log.warning("HL %s on %s", resp.status_code, body.get("type"))
                    self._sleep_backoff(attempt)
                    last_err = RuntimeError(f"HTTP {resp.status_code}")
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                self.log.warning("HL info error (%s/%s): %s", attempt + 1, self.retries, exc)
                self._sleep_backoff(attempt)
        raise RuntimeError(f"HL info failed after retries: {last_err}")

    def fetch_leaderboard_payload(self) -> Any:
        last_err: Exception | None = None
        for url in LEADERBOARD_URLS:
            try:
                self.log.info("Downloading leaderboard %s", url)
                resp = self.session.get(url, timeout=self.leaderboard_timeout_s)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                self.log.warning("Leaderboard fetch failed (%s): %s", url, exc)
        raise RuntimeError(f"Could not download HL leaderboard: {last_err}")


def parse_leaderboard(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = (
            payload.get("leaderboardRows")
            or payload.get("leaderboard_rows")
            or payload.get("leaderboard")
            or []
        )
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        addr = str(row.get("ethAddress") or row.get("eth_address") or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        windows: dict[str, dict[str, float]] = {}
        raw = row.get("windowPerformances") or row.get("window_performances") or []
        items = raw.items() if isinstance(raw, dict) else raw
        for item in items:
            name = ""
            block: Any = None
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                name, block = str(item[0]), item[1]
            elif isinstance(item, dict):
                name = str(item.get("window") or item.get("name") or "")
                block = item
            if not name or not isinstance(block, dict):
                continue
            windows[name] = {
                "pnl": fnum(block.get("pnl")),
                "roi": fnum(block.get("roi")),
                "volume": fnum(block.get("vlm", block.get("volume"))),
            }
        name = row.get("displayName")
        out.append(
            {
                "address": addr,
                "account_value": fnum(row.get("accountValue") or row.get("account_value")),
                "display_name": str(name) if name else None,
                "windows": windows,
            }
        )
    return out


def window_perf(row: dict[str, Any], rank_window: str) -> dict[str, float] | None:
    windows = row.get("windows") or {}
    key = rank_window_key(rank_window)
    block = windows.get(key) or windows.get(rank_window)
    if isinstance(block, dict):
        return block
    return None


def shortlist_top_roi(
    rows: list[dict[str, Any]],
    *,
    rank_window: str,
    limit: int,
) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        w = window_perf(row, rank_window)
        if w is None:
            continue
        scored.append(
            {
                "address": row["address"],
                "display_name": row.get("display_name"),
                "account_value": float(row.get("account_value") or 0),
                "pnl": float(w.get("pnl") or 0),
                "roi": float(w.get("roi") or 0),
                "volume": float(w.get("volume") or 0),
                "score": float(w.get("roi") or 0),
            }
        )
    scored.sort(key=lambda x: x["score"], reverse=True)
    out = []
    for i, row in enumerate(scored[:limit], start=1):
        row = dict(row)
        row["rank"] = i
        out.append(row)
    return out


def load_leaderboard(
    client: HyperliquidPublic,
    cache_path: Path,
    cache_hours: float = 0.0,
) -> tuple[list[dict[str, Any]], bool]:
    """Always download first so this hour's books use this hour's top-N list.

    Disk cache is only used if the live download fails.
    """
    cached = read_json(cache_path, None)
    now = time.time()
    try:
        payload = client.fetch_leaderboard_payload()
        rows = parse_leaderboard(payload)
        if not rows:
            raise RuntimeError("Leaderboard payload parsed to 0 wallets")
        atomic_write_json(
            cache_path,
            {"fetched_at": now, "count": len(rows), "payload": payload},
        )
        client.log.info("Leaderboard stored (%s wallets)", len(rows))
        return rows, True
    except Exception as exc:
        if isinstance(cached, dict) and cached.get("payload") is not None:
            age_h = (now - float(cached.get("fetched_at") or 0)) / 3600.0
            client.log.warning(
                "Leaderboard download failed (%.1fh-old cache, limit=%.1fh): %s",
                age_h,
                cache_hours,
                exc,
            )
            return parse_leaderboard(cached["payload"]), False
        raise


def iter_clearinghouse_states(raw: Any, *, default_dex: str = "") -> list[tuple[str, dict[str, Any]]]:
    tagged: list[tuple[str, dict[str, Any]]] = []
    seen: set[int] = set()

    def add(dex: str, state: dict[str, Any]) -> None:
        if "assetPositions" not in state:
            return
        sid = id(state)
        if sid in seen:
            return
        seen.add(sid)
        tagged.append((str(dex or ""), state))

    def is_pair(obj: Any) -> bool:
        return (
            isinstance(obj, (list, tuple))
            and len(obj) == 2
            and isinstance(obj[0], str)
            and isinstance(obj[1], dict)
            and "assetPositions" in obj[1]
        )

    def walk(obj: Any, dex_hint: str) -> None:
        if obj is None:
            return
        if is_pair(obj):
            add(obj[0], obj[1])
            return
        if isinstance(obj, dict):
            if "assetPositions" in obj:
                add(dex_hint, obj)
            for key, val in obj.items():
                if key == "assetPositions":
                    continue
                next_dex = dex_hint
                if (
                    isinstance(key, str)
                    and key not in _CLEARINGHOUSE_META_KEYS
                    and isinstance(val, dict)
                    and "assetPositions" in val
                ):
                    next_dex = key
                walk(val, next_dex)
            return
        if isinstance(obj, (list, tuple)):
            for item in obj:
                walk(item, dex_hint)

    walk(raw, default_dex or "")
    return tagged


def account_value_from_states(states: list[tuple[str, dict[str, Any]]]) -> float:
    best = 0.0
    for _dex, state in states:
        for key in ("marginSummary", "crossMarginSummary"):
            block = state.get(key) or {}
            if isinstance(block, dict):
                best = max(best, fnum(block.get("accountValue")))
    return best


def parse_positions(
    states: list[tuple[str, dict[str, Any]]],
    account_value: float,
) -> list[dict[str, Any]]:
    raw: list[tuple[str, str, float, float, float | None, int, bool]] = []
    seen: set[str] = set()
    for dex, state in states:
        for ap in state.get("assetPositions") or []:
            pos = ap.get("position", ap) if isinstance(ap, dict) else {}
            szi = fnum(pos.get("szi"))
            if abs(szi) < 1e-12:
                continue
            coin = coin_key(str(pos.get("coin") or ""), dex)
            if not coin or coin in seen:
                continue
            seen.add(coin)
            lev_raw = pos.get("leverage") or {}
            if isinstance(lev_raw, dict):
                lev = max(1, int(fnum(lev_raw.get("value"), 1)))
                isolated = str(lev_raw.get("type") or "").lower() == "isolated"
            else:
                lev = max(1, int(fnum(lev_raw, 1)))
                isolated = False
            notional = abs(fnum(pos.get("positionValue")))
            if notional <= 0:
                entry = fnum(pos.get("entryPx"))
                notional = abs(szi) * entry if entry > 0 else 0.0
            raw.append(
                (
                    coin,
                    "long" if szi > 0 else "short",
                    abs(szi),
                    notional,
                    fnum(pos.get("entryPx")) or None,
                    lev,
                    isolated,
                )
            )
    equity = max(account_value, sum(p[3] for p in raw), 1e-9)
    out: list[dict[str, Any]] = []
    for coin, side, size, notional, entry_px, lev, isolated in raw:
        signed = 1.0 if side == "long" else -1.0
        out.append(
            {
                "coin": coin,
                "side": side,
                "size": size,
                "notional": notional,
                "entry_px": entry_px,
                "leverage": lev,
                "isolated": isolated,
                "conviction": (notional / equity) * signed,
            }
        )
    return out


def fingerprint(positions: list[dict[str, Any]]) -> str:
    parts = []
    for p in sorted(positions, key=lambda x: x["coin"]):
        parts.append(f"{p['coin']}:{p['side']}:{round(float(p['conviction']), 3)}")
    return "|".join(parts)


def snapshot_wallet(client: HyperliquidPublic, address: str, now: float) -> dict[str, Any]:
    addr = address.lower()
    try:
        raw = client.post_info(
            {"type": "clearinghouseState", "user": addr, "dex": "ALL_DEXES"}
        )
        states = iter_clearinghouse_states(raw)
        if not states:
            raw = client.post_info({"type": "clearinghouseState", "user": addr})
            states = iter_clearinghouse_states(raw)
        equity = account_value_from_states(states)
        positions = parse_positions(states, equity)
        return {
            "address": addr,
            "account_value": equity,
            "positions": positions,
            "fetched_at": now,
            "fingerprint": fingerprint(positions),
            "ok": True,
            "error": "",
        }
    except Exception as exc:
        return {
            "address": addr,
            "account_value": 0.0,
            "positions": [],
            "fetched_at": now,
            "fingerprint": "",
            "ok": False,
            "error": str(exc)[:300],
        }
