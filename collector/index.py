"""Majority portfolio index — same tally as the trading script's majority mode."""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any

from .hl import in_scope


def tally_holds(
    snaps: list[dict[str, Any]],
    *,
    dex_scope: str,
    min_notional_usd: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ok: list[dict[str, Any]] = []
    empty = 0
    errors = 0
    for s in snaps:
        if not s.get("ok"):
            errors += 1
            continue
        ok.append(s)
        if not s.get("positions"):
            empty += 1

    n_ok = len(ok)
    long_n: dict[str, int] = defaultdict(int)
    short_n: dict[str, int] = defaultdict(int)
    long_lev: dict[str, list[int]] = defaultdict(list)
    short_lev: dict[str, list[int]] = defaultdict(list)
    long_conv: dict[str, list[float]] = defaultdict(list)
    short_conv: dict[str, list[float]] = defaultdict(list)
    long_ntl: dict[str, float] = defaultdict(float)
    short_ntl: dict[str, float] = defaultdict(float)

    for s in ok:
        seen: set[str] = set()
        for p in s.get("positions") or []:
            coin = str(p.get("coin") or "")
            if not coin or not in_scope(coin, dex_scope):
                continue
            if float(p.get("notional") or 0) < min_notional_usd:
                continue
            if coin in seen:
                continue
            seen.add(coin)
            lev = max(1, int(p.get("leverage") or 1))
            conv = abs(float(p.get("conviction") or 0))
            ntl = float(p.get("notional") or 0)
            if p.get("side") == "long":
                long_n[coin] += 1
                long_lev[coin].append(lev)
                long_conv[coin].append(conv)
                long_ntl[coin] += ntl
            else:
                short_n[coin] += 1
                short_lev[coin].append(lev)
                short_conv[coin].append(conv)
                short_ntl[coin] += ntl

    coins = set(long_n) | set(short_n)
    rows: list[dict[str, Any]] = []
    for coin in coins:
        ln = int(long_n.get(coin, 0))
        sn = int(short_n.get(coin, 0))
        if ln >= sn and ln > 0:
            side = "long"
            n = ln
            levs = long_lev[coin]
            convs = long_conv[coin]
            ntl = long_ntl[coin]
        else:
            side = "short"
            n = sn
            levs = short_lev[coin]
            convs = short_conv[coin]
            ntl = short_ntl[coin]
        both = ln + sn
        hold_pct = (n / n_ok) if n_ok else 0.0
        agr = (n / both) if both else 0.0
        med = int(round(float(statistics.median(levs)))) if levs else 1
        mean_lev = float(sum(levs) / len(levs)) if levs else 1.0
        avg_c = float(sum(convs) / len(convs)) if convs else 0.0
        rows.append(
            {
                "coin": coin,
                "side": side,
                "wallets": n,
                "hold_pct": hold_pct,
                "agreement": agr,
                "long_n": ln,
                "short_n": sn,
                "median_leverage": med,
                "mean_leverage": mean_lev,
                "avg_conviction": avg_c,
                "notional_usd": ntl,
            }
        )
    rows.sort(key=lambda r: (-r["wallets"], -r["hold_pct"], r["coin"]))
    for i, row in enumerate(rows, start=1):
        row["rank"] = i
    stats = {
        "snapped": len(snaps),
        "ok": n_ok,
        "empty": empty,
        "errors": errors,
        "with_pos": n_ok - empty,
        "coins": len(rows),
    }
    return rows, stats
