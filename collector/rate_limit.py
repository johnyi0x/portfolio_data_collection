"""Hyperliquid IP budget for the collector (info REST only).

Hyperliquid: 1200 weight/min per IP. clearinghouseState is weight 2.
Same numbers the trading script uses; this process is the only client on Railway.
"""

from __future__ import annotations

import logging
import time

IP_WEIGHT_PER_MINUTE = 1200
WEIGHT_CLEARINGHOUSE = 2
WEIGHT_DEFAULT_INFO = 20


def info_weight(req_type: str) -> int:
    if req_type in (
        "clearinghouseState",
        "spotClearinghouseState",
        "allMids",
        "l2Book",
        "orderStatus",
        "exchangeStatus",
    ):
        return WEIGHT_CLEARINGHOUSE
    return WEIGHT_DEFAULT_INFO


class IpGuard:
    def __init__(
        self,
        *,
        min_interval_s: float,
        reserve: int,
        logger: logging.Logger,
    ) -> None:
        self.min_interval_s = max(0.05, float(min_interval_s))
        self.reserve = max(0, int(reserve))
        self.log = logger
        self._last_send = 0.0
        self._minute = 0
        self._used = 0

    def wait(self, weight: int) -> None:
        weight = max(1, int(weight))
        cap = IP_WEIGHT_PER_MINUTE - self.reserve
        while True:
            minute = int(time.time()) // 60
            if minute != self._minute:
                self._minute = minute
                self._used = 0
            if self._used + weight <= cap:
                self._used += weight
                break
            wait_s = 60 - (time.time() % 60) + 0.05
            self.log.warning(
                "HL IP weight %s/%s — pause %.0fs so we stay under the cap",
                self._used,
                IP_WEIGHT_PER_MINUTE,
                wait_s,
            )
            time.sleep(min(wait_s, 5.0))
        now = time.monotonic()
        gap = self._last_send + self.min_interval_s - now
        if gap > 0:
            time.sleep(gap)
        self._last_send = time.monotonic()
