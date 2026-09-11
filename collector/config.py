from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _f(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return float(raw)


def _i(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return int(float(raw))


def _s(name: str, default: str) -> str:
    raw = os.environ.get(name, "").strip()
    return raw if raw else default


@dataclass(frozen=True)
class Settings:
    database_url: str
    venue: str
    hl_info_url: str
    data_dir: Path
    basket_size: int
    rank_window: str
    snapshot_interval_hours: float
    leaderboard_refresh_hours: float
    dex_scope: str
    min_notional_usd: float
    request_gap_s: float
    snapshot_retries: int
    min_coverage: float
    request_timeout_s: float
    leaderboard_timeout_s: float

    @property
    def cohort(self) -> str:
        win = self.rank_window.strip().lower()
        return f"top{self.basket_size}_{win}"


def load_settings() -> Settings:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL is required. Use the Neon direct URI "
            "(host without -pooler, sslmode=require)."
        )
    data_dir = Path(_s("DATA_DIR", "data"))
    if not data_dir.is_absolute():
        data_dir = Path(__file__).resolve().parent.parent / data_dir
    return Settings(
        database_url=url,
        venue=_s("VENUE", "hyperliquid"),
        hl_info_url=_s("HL_INFO_URL", "https://api.hyperliquid.xyz/info").rstrip("/"),
        data_dir=data_dir,
        basket_size=max(1, _i("BASKET_SIZE", 200)),
        rank_window=_s("RANK_WINDOW", "week"),
        snapshot_interval_hours=max(1.0, _f("SNAPSHOT_INTERVAL_HOURS", 1.0)),
        leaderboard_refresh_hours=max(1.0, _f("LEADERBOARD_REFRESH_HOURS", 1.0)),
        dex_scope=_s("DEX_SCOPE", "include").lower(),
        min_notional_usd=max(0.0, _f("MAJORITY_MIN_NOTIONAL_USD", 50.0)),
        request_gap_s=max(0.05, _f("REQUEST_GAP_S", 0.15)),
        snapshot_retries=max(1, _i("SNAPSHOT_RETRIES", 3)),
        min_coverage=min(1.0, max(0.0, _f("MIN_COVERAGE", 0.70))),
        request_timeout_s=_f("HL_REQUEST_TIMEOUT_S", 30.0),
        leaderboard_timeout_s=_f("HL_LEADERBOARD_TIMEOUT_S", 90.0),
    )
