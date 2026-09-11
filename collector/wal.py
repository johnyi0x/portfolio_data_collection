from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"), default=str)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        logging.getLogger("collector").warning("Could not read %s", path)
        return default


class CycleWal:
    """Disk copy of each hour so a Neon outage does not drop the cycle."""

    def __init__(self, data_dir: Path) -> None:
        self.dir = data_dir / "wal"
        self.dir.mkdir(parents=True, exist_ok=True)

    def path(self, cycle_key: str) -> Path:
        return self.dir / f"{cycle_key}.json"

    def synced_path(self, cycle_key: str) -> Path:
        return self.dir / f"{cycle_key}.synced"

    def write(self, cycle_key: str, payload: dict[str, Any]) -> Path:
        dest = self.path(cycle_key)
        atomic_write_json(dest, payload)
        return dest

    def load(self, cycle_key: str) -> dict[str, Any] | None:
        raw = read_json(self.path(cycle_key), None)
        return raw if isinstance(raw, dict) else None

    def mark_synced(self, cycle_key: str) -> None:
        p = self.synced_path(cycle_key)
        p.write_text(str(time.time()), encoding="utf-8")

    def is_synced(self, cycle_key: str) -> bool:
        return self.synced_path(cycle_key).exists()

    def unsynced_keys(self) -> list[str]:
        keys: list[str] = []
        for path in sorted(self.dir.glob("*.json")):
            key = path.stem
            if not self.synced_path(key).exists():
                keys.append(key)
        return keys

    def prune_synced(self, keep_days: int = 7) -> None:
        cutoff = time.time() - keep_days * 86400
        for synced in self.dir.glob("*.synced"):
            try:
                if synced.stat().st_mtime >= cutoff:
                    continue
                json_path = self.path(synced.stem)
                json_path.unlink(missing_ok=True)
                synced.unlink(missing_ok=True)
            except OSError:
                continue
