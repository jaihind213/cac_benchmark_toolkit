"""
pipeline/timer.py
-----------------
Shared utilities: timing, logging, year parsing.
"""

import csv
import functools
import logging
import time
from datetime import datetime
from pathlib import Path

LOG_DIR = Path("results/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cac.timer")


def _write_log(step: str, scope: str, elapsed_ms: float, rows: int = None, note: str = ""):
    row = {
        "timestamp":  datetime.now().isoformat(),
        "step":       step,
        "scope":      scope,
        "elapsed_ms": round(elapsed_ms),
        "elapsed_s":  round(elapsed_ms / 1000, 2),
        "rows":       rows or "",
        "note":       note,
    }
    write_header = not LOG_FILE.exists()
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            w.writeheader()
        w.writerow(row)
    logger.info(
        f"[{step}] {scope} | {elapsed_ms:.0f}ms"
        + (f" | {rows:,} rows" if rows else "")
        + (f" | {note}" if note else "")
    )


def timed(step_name: str, scope: str = ""):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            _scope = scope or _infer_scope(args, kwargs)
            t0 = time.perf_counter()
            result = fn(*args, **kwargs)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            rows = None
            if isinstance(result, tuple) and len(result) == 2:
                result, rows = result
            _write_log(step_name, _scope, elapsed_ms, rows)
            return result
        return wrapper
    return decorator


def _infer_scope(args, kwargs) -> str:
    if "date" in kwargs:
        return f"date={kwargs['date']}"
    if args:
        return str(args[0])
    return ""


class TimerLog:
    def __init__(self, step: str, scope: str = "", note: str = ""):
        self.step  = step
        self.scope = scope
        self.note  = note
        self.rows  = None
        self._t0   = None

    def __enter__(self):
        self._t0 = time.perf_counter()
        logger.info(f"[{self.step}] starting — {self.scope}")
        return self

    def __exit__(self, *_):
        elapsed_ms = (time.perf_counter() - self._t0) * 1000
        _write_log(self.step, self.scope, elapsed_ms, self.rows, self.note)


def parse_years(years_arg: list[str]) -> list[int]:
    """
    Parse years argument supporting both list and range:
      --years 2009 2010 2011     → [2009, 2010, 2011]
      --years 2009-2015          → [2009, 2010, 2011, 2012, 2013, 2014, 2015]
    """
    result = []
    for y in years_arg:
        y = str(y)
        if "-" in y and not y.lstrip("-").isdigit():
            start, end = y.split("-")
            result.extend(range(int(start), int(end) + 1))
        else:
            result.append(int(y))
    return sorted(set(result))


def paths_for_entity(base_dir: str, entity: str, years: list[int] = None) -> list[Path]:
    """Return sorted parquet files for an entity, optionally filtered by year."""
    root = Path(base_dir) / "entities" / entity
    if years:
        files = sorted(f for y in years for f in root.glob(f"{y}/**/*.parquet"))
    else:
        files = sorted(root.glob("**/*.parquet"))
    return files


def entity_dir(base_dir: str, entity: str) -> Path:
    """Return the entity directory under base_dir."""
    return Path(base_dir) / "entities" / entity
