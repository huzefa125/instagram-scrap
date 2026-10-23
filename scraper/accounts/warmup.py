"""Pure warm-up calculations; account state handles persistence."""

import math
from datetime import date


def calculate_limit(doc: dict, max_per_day: int, config, today: date) -> tuple[int, dict]:
    """Return today's effective limit and any state changes to persist."""
    if not config.warmup_enabled:
        return max_per_day, {}

    today_key = today.isoformat()
    level = doc.get("warmup_level")
    updates: dict = {}

    if level is None:
        level = float(config.warmup_start)
        updates = {"warmup_level": level, "warmup_started": today_key, "warmup_last_ramp": today_key}
    elif doc.get("last_block_at") and not doc.get("warmup_block_applied"):
        level = max(float(config.warmup_start), level * config.warmup_drop)
        updates = {"warmup_level": level, "warmup_block_applied": True, "warmup_last_ramp": today_key}
    elif (doc.get("warmup_last_ramp") or today_key) < today_key:
        last_ramp = doc["warmup_last_ramp"]
        worked_day = doc.get("day")
        used = doc.get("requests_today", 0)
        worked = (worked_day and last_ramp <= worked_day < today_key
                  and used >= 0.5 * min(level, max_per_day))
        if worked:
            level = min(level * (1 + config.warmup_growth), float(max_per_day))
        updates = {"warmup_level": level, "warmup_last_ramp": today_key}

    return max(1, round(min(level, max_per_day))), updates


def describe_warmup(doc: dict, max_per_day: int, config, today: date) -> dict:
    """Read-only account warm-up status for the CLI and API."""
    limit, _ = calculate_limit(doc, max_per_day, config, today)
    started = doc.get("warmup_started")
    days_left = 0
    if config.warmup_enabled and limit < max_per_day:
        days_left = math.ceil(math.log(max_per_day / limit) / math.log(1 + config.warmup_growth))
    return {
        "enabled": config.warmup_enabled,
        "limit_today": limit,
        "max_per_day": max_per_day,
        "stage": "off" if not config.warmup_enabled else ("ready" if limit >= max_per_day else "warming"),
        "day": (today - date.fromisoformat(started)).days + 1 if started else 0,
        "working_days_to_max": days_left,
        "last_block_at": doc.get("last_block_at"),
    }
