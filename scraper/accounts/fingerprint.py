"""Read-only audit of each account's saved device values (no login, no Instagram calls)."""

import json
from collections import defaultdict
from pathlib import Path

ID_FIELDS = ("phone_id", "uuid", "client_session_id", "advertising_id", "android_device_id", "device_id")


def load_session(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def device_summary(settings: dict) -> dict:
    device = settings.get("device_settings") or {}
    uuids = settings.get("uuids") or {}
    return {
        "model": f"{device.get('manufacturer', '?')} {device.get('model', '?')}",
        "android": device.get("android_release", "?"),
        "app_version": device.get("app_version", "?"),
        "country": settings.get("country"),
        "locale": settings.get("locale"),
        "timezone_offset": settings.get("timezone_offset"),
        "ids": {k: v for k, v in {**uuids, "android_device_id": settings.get("android_device_id")}.items()
                if k in ID_FIELDS and v},
    }


def audit(summaries: dict[str, dict], geo: dict[str, dict] | None = None) -> list[tuple[str, str]]:
    """summaries: username -> device_summary. geo: username -> {"country": "IN", "offset": 19800}."""
    problems: list[tuple[str, str]] = []
    owners: dict[str, list[str]] = defaultdict(list)
    for name, info in summaries.items():
        for key, value in info["ids"].items():
            owners[f"{key}={value}"].append(name)
        if not info["ids"]:
            problems.append(("warn", f"{name}: session has no device ids yet; log in once first"))
        place = (geo or {}).get(name)
        if place:
            if info["country"] and place.get("country") and info["country"] != place["country"]:
                problems.append(("warn", f"{name}: session country {info['country']} but proxy is in {place['country']}"))
            if info["timezone_offset"] is not None and place.get("offset") is not None \
                    and int(info["timezone_offset"]) != int(place["offset"]):
                problems.append(("warn", f"{name}: session timezone offset {info['timezone_offset']} "
                                         f"but proxy region is {place['offset']}"))
    for ident, names in owners.items():
        if len(names) > 1:
            problems.append(("error", f"{ident.split('=')[0]} is shared by {', '.join(sorted(names))}"))
    return problems
