"""Account safety state: freeze, request budget, warm-up ramp, and error log."""

import uuid
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

from .warmup import calculate_limit, describe_warmup

# Only these last_json fields are kept; never cookies, tokens or credentials.
LAST_JSON_FIELDS = ("message", "feedback_message", "feedback_title", "error_type", "status", "spam")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _today() -> date:
    return date.today()


def proxy_host(proxy_url: str) -> str | None:
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    return f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname


class AccountState:
    def __init__(self, store, username: str, proxy_url: str = ""):
        self.store = store
        self.username = username or "default"
        self.proxy = proxy_host(proxy_url)

    def get(self) -> dict:
        return self.store.get("account_state", self.username) or {"_id": self.username}

    # --- freeze -----------------------------------------------------------

    def frozen(self) -> tuple[bool, str | None, str | None]:
        doc = self.get()
        until = doc.get("frozen_until")
        if until and datetime.fromisoformat(until) > _now():
            return True, doc.get("freeze_reason"), until
        return False, None, None

    def freeze(self, hours: float, reason: str) -> str:
        until = (_now() + timedelta(hours=hours)).isoformat()
        self.store.upsert("account_state", self.username, {
            "frozen_until": until, "freeze_reason": reason,
            # Warm-up: the daily limit is cut the next time it is computed (see effective_limit).
            "last_block_at": _now().isoformat(), "warmup_block_applied": False,
        })
        return until

    # --- server lock: one account runs on one server at a time -------------

    def acquire_lock(self, owner: str, ttl: timedelta) -> bool:
        """Take the lock if free or expired, or keep it if `owner` already holds it."""
        self.store.insert_if_missing("account_state", self.username, {"lock_until": None})
        now = _now()
        took = self.store.claim("account_state", {"_id": self.username},
                                {"lock_owner": owner, "lock_until": (now + ttl).isoformat()},
                                now=now.isoformat(), lease_field="lock_until")
        if took:
            return True
        if self.get().get("lock_owner") == owner:  # restarted before our own lock expired
            self.renew_lock(owner, ttl)
            return True
        return False

    def renew_lock(self, owner: str, ttl: timedelta) -> None:
        self.store.upsert("account_state", self.username,
                          {"lock_owner": owner, "lock_until": (_now() + ttl).isoformat()})

    def release_lock(self, owner: str) -> None:
        if self.get().get("lock_owner") == owner:
            self.store.upsert("account_state", self.username, {"lock_owner": None, "lock_until": None})

    def lock_holder(self) -> str | None:
        doc = self.get()
        until = doc.get("lock_until")
        return doc.get("lock_owner") if until and until > _now().isoformat() else None

    def mark_needs_login(self, reason: str) -> None:
        self.store.upsert("account_state", self.username, {"needs_login": True, "needs_login_reason": reason})

    def clear_needs_login(self) -> None:
        self.store.upsert("account_state", self.username, {"needs_login": False, "needs_login_reason": None})

    # --- daily budget -----------------------------------------------------

    def requests_today(self) -> int:
        doc = self.get()
        return doc.get("requests_today", 0) if doc.get("day") == _today().isoformat() else 0

    def count_request(self, n: int = 1) -> None:
        self.store.upsert("account_state", self.username, {
            "day": _today().isoformat(),
            "requests_today": self.requests_today() + n,
        })

    # --- warm-up ramp -------------------------------------------------------
    #
    # New accounts start at WARMUP_START_PER_DAY. Each day the account actually worked (used at
    # least half of that day's limit) the limit grows WARMUP_GROWTH_PERCENT, up to the account's
    # maximum. Idle days don't count. After a block (freeze) the limit drops to
    # WARMUP_DROP_PERCENT of its level, and it climbs again from there.

    def effective_limit(self, max_per_day: int, config, persist: bool = True) -> int:
        """Today's request limit. persist=False only looks (status pages); the worker persists."""
        limit, updates = calculate_limit(self.get(), max_per_day, config, _today())
        if persist and updates:
            self.store.upsert("account_state", self.username, updates)
        return limit

    def warmup_info(self, max_per_day: int, config) -> dict:
        return describe_warmup(self.get(), max_per_day, config, _today())

    def set_warmup_level(self, level: float) -> None:
        self.store.upsert("account_state", self.username, {
            "warmup_level": float(level), "warmup_last_ramp": _today().isoformat(),
            "warmup_started": self.get().get("warmup_started") or _today().isoformat(),
            "warmup_block_applied": True,
        })

    def restart_warmup(self) -> None:
        self.store.upsert("account_state", self.username, {
            "warmup_level": None, "warmup_started": None, "warmup_last_ramp": None, "warmup_block_applied": True,
        })

    # --- error log --------------------------------------------------------

    def log_event(self, method: str, exc: Exception, last_json: dict | None, status_code: int | None,
                  action: str) -> None:
        last_json = last_json or {}
        self.store.upsert("events", uuid.uuid4().hex, {
            "at": _now().isoformat(),
            "account": self.username,
            "proxy": self.proxy,
            "method": method,
            "exception": type(exc).__name__,
            "message": str(exc)[:300],
            "http_status": status_code,
            "last_json": {k: last_json[k] for k in LAST_JSON_FIELDS if k in last_json},
            "action": action,
        })
