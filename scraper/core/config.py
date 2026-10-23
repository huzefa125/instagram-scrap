import json
import logging
import os
import re
import socket
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
ACCOUNTS_FILE = ROOT / "accounts.json"
load_dotenv(ROOT / ".env")
log = logging.getLogger(__name__)

ROLES = ("engagement", "audience", "both", "activity")


def _int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def _float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else default


@dataclass(frozen=True)
class Config:
    ig_username: str
    ig_password: str
    proxy_url: str
    mongo_uri: str
    mongo_db: str
    data_dir: Path
    session_dir: Path
    posts_per_influencer: int
    likers_per_post: int
    comments_per_post: int
    audience_profiles_per_influencer: int
    audience_recent_posts: int
    ig_country: str
    ig_country_code: int
    ig_locale: str
    ig_timezone_offset: int
    delay_min: float
    delay_max: float
    warmup_delay_min: float
    warmup_delay_max: float
    max_requests_per_run: int
    max_requests_per_day: int
    worker_id: str
    poll_seconds: float
    warmup_enabled: bool
    warmup_start: int
    warmup_growth: float     # 0.15 = +15% per working day
    warmup_drop: float       # 0.5 = cut to 50% after a block
    activity_enabled: bool
    activity_file: Path
    activity_browser: bool = False       # watch feed Reels in a real browser
    browser_headless: bool = True
    activity_mobile: bool = False        # watch feed Reels in Instagram Lite via Appium instead
    devices_file: Path = Path("devices.json")
    proxy_check_minutes: int = 30
    trace_days: int = 14


@dataclass(frozen=True)
class AccountConfig:
    """One Instagram account: its own login, proxy, session, budget and freeze state."""
    username: str
    password: str
    proxy: str = ""
    role: str = "both"                 # engagement | audience | both | activity (daily activity only, no collection)
    max_requests_per_day: int | None = None   # None = MAX_REQUESTS_PER_DAY


def load_config() -> Config:
    return Config(
        ig_username=os.getenv("IG_USERNAME", "").strip(),
        ig_password=os.getenv("IG_PASSWORD", "").strip(),
        proxy_url=os.getenv("PROXY_URL", "").strip(),
        mongo_uri=os.getenv("MONGO_URI", "").strip(),
        mongo_db=os.getenv("MONGO_DB", "instagram_audience").strip(),
        data_dir=ROOT / os.getenv("DATA_DIR", "data").strip(),
        session_dir=ROOT / "sessions",
        posts_per_influencer=_int("POSTS_PER_INFLUENCER", 25),
        likers_per_post=_int("LIKERS_PER_POST", 50),
        comments_per_post=_int("COMMENTS_PER_POST", 50),
        audience_profiles_per_influencer=_int("AUDIENCE_PROFILES_PER_INFLUENCER", 0),
        audience_recent_posts=_int("AUDIENCE_RECENT_POSTS", 10),
        ig_country=os.getenv("IG_COUNTRY", "IN").strip() or "IN",
        ig_country_code=_int("IG_COUNTRY_CODE", 91),
        ig_locale=os.getenv("IG_LOCALE", "en_IN").strip() or "en_IN",
        ig_timezone_offset=_int("IG_TIMEZONE_OFFSET", 19800),
        delay_min=_float("DELAY_MIN", 2),
        delay_max=_float("DELAY_MAX", 6),
        # Longer pauses while an account is still inside its warm-up ramp.
        warmup_delay_min=_float("WARMUP_DELAY_MIN", 15),
        warmup_delay_max=_float("WARMUP_DELAY_MAX", 45),
        max_requests_per_run=_int("MAX_REQUESTS_PER_RUN", 2000),
        max_requests_per_day=_int("MAX_REQUESTS_PER_DAY", 500),
        # Names this server in the shared queue and account locks; must differ per server.
        worker_id=os.getenv("WORKER_ID", "").strip() or socket.gethostname(),
        poll_seconds=_float("WORKER_POLL_SECONDS", 30),
        warmup_enabled=os.getenv("WARMUP", "on").strip().lower() not in ("off", "0", "false", "no"),
        warmup_start=_int("WARMUP_START_PER_DAY", 100),
        warmup_growth=_float("WARMUP_GROWTH_PERCENT", 15) / 100,
        warmup_drop=_float("WARMUP_DROP_PERCENT", 50) / 100,
        activity_enabled=os.getenv("ACTIVITY_ENABLED", "off").strip().lower() in ("on", "1", "true", "yes"),
        activity_file=ROOT / (os.getenv("ACTIVITY_FILE", "activity.json").strip() or "activity.json"),
        activity_browser=os.getenv("ACTIVITY_BROWSER", "off").strip().lower() in ("on", "1", "true", "yes"),
        browser_headless=os.getenv("BROWSER_HEADLESS", "on").strip().lower() in ("on", "1", "true", "yes"),
        activity_mobile=os.getenv("ACTIVITY_MOBILE", "off").strip().lower() in ("on", "1", "true", "yes"),
        devices_file=ROOT / (os.getenv("DEVICES_FILE", "devices.json").strip() or "devices.json"),
        proxy_check_minutes=_int("PROXY_CHECK_MINUTES", 30),
        trace_days=_int("TRACE_DAYS", 14),
    )


def _numbered_env_accounts() -> list[AccountConfig]:
    """IG_USERNAME_1 / IG_PASSWORD_1 / PROXY_URL_1 / IG_ROLE_1 / IG_MAX_PER_DAY_1, then _2, _3 ...

    Numbers may have gaps; all configured numeric suffixes are discovered (including 200+).
    """
    accounts = []
    numbers = sorted({int(match.group(1)) for key in os.environ
                      if (match := re.fullmatch(r"IG_USERNAME_([1-9][0-9]*)", key))})
    for n in numbers:
        username = os.getenv(f"IG_USERNAME_{n}", "").strip()
        if not username:
            continue
        max_per_day = os.getenv(f"IG_MAX_PER_DAY_{n}", "").strip()
        accounts.append(AccountConfig(
            username=username,
            password=os.getenv(f"IG_PASSWORD_{n}", "").strip(),
            proxy=os.getenv(f"PROXY_URL_{n}", "").strip(),
            role=(os.getenv(f"IG_ROLE_{n}", "") or "both").strip().lower(),
            max_requests_per_day=int(max_per_day) if max_per_day else None,
        ))
    return accounts


def load_accounts(config: Config, path: Path = ACCOUNTS_FILE) -> list[AccountConfig]:
    """Accounts, first match wins:
    1. accounts.json
    2. numbered accounts in .env (IG_USERNAME_1, IG_USERNAME_2, ...)
    3. the single IG_USERNAME account in .env
    """
    numbered = _numbered_env_accounts()
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8"))
        accounts = []
        for item in raw:
            role = (item.get("role") or "both").lower()
            if role not in ROLES:
                raise SystemExit(f"accounts.json: role for {item.get('username')} must be one of {ROLES}")
            accounts.append(AccountConfig(
                username=item["username"].strip(),
                password=item["password"],
                proxy=(item.get("proxy") or "").strip(),
                role=role,
                max_requests_per_day=item.get("max_requests_per_day"),
            ))
    elif numbered:
        accounts = numbered
    elif config.ig_username:
        accounts = [AccountConfig(config.ig_username, config.ig_password, config.proxy_url, "both")]
    else:
        accounts = []

    for acc in accounts:
        if acc.role not in ROLES:
            raise SystemExit(f"Account {acc.username}: role must be one of {ROLES}, got '{acc.role}'")
        if not acc.password:
            raise SystemExit(f"Account {acc.username}: password is missing")

    # Guide: one stable proxy per account. Warn when accounts share one.
    seen: dict[str, str] = {}
    for acc in accounts:
        if acc.proxy and acc.proxy in seen:
            log.warning("Accounts %s and %s share the same proxy; give each account its own", seen[acc.proxy], acc.username)
        seen.setdefault(acc.proxy, acc.username)
    return accounts
