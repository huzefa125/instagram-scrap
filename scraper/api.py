"""REST API with Swagger docs.

  python -m scraper api            -> http://127.0.0.1:8000/docs

Set API_KEY in .env to require an `X-API-Key` header on every request.
"""

import logging
import os
import threading
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from . import analytics
from .account import AccountState, proxy_host
from .config import ROOT, load_accounts, load_config
from .jobqueue import WorkQueue
from .runner import CannotStart, build_accounts, clean_usernames, daily_limit, read_usernames, why_not_ready
from .worker import Worker
from .storage import open_store

log = logging.getLogger(__name__)
config = load_config()
store = open_store(config)

# --- auth -------------------------------------------------------------------

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_key(key: str | None = Security(api_key_header)) -> None:
    expected = os.getenv("API_KEY", "").strip()
    if expected and key != expected:
        raise HTTPException(status_code=401, detail="Missing or wrong X-API-Key")


# --- optional in-process worker (python -m scraper api --worker) ---------------

_worker: dict = {"enabled": False, "running": False, "accounts": [], "error": None}


def _start_worker() -> None:
    _worker["enabled"] = True
    try:
        accounts, skipped = build_accounts(config, store, force=False, interactive=False, always_on=True)
    except CannotStart as exc:
        _worker["error"] = str(exc)
        log.error("In-process worker not started: %s", exc)
        return
    worker = Worker(config, store, accounts, config.worker_id)
    thread = threading.Thread(target=worker.run, name="worker", daemon=True)
    thread.start()
    _worker.update(running=True, accounts=[a.name for a in accounts], skipped=skipped, obj=worker, thread=thread)


def _stop_worker() -> None:
    worker, thread = _worker.get("obj"), _worker.get("thread")
    if worker:
        worker.stop()
        thread.join(timeout=120)
        _worker["running"] = False


@asynccontextmanager
async def lifespan(_app):
    if os.getenv("SCRAPER_API_WORKER") == "1":
        _start_worker()
    yield
    _stop_worker()


app = FastAPI(
    title="Instagram Audience API",
    version="2.0.0",
    description=(
        "Read collected influencer, post and audience data, get audience analytics, "
        "and add influencers to the collection queue.\n\n"
        "`POST /jobs` only queues influencers. Workers (`python -m scraper worker`, one per server) "
        "pick them up: **engagement** accounts collect profiles, posts, likers and commenters; "
        "**audience** accounts look up each unique engager's public profile and recent posts. "
        "Each account keeps its own proxy, session, daily budget, freeze state and server lock."
    ),
    dependencies=[Depends(require_key)],
    lifespan=lifespan,
)

# --- models -----------------------------------------------------------------


class Page(BaseModel):
    total: int
    skip: int
    limit: int
    items: list[dict[str, Any]]


class InfluencerSummary(BaseModel):
    username: str
    full_name: str | None = None
    follower_count: int | None = None
    is_private: bool | None = None
    is_verified: bool | None = None
    posts_collected: int = 0
    unique_engagers: int = 0
    status: str | None = None
    stages_done: list[str] = []


class AccountStatus(BaseModel):
    username: str
    role: str
    proxy: str | None = Field(None, description="Proxy host only, never credentials")
    has_session: bool
    requests_today: int
    max_requests_per_day: int
    frozen: bool
    frozen_until: str | None = None
    freeze_reason: str | None = None
    ready: bool = Field(description="Can work right now")
    not_ready_reason: str | None = None
    running_on: str | None = Field(None, description="Server (WORKER_ID) currently running this account")
    needs_login: bool = False


class StatusResponse(BaseModel):
    influencers: int
    influencer_statuses: dict[str, int]
    posts: int
    audience_unique: int
    audience_waiting: int = Field(description="Engagers still waiting for a profile lookup")
    queue: dict[str, Any]
    accounts: list[AccountStatus]
    errors_by_type: dict[str, int]


class JobRequest(BaseModel):
    usernames: list[str] = Field(
        default_factory=list,
        description="Usernames or profile URLs. Empty = use influencers.csv.",
        examples=[["natgeo", "https://www.instagram.com/nasa/"]],
    )
    limit: int | None = Field(None, ge=1, description="Only process the first N usernames")
    retry_failed: bool = Field(False, description="Queue influencers that failed or were skipped again")


class EnqueueResult(BaseModel):
    usernames: int
    enqueued: int
    already_queued: int
    already_done: int
    requeued: int
    queue: dict[str, Any]


# --- helpers ----------------------------------------------------------------

HEAVY_FIELDS = ("audience_ids", "post_ids")


def _get_influencer(username: str) -> dict:
    doc = store.get("influencers", username.lower().lstrip("@"))
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Influencer '{username}' not collected")
    return doc


def _page(items: list[dict], skip: int, limit: int) -> Page:
    return Page(total=len(items), skip=skip, limit=limit, items=items[skip:skip + limit])


def _summary(doc: dict, cp: dict | None) -> InfluencerSummary:
    cp = cp or {}
    return InfluencerSummary(
        username=doc["_id"],
        full_name=doc.get("full_name"),
        follower_count=doc.get("follower_count"),
        is_private=doc.get("is_private"),
        is_verified=doc.get("is_verified"),
        posts_collected=len(doc.get("post_ids", [])),
        unique_engagers=len(doc.get("audience_ids", [])),
        status=cp.get("status"),
        stages_done=cp.get("done", []),
    )


def _account_statuses() -> list[AccountStatus]:
    from .session import session_file

    rows = []
    for acc in load_accounts(config):
        state = AccountState(store, acc.username, acc.proxy)
        frozen, reason, until = state.frozen()
        not_ready = why_not_ready(config, store, acc, force=False, need_session=True)
        rows.append(AccountStatus(
            username=acc.username, role=acc.role, proxy=proxy_host(acc.proxy),
            has_session=session_file(config, acc.username).exists(),
            requests_today=state.requests_today(), max_requests_per_day=daily_limit(config, acc),
            frozen=frozen, frozen_until=until, freeze_reason=reason,
            ready=not_ready is None, not_ready_reason=not_ready,
            running_on=state.lock_holder(), needs_login=bool(state.get().get("needs_login")),
        ))
    return rows


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- system -----------------------------------------------------------------


@app.get("/health", tags=["System"])
def health() -> dict:
    return {
        "ok": True,
        "storage": "mongodb" if config.mongo_uri else "json",
        "accounts": len(load_accounts(config)),
        "in_process_worker": {k: v for k, v in _worker.items() if k not in ("obj", "thread")},
        "active_workers": WorkQueue(store).active_workers(),
    }


@app.get("/status", tags=["System"], response_model=StatusResponse)
def status() -> StatusResponse:
    q = WorkQueue(store).counts()
    return StatusResponse(
        influencers=store.count("influencers"),
        influencer_statuses=q["influencers"],
        posts=store.count("posts"),
        audience_unique=store.count("audience"),
        audience_waiting=q["audience_waiting"],
        queue=q,
        accounts=_account_statuses(),
        errors_by_type=dict(Counter(e["exception"] for e in store.find("events"))),
    )


@app.get("/accounts", tags=["System"], response_model=list[AccountStatus],
         summary="Every Instagram account: role, session, budget, freeze")
def accounts() -> list[AccountStatus]:
    return _account_statuses()


@app.get("/events", tags=["System"], response_model=Page, summary="Instagram error log")
def events(
    exception: str | None = Query(None, description="Filter by exception name, e.g. ClientThrottledError"),
    account: str | None = Query(None, description="Filter by Instagram account"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> Page:
    items = [e for e in store.find("events")
             if (not exception or e["exception"] == exception) and (not account or e["account"] == account)]
    items.sort(key=lambda e: e["at"], reverse=True)
    return _page(items, skip, limit)


# --- influencers ------------------------------------------------------------


@app.get("/influencers", tags=["Influencers"], response_model=list[InfluencerSummary])
def list_influencers(
    status: str | None = Query(None, description="complete | in_progress | retry | failed | skipped"),
) -> list[InfluencerSummary]:
    docs = list(store.find("influencers"))
    cps = store.get_many("checkpoints", [d["_id"] for d in docs])
    rows = [_summary(doc, cps.get(doc["_id"])) for doc in docs]
    if status:
        rows = [r for r in rows if r.status == status]
    return sorted(rows, key=lambda r: r.username)


@app.get("/influencers/{username}", tags=["Influencers"], summary="Full influencer profile")
def get_influencer(username: str) -> dict:
    doc = _get_influencer(username)
    out = {k: v for k, v in doc.items() if k not in HEAVY_FIELDS}
    out["posts_collected"] = len(doc.get("post_ids", []))
    out["unique_engagers"] = len(doc.get("audience_ids", []))
    out["checkpoint"] = store.get("checkpoints", doc["_id"])
    return out


@app.get("/influencers/{username}/posts", tags=["Influencers"], response_model=Page)
def influencer_posts(
    username: str,
    include_engagement: bool = Query(False, description="Include liker ids and comments"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> Page:
    ids = _get_influencer(username).get("post_ids", [])
    found = store.get_many("posts", ids)
    posts = [found[i] for i in ids if i in found]
    if not include_engagement:
        posts = [{k: v for k, v in p.items() if k not in ("liker_ids", "comments")} for p in posts]
    return _page(posts, skip, limit)


@app.get("/influencers/{username}/audience", tags=["Influencers"], response_model=Page)
def influencer_audience(
    username: str,
    enriched: bool | None = Query(None, description="Only users with (true) or without (false) full profiles"),
    is_private: bool | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> Page:
    ids = _get_influencer(username).get("audience_ids", [])
    found = store.get_many("audience", ids)
    users = [found[i] for i in ids if i in found]
    if enriched is not None:
        users = [u for u in users if bool(u.get("enriched")) == enriched]
    if is_private is not None:
        users = [u for u in users if bool(u.get("is_private")) == is_private]
    return _page(users, skip, limit)


@app.get("/influencers/{username}/analytics", tags=["Analytics"], summary="Audience analytics for one influencer")
def influencer_analytics(username: str) -> dict:
    return analytics.influencer_analytics(store, _get_influencer(username)["_id"])


@app.get("/influencers/{username}/overlap", tags=["Analytics"], summary="Influencers sharing this audience")
def influencer_overlap(username: str, top: int = Query(20, ge=1, le=200)) -> dict:
    return analytics.audience_overlap(store, _get_influencer(username)["_id"], top)


@app.get("/analytics", tags=["Analytics"], summary="Analytics for every collected influencer")
def all_analytics() -> list[dict]:
    ids = sorted(doc["_id"] for doc in store.find("influencers"))
    return [analytics.influencer_analytics(store, u) for u in ids]


# --- audience ---------------------------------------------------------------


@app.get("/audience/{user_id}", tags=["Audience"], summary="One audience member (deduplicated profile)")
def get_audience_member(user_id: str) -> dict:
    doc = store.get("audience", user_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Audience user {user_id} not found")
    return doc


# --- jobs -------------------------------------------------------------------


@app.post("/jobs", tags=["Jobs"], response_model=EnqueueResult, status_code=202,
          summary="Add influencers to the collection queue")
def enqueue_jobs(body: JobRequest) -> EnqueueResult:
    if body.usernames:
        usernames = clean_usernames(body.usernames)
    else:
        csv_path = ROOT / "influencers.csv"
        usernames = read_usernames(csv_path) if csv_path.exists() else []
    if body.limit:
        usernames = usernames[:body.limit]
    if not usernames:
        raise HTTPException(status_code=400, detail="No usernames given and influencers.csv is missing or empty")
    queue = WorkQueue(store)
    counts = queue.enqueue(usernames, retry_failed=body.retry_failed)
    return EnqueueResult(usernames=len(usernames), **counts, queue=queue.counts())


@app.get("/queue", tags=["Jobs"], summary="Queue depth: influencers and engagers waiting")
def queue_status() -> dict:
    return WorkQueue(store).counts()


# --- one influencer: request it, then poll for the result ------------------------


class ScrapeStatus(BaseModel):
    username: str
    state: str = Field(description="queued | collecting_engagement | looking_up_audience | complete | failed | skipped")
    stages_done: list[str] = []
    attempts: int = 0
    last_error: str | None = None
    posts: int = 0
    engagers: int = 0
    audience_to_look_up: int = 0
    audience_looked_up: int = 0
    percent_done: float = 0
    workers_running: list[str] = Field(description="Servers with a worker running; empty = nothing will process it")
    message: str | None = None
    analytics: dict | None = Field(None, description="Audience analytics; filled when state is complete (or ?partial=true)")


def _scrape_status(username: str, partial: bool = False) -> ScrapeStatus:
    queue = WorkQueue(store)
    progress = queue.progress(username)
    if progress is None:
        raise HTTPException(status_code=404, detail=f"'{username}' was never requested. POST /scrape/{username} first.")
    workers = queue.active_workers()
    message = None
    if not workers and progress["state"] not in ("complete", "failed", "skipped"):
        message = "No worker is running. Start one: `python -m scraper worker`, or run the API with --worker."
    elif progress["state"] == "skipped":
        message = "Skipped: the account is private or does not exist."
    result = None
    if store.get("influencers", username) and (progress["state"] == "complete" or partial):
        result = analytics.influencer_analytics(store, username)
    return ScrapeStatus(**progress, workers_running=workers, message=message, analytics=result)


@app.post("/scrape/{username}", tags=["Scrape"], response_model=ScrapeStatus, status_code=202,
          summary="Request one influencer (username, @name or profile URL)")
def request_scrape(username: str, retry_failed: bool = Query(False, description="Request again if it failed or was skipped")) -> ScrapeStatus:
    names = clean_usernames([username])
    if not names:
        raise HTTPException(status_code=400, detail="Empty username")
    WorkQueue(store).enqueue(names, retry_failed=retry_failed)
    return _scrape_status(names[0])


@app.get("/scrape/{username}", tags=["Scrape"], response_model=ScrapeStatus,
         summary="Progress of one influencer, and its analytics when complete")
def scrape_status(username: str, partial: bool = Query(False, description="Include analytics before the audience is complete")) -> ScrapeStatus:
    names = clean_usernames([username])
    return _scrape_status(names[0] if names else username, partial)
