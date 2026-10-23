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

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Security
from fastapi.responses import RedirectResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from ..analytics import service as analytics
from ..accounts.state import AccountState, proxy_host
from ..core.config import ROOT, load_accounts, load_config
from ..jobs.queue import WorkQueue
from ..collection.runner import CannotStart, build_accounts, clean_usernames, daily_limit, read_usernames, why_not_ready
from ..jobs.worker import Worker
from ..storage.store import open_store

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


DESCRIPTION = """
Collect an influencer's posts, likers, commenters and audience, then read it back as JSON.

## Quick start (3 steps)
1. **Request**: `POST /scrape/{username}` with e.g. `narendramodi`.
2. **Watch**: `GET /scrape/{username}` until `state` is `complete` (`percent_done` shows progress).
3. **Download**: `GET /influencers/{username}/export` gives everything in one JSON.

Nothing is collected unless a worker is running (`python -m scraper worker`, or start this API with `--worker`).
`GET /health` and `GET /accounts` tell you if the workers and accounts are fine.

Each account has its own proxy, session, daily budget and freeze state. An account that Instagram
challenged stays parked until you confirm it in the Instagram app and call `POST /accounts/{username}/verified`.

If `API_KEY` is set, press **Authorize** (top right) and enter it once; Swagger remembers it.
"""

TAGS = [
    {"name": "Scrape", "description": "**Start here.** Request one influencer and follow its progress."},
    {"name": "Export", "description": "Download the collected data of one influencer as a single JSON."},
    {"name": "Accounts", "description": "Instagram accounts: budget, warm-up, freezes. Fix a challenged account here."},
    {"name": "System", "description": "Health, errors and overall status."},
    {"name": "Influencers", "description": "Browse stored influencers, posts and audience."},
    {"name": "Analytics", "description": "Audience analytics and overlap between influencers."},
    {"name": "Audience", "description": "One deduplicated audience member."},
    {"name": "Jobs", "description": "Queue many influencers at once and see the queue."},
]

app = FastAPI(
    title="Instagram Audience API",
    version="2.1.0",
    description=DESCRIPTION,
    openapi_tags=TAGS,
    dependencies=[Depends(require_key)],
    lifespan=lifespan,
    swagger_ui_parameters={
        "persistAuthorization": True,       # keep the API key after a refresh
        "displayRequestDuration": True,
        "docExpansion": "none",             # sections start collapsed, open the one you need
        "filter": True,                     # search box
        "tryItOutEnabled": True,            # "Try it out" is already on
        "defaultModelsExpandDepth": -1,     # hide the long schema list at the bottom
    },
)


@app.get("/", include_in_schema=False)
def home() -> RedirectResponse:
    return RedirectResponse("/docs")


UsernamePath = Path(description="Instagram username, @name or profile URL", examples=["narendramodi"])

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
    limit_today: int = Field(description="Today's limit after warm-up")
    max_requests_per_day: int
    warmup_stage: str = Field(description="warming | ready | off")
    warmup_day: int = 0
    working_days_to_max: int = 0
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
    posts: int | None = Field(None, ge=1, le=100, description="Posts per influencer (default: POSTS_PER_INFLUENCER)")
    likers: int | None = Field(None, ge=0, le=5000, description="Likers per post (default: LIKERS_PER_POST)")
    comments: int | None = Field(None, ge=0, le=5000, description="Comments per post (default: COMMENTS_PER_POST)")
    enrich_profiles: int | None = Field(
        None, ge=0, le=100000,
        description="How many engagers get a full profile + recent posts lookup. 0 = everyone "
                    "(default: AUDIENCE_PROFILES_PER_INFLUENCER)")


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
    from ..accounts.session import session_file

    rows = []
    for acc in load_accounts(config):
        state = AccountState(store, acc.username, acc.proxy)
        frozen, reason, until = state.frozen()
        not_ready = why_not_ready(config, store, acc, force=False, need_session=True)
        rows.append(AccountStatus(
            username=acc.username, role=acc.role, proxy=proxy_host(acc.proxy),
            has_session=session_file(config, acc.username).exists(),
            requests_today=state.requests_today(),
            limit_today=(w := state.warmup_info(daily_limit(config, acc), config))["limit_today"],
            max_requests_per_day=daily_limit(config, acc),
            warmup_stage=w["stage"], warmup_day=w["day"], working_days_to_max=w["working_days_to_max"],
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


@app.get("/accounts", tags=["Accounts"], response_model=list[AccountStatus],
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
def get_influencer(username: str = UsernamePath) -> dict:
    doc = _get_influencer(username)
    out = {k: v for k, v in doc.items() if k not in HEAVY_FIELDS}
    out["posts_collected"] = len(doc.get("post_ids", []))
    out["unique_engagers"] = len(doc.get("audience_ids", []))
    out["checkpoint"] = store.get("checkpoints", doc["_id"])
    return out


@app.get("/influencers/{username}/posts", tags=["Influencers"], response_model=Page)
def influencer_posts(
    username: str = UsernamePath,
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
    username: str = UsernamePath,
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
def influencer_analytics(username: str = UsernamePath) -> dict:
    return analytics.influencer_analytics(store, _get_influencer(username)["_id"])


@app.get("/influencers/{username}/overlap", tags=["Analytics"], summary="Influencers sharing this audience")
def influencer_overlap(username: str = UsernamePath, top: int = Query(20, ge=1, le=200)) -> dict:
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
    counts = queue.enqueue(usernames, retry_failed=body.retry_failed, options={
        "posts": body.posts, "likers": body.likers, "comments": body.comments, "enrich_profiles": body.enrich_profiles})
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
def request_scrape(
    username: str = UsernamePath,
    retry_failed: bool = Query(False, description="Request again if it failed or was skipped"),
    posts: int | None = Query(None, ge=1, le=100, description="Posts to collect (default from .env)"),
    likers: int | None = Query(None, ge=0, le=5000, description="Likers per post (default from .env)"),
    comments: int | None = Query(None, ge=0, le=5000, description="Comments per post (default from .env)"),
    enrich_profiles: int | None = Query(
        None, ge=0, le=100000,
        description="How many engagers get a full profile + recent posts lookup. 0 = everyone (default from .env)"),
) -> ScrapeStatus:
    names = clean_usernames([username])
    if not names:
        raise HTTPException(status_code=400, detail="Empty username")
    WorkQueue(store).enqueue(names, retry_failed=retry_failed, options={
        "posts": posts, "likers": likers, "comments": comments, "enrich_profiles": enrich_profiles})
    return _scrape_status(names[0])


@app.get("/scrape/{username}", tags=["Scrape"], response_model=ScrapeStatus,
         summary="Progress of one influencer, and its analytics when complete")
def scrape_status(username: str = UsernamePath, partial: bool = Query(False, description="Include analytics before the audience is complete")) -> ScrapeStatus:
    names = clean_usernames([username])
    return _scrape_status(names[0] if names else username, partial)


# --- export: everything for one influencer in one JSON ---------------------------

_USER_FIELDS = ("username", "full_name", "is_private", "is_verified", "follower_count",
                "following_count", "media_count", "biography", "category", "public_email",
                "contact_phone_number", "recent_posts")


def _user_row(user_id: str, audience: dict) -> dict:
    doc = audience.get(user_id) or {}
    return {"user_id": user_id, **{k: doc[k] for k in _USER_FIELDS if k in doc}}


@app.get("/influencers/{username}/export", tags=["Export"],
         summary="Everything collected for one influencer as one JSON (posts, likers, comments)")
def export_influencer(
    username: str = UsernamePath,
    posts: int = Query(10, ge=1, le=100, description="Newest N posts"),
    likers: int = Query(200, ge=0, le=5000, description="Max likers per post"),
    comments: int = Query(200, ge=0, le=5000, description="Max comments per post"),
) -> dict:
    inf = _get_influencer(username)
    ids = inf.get("post_ids", [])[:posts]
    found = store.get_many("posts", ids)
    post_docs = [found[i] for i in ids if i in found]
    user_ids = {u for p in post_docs for u in p.get("liker_ids", [])[:likers]}
    user_ids |= {c["user_id"] for p in post_docs for c in p.get("comments", [])[:comments] if c.get("user_id")}
    audience = store.get_many("audience", list(user_ids))
    out_posts = []
    for p in post_docs:
        row = {k: v for k, v in p.items() if k not in ("liker_ids", "comments", "engagement_done")}
        row["likers"] = [_user_row(u, audience) for u in p.get("liker_ids", [])[:likers]]
        row["comments"] = [{**_user_row(c.get("user_id"), audience), "text": c.get("text"),
                            "created_at": c.get("created_at")} for c in p.get("comments", [])[:comments]]
        out_posts.append(row)
    return {"influencer": {k: v for k, v in inf.items() if k not in HEAVY_FIELDS}, "posts": out_posts}


# --- account actions ----------------------------------------------------------------


def _account_state(username: str) -> AccountState:
    acc = next((a for a in load_accounts(config) if a.username == username), None)
    if acc is None:
        raise HTTPException(status_code=404, detail=f"Unknown account '{username}'")
    return AccountState(store, acc.username, acc.proxy)


@app.post("/accounts/{username}/verified", tags=["Accounts"],
          summary="I confirmed this challenged account in the Instagram app: let it work again")
def account_verified(username: str = Path(description="Instagram account from .env", examples=["neer0989"])) -> dict:
    _account_state(username).clear_manual_verification()
    return {"account": username, "cleared": True, "note": "It logs in again the next time a worker starts."}


@app.post("/accounts/{username}/warmup", tags=["Accounts"],
          summary="Set today's request limit for an account (warm-up)")
def account_warmup(
    username: str = Path(description="Instagram account from .env", examples=["manan8754995"]),
    limit_today: int = Query(..., ge=1, le=500, description="Requests allowed today"),
) -> dict:
    _account_state(username).set_warmup_level(limit_today)
    return {"account": username, "limit_today": limit_today}
