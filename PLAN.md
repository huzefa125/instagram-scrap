# Build plan

Follows the architecture diagram (`Untitled-2026-09-23-1145.svg`). The current distributed queue uses MongoDB leases; Redis/Celery/Docker are not required for this phase.

## Code structure

| Package | Responsibility |
|---|---|
| `scraper/core/` | Configuration, account definitions, environment loading |
| `scraper/accounts/` | Session files, account state and locks, rate limits, request warm-up policy, opt-in daily activity |
| `scraper/jobs/` | Shared queue, checkpoints, account workers, role-aware local distribution |
| `scraper/collection/` | Instagram calls, influencer and audience pipelines, run setup |
| `scraper/storage/` | JSON pilot store and MongoDB production store |
| `scraper/analytics/` | Calculations over collected profiles and posts |
| `scraper/api/` | HTTP routes and Swagger documentation |
| `scraper/cli.py` | CLI commands; `scraper/__main__.py` is the entry point |

Tracing lives in `scraper/observability/`; the dashboard UI belongs in its own application. The diagram shows the intended system; the table below maps its boxes to the current implementation.

| Diagram box | Module |
|---|---|
| Influencer List (2,500–3,000) | `influencers.csv`, or usernames in `POST /jobs` |
| Job Queue | `scraper/jobs/queue.py`: MongoDB leases across servers, JSON storage for local runs |
| Python Workers (instagrapi) | `scraper/jobs/worker.py`, `scraper/collection/collectors.py`, `scraper/collection/pipeline.py` |
| Session Manager | `scraper/accounts/session.py`, one saved session per account in `sessions/` |
| Proxy Configuration (authorized stable egress) | one proxy per account in `accounts.json` |
| Rate Limit / Challenge Handler | `scraper/accounts/rate_limit.py` |
| Retry & Checkpoint Manager ⇄ Job Queue | `scraper/jobs/checkpoint.py`; failed influencers go back on the queue (max 3 attempts) |
| Influencer Profile → Recent Posts (20–30) | engagement accounts, stages `profile`, `posts` |
| Post Engagement → Commenters / Likers | engagement accounts, stage `engagement` (`LIKERS_PER_POST`, `COMMENTS_PER_POST`) |
| Unique Profile Deduplication | `audience` collection keyed by user id, atomic insert |
| Public Profile Data + Recent Profile Posts (≤10) | audience accounts, every unique engager (`AUDIENCE_PROFILES_PER_INFLUENCER=0`) |
| MongoDB | `scraper/storage/store.py`: MongoDB Atlas via `MONGO_URI`, JSON files otherwise |
| Audience Analytics Dashboard | REST API with Swagger docs: `scraper/api/app.py`, `scraper/analytics/service.py`; dashboard UI is pending |

## Done
- **Collector**: config, storage, sessions, rate-limit guard, checkpoints, CLI (`login`, `run`, `status`).
- **Safety** (instagrapi best-practices guide): per-error handling, account freeze, daily budget, error log.
- **Analytics API**: `python -m scraper api` → http://127.0.0.1:8000/docs. Engagement rate, audience mix,
  follower-size buckets, categories, cities, activity, suspicious %, repeat engagers, overlap.
- **Multiple accounts**: `accounts.json` with roles `engagement` / `audience` / `both`; accounts work in
  parallel; a frozen account's work is picked up by the others.
- **Local fair distribution**: `scraper/jobs/distribution.py` chooses randomly among the eligible
  accounts with the fewest Instagram requests in the current worker run.
- **Opt-in daily activity**: `scraper/accounts/activity.py` stores five randomly timed actions per
  account per local day, using the worker's session, proxy, lock and request budget. Targets and
  exact comment text must be configured; see `ACTIVITY.md`. Reel metadata checks are not playback.

- **Proxy health**: `scraper/accounts/proxy.py` checks each account's proxy every `PROXY_CHECK_MINUTES`,
  pins its first IP, and pauses only that account when the proxy is down or its IP changes. It never
  rotates proxies or falls back to the direct IP. `python -m scraper proxy [--accept-ip]`.
- **Tracing**: `scraper/observability/tracing.py` writes a span for every Instagram call, activity and job to
  `data/traces/*.jsonl` (kept `TRACE_DAYS`). `python -m scraper trace` shows counts, errors, p50/p95 latency.

## Scaling to 100+ influencers/day (cloud servers, 50–200 accounts)
1. **Done: shared queue + always-on worker.** Queue lives in MongoDB (`scraper/jobs/queue.py`): influencer
   and engager jobs are claimed with leases, so any number of servers share the work and a crashed
   server's jobs are picked up by others. `python -m scraper worker` runs a server's accounts forever;
   frozen / out-of-budget accounts sleep and resume. Account locks stop one account running on two servers.
   Commands: `enqueue`, `worker`, `run` (pilot), `status`. API: `POST /jobs` (enqueue), `GET /queue`.
2. **Done: automatic warm-up ramp.** Each account starts at `WARMUP_START_PER_DAY` (100) and grows
   `WARMUP_GROWTH_PERCENT` (15%) per working day up to its max; idle days don't count; a block cuts it to
   `WARMUP_DROP_PERCENT` (50%). `python -m scraper warmup` shows/adjusts it (`--set`, `--ready`, `--restart`).
   Still to do here: pacing each account's requests across active hours.
3. Harden multi-server ownership: conditional lease renewal/release, atomic request accounting, worker crash recovery.
4. Add tracing and global fairness across servers. Local workers now share jobs approximately evenly within each role while randomizing account turns. With one engagement and four audience accounts, 400 audience Instagram calls target about 100 per audience account when all are available; engagement work stays with the engagement account. Jobs that use different numbers of calls can make the final request totals differ slightly.
5. Add active-hour pacing and session recovery, then precomputed analytics per influencer + overlap via MongoDB aggregation.
6. Add monitoring endpoints (throughput, account health, ETA), dashboard UI, service setup, and deploy guide.

## Safety rules (from instagrapi's best-practices guide)
- One dedicated account per stable proxy, never rotated. `login` checks each account's proxy IP first,
  and a warning is logged if two accounts share a proxy.
- Log in once per account; every later run reuses the saved session and device.
- Region (country, locale, timezone) is set on the first login only.
- Collection stays read-only. Optional daily activity can like/comment only when explicitly enabled
- One request at a time per account (no concurrency within an account).
- Each block type is handled differently (`scraper/accounts/rate_limit.py`). After a challenge, an action block or
  repeated "please wait", that account is frozen and sits out until the cooldown ends.
- Daily request budget per account (`MAX_REQUESTS_PER_DAY`, overridable per account) for gradual warmup.
- Every Instagram error goes to the `events` collection with the account name; `status` summarises them.

## Capacity
Requests per influencer, with defaults (25 posts, 50 likers + up to 50 commenters per post):
- Engagement: ~2 (profile, posts) + 2 per post = **~52 requests**, giving up to ~1,250 unique engagers.
- Audience: 1–2 per engager (profile, plus recent posts if public) = **~1,000–2,500 requests**.

So audience lookups are ~95% of the work. At 500 requests/day per account, plan roughly
**1 engagement account per 10 audience accounts**, and about 2–5 audience-account-days per influencer.
