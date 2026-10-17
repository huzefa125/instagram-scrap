# Build plan

Follows the architecture diagram (`Untitled-2026-09-23-1145.svg`). No Redis/Celery/Docker for now.

| Diagram box | Module |
|---|---|
| Influencer List (2,500–3,000) | `influencers.csv`, or usernames in `POST /jobs` |
| Job Queue | `scraper/pool.py`: shared in-memory queues, one thread per account (Celery later if needed) |
| Python Workers (instagrapi) | `scraper/collectors.py`, `scraper/pipeline.py` |
| Session Manager | `scraper/session.py`, one saved session per account in `sessions/` |
| Proxy Configuration (authorized stable egress) | one proxy per account in `accounts.json` |
| Rate Limit / Challenge Handler | `scraper/ratelimit.py` |
| Retry & Checkpoint Manager ⇄ Job Queue | `scraper/checkpoint.py`; failed influencers go back on the queue (max 3 attempts) |
| Influencer Profile → Recent Posts (20–30) | engagement accounts, stages `profile`, `posts` |
| Post Engagement → Commenters / Likers | engagement accounts, stage `engagement` (`LIKERS_PER_POST`, `COMMENTS_PER_POST`) |
| Unique Profile Deduplication | `audience` collection keyed by user id, atomic insert |
| Public Profile Data + Recent Profile Posts (≤10) | audience accounts, every unique engager (`AUDIENCE_PROFILES_PER_INFLUENCER=0`) |
| MongoDB | `scraper/storage.py`: MongoDB Atlas via `MONGO_URI`, JSON files otherwise |
| Audience Analytics Dashboard | REST API with Swagger docs: `scraper/api.py`, `scraper/analytics.py` |

## Done
- **Collector**: config, storage, sessions, rate-limit guard, checkpoints, CLI (`login`, `run`, `status`).
- **Safety** (instagrapi best-practices guide): per-error handling, account freeze, daily budget, error log.
- **Analytics API**: `python -m scraper api` → http://127.0.0.1:8000/docs. Engagement rate, audience mix,
  follower-size buckets, categories, cities, activity, suspicious %, repeat engagers, overlap.
- **Multiple accounts**: `accounts.json` with roles `engagement` / `audience` / `both`; accounts work in
  parallel; a frozen account's work is picked up by the others.

## Scaling to 100+ influencers/day (cloud servers, 50–200 accounts)
1. **Done: shared queue + always-on worker.** Queue lives in MongoDB (`scraper/jobqueue.py`): influencer
   and engager jobs are claimed with leases, so any number of servers share the work and a crashed
   server's jobs are picked up by others. `python -m scraper worker` runs a server's accounts forever;
   frozen / out-of-budget accounts sleep and resume. Account locks stop one account running on two servers.
   Commands: `enqueue`, `worker`, `run` (pilot), `status`. API: `POST /jobs` (enqueue), `GET /queue`.
2. Pacing across active hours + automatic warm-up ramp for new accounts.
3. Precomputed analytics per influencer + overlap via MongoDB aggregation.
4. Monitoring endpoints (throughput, account health, ETA) + systemd service + deploy guide.

## Safety rules (from instagrapi's best-practices guide)
- One dedicated account per stable proxy, never rotated. `login` checks each account's proxy IP first,
  and a warning is logged if two accounts share a proxy.
- Log in once per account; every later run reuses the saved session and device.
- Region (country, locale, timezone) is set on the first login only.
- Read-only: nothing likes, follows, comments, posts or sends DMs.
- One request at a time per account (no concurrency within an account).
- Each block type is handled differently (`scraper/ratelimit.py`). After a challenge, an action block or
  repeated "please wait", that account is frozen and sits out until the cooldown ends.
- Daily request budget per account (`MAX_REQUESTS_PER_DAY`, overridable per account) for gradual warmup.
- Every Instagram error goes to the `events` collection with the account name; `status` summarises them.

## Capacity
Requests per influencer, with defaults (25 posts, 50 likers + up to 50 commenters per post):
- Engagement: ~2 (profile, posts) + 2 per post = **~52 requests**, giving up to ~1,250 unique engagers.
- Audience: 1–2 per engager (profile, plus recent posts if public) = **~1,000–2,500 requests**.

So audience lookups are ~95% of the work. At 500 requests/day per account, plan roughly
**1 engagement account per 10 audience accounts**, and about 2–5 audience-account-days per influencer.
