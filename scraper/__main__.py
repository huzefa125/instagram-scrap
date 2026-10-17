"""Command line.

  python -m scraper login                     check each account's proxy IP, log in, save sessions
  python -m scraper login --account NAME      just one account
  python -m scraper run natgeo                collect one influencer now and wait until it's done
  python -m scraper run natgeo @nasa          several usernames (or a CSV file of them)
  python -m scraper enqueue natgeo            add to the shared queue for the workers
  python -m scraper worker                    always-on worker for this server's accounts (a service)
  python -m scraper status                    queue, per-account state and server locks, errors
  python -m scraper api                       REST API, Swagger docs at http://127.0.0.1:8000/docs
  python -m scraper api --worker              API + this server's accounts in one process:
                                              POST /scrape/natgeo, then GET /scrape/natgeo

Accounts come from accounts.json, numbered IG_USERNAME_1... in .env, or a single IG_USERNAME.
On several servers, each server's .env lists only its own accounts and has its own WORKER_ID.
"""

import argparse
import logging
import sys
from collections import Counter

from instagrapi.exceptions import ClientError

from .account import AccountState
from .config import load_accounts, load_config
from .jobqueue import WorkQueue
from .runner import CannotStart, daily_limit, resolve_targets, run_collection, run_worker
from .session import SessionManager
from .storage import open_store


def cmd_login(config, only: str | None) -> None:
    accounts = [a for a in load_accounts(config) if not only or a.username == only]
    if not accounts:
        raise SystemExit("No matching accounts. Create accounts.json or set IG_USERNAME in .env.")
    failed = 0
    for acc in accounts:
        print(f"\n== {acc.username} ({acc.role})")
        session = SessionManager(config, acc)
        direct, proxied = session.check_proxy()
        if proxied is None:
            print(f"  No proxy: Instagram sees your own IP ({direct}).")
        elif proxied == direct:
            print(f"  Proxy is set but the IP did not change ({proxied}). Fix the proxy first; skipping.")
            failed += 1
            continue
        else:
            print(f"  IP through proxy: {proxied}")
        reused = session.settings_path.exists()
        try:
            session.login()
        except ClientError as exc:
            print(f"  Login failed: {type(exc).__name__}: {exc}")
            failed += 1
            continue
        print(f"  {'Reused saved session' if reused else 'New session saved'}")
        AccountState(open_store(config), acc.username, acc.proxy).clear_needs_login()
    if failed:
        sys.exit(1)


def cmd_run(config, targets: list[str], limit: int | None, force: bool) -> None:
    usernames = resolve_targets(targets)
    if limit:
        usernames = usernames[:limit]
    try:
        result = run_collection(config, open_store(config), usernames, force)
    except CannotStart as exc:
        logging.error("%s", exc)
        sys.exit(3)
    for name, acc in result["accounts"].items():
        logging.info("%-24s %-10s %4d requests, %d influencers, %d profiles%s", name, acc["role"], acc["requests"],
                     acc["influencers"], acc["profiles"], f" | stopped: {acc['stopped']}" if acc["stopped"] else "")
    if result["stopped"]:
        logging.error("Work left: %d influencers, %s engagers. Run again later to resume.",
                      result["influencers_left"], result["audience_left"])
        sys.exit(2)


def cmd_enqueue(config, targets: list[str], limit: int | None, retry_failed: bool) -> None:
    usernames = resolve_targets(targets)
    if limit:
        usernames = usernames[:limit]
    counts = WorkQueue(open_store(config)).enqueue(usernames, retry_failed)
    print(f"{len(usernames)} usernames: " + ", ".join(f"{v} {k.replace('_', ' ')}" for k, v in counts.items()))


def cmd_worker(config) -> None:
    try:
        result = run_worker(config, open_store(config))
    except CannotStart as exc:
        logging.error("%s", exc)
        sys.exit(3)
    logging.info("Worker stopped: %s", result["accounts"])


def cmd_api(host: str, port: int, worker: bool) -> None:
    import os

    import uvicorn

    if worker:
        os.environ["SCRAPER_API_WORKER"] = "1"  # read by scraper.api at startup
    print(f"Swagger docs: http://{host}:{port}/docs")
    uvicorn.run("scraper.api:app", host=host, port=port)


def cmd_status(config) -> None:
    store = open_store(config)
    q = WorkQueue(store).counts()
    print(f"Influencers:  {q['influencers_pending']} pending  {q['influencers']}")
    print(f"Posts:        {store.count('posts')}")
    print(f"Audience:     {store.count('audience')} unique, {q['audience_done']} looked up, "
          f"{q['audience_waiting']} waiting")

    print(f"Accounts on this server ({config.worker_id}):")
    for acc in load_accounts(config):
        state = AccountState(store, acc.username, acc.proxy)
        frozen, reason, until = state.frozen()
        doc = state.get()
        holder = state.lock_holder()
        flags = []
        if frozen:
            flags.append(f"FROZEN until {until} ({reason})")
        if doc.get("needs_login"):
            flags.append("NEEDS LOGIN")
        if holder:
            flags.append(f"running on {holder}")
        print(f"  {acc.username:24s} {acc.role:10s} {state.requests_today():4d}/{daily_limit(config, acc)} today  "
              + "  ".join(flags))

    events = list(store.find("events"))
    if events:
        print(f"Errors:       {len(events)} logged")
        for name, count in Counter(e["exception"] for e in events).most_common():
            print(f"  {count:5d}  {name}")
        print("  by account: " + ", ".join(f"{a} {c}" for a, c in Counter(e["account"] for e in events).most_common()))


def main() -> None:
    parser = argparse.ArgumentParser(prog="scraper")
    sub = parser.add_subparsers(dest="command", required=True)
    login = sub.add_parser("login")
    login.add_argument("--account", help="only this account")
    run = sub.add_parser("run", help="collect influencers now and wait until done")
    run.add_argument("targets", nargs="+", help="usernames, @names, profile URLs or a CSV file")
    run.add_argument("--limit", type=int)
    run.add_argument("--force", action="store_true", help="run even if accounts are frozen")
    enqueue = sub.add_parser("enqueue", help="add influencers to the shared queue")
    enqueue.add_argument("targets", nargs="+", help="usernames, @names, profile URLs or a CSV file")
    enqueue.add_argument("--limit", type=int)
    enqueue.add_argument("--retry-failed", action="store_true", help="queue failed/skipped influencers again")
    sub.add_parser("worker", help="always-on worker for this server's accounts")
    sub.add_parser("status")
    api = sub.add_parser("api", help="start the REST API with Swagger docs at /docs")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--worker", action="store_true", help="also run this server's accounts in the API process")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
                        datefmt="%H:%M:%S")
    config = load_config()

    if args.command == "login":
        cmd_login(config, args.account)
    elif args.command == "run":
        cmd_run(config, args.targets, args.limit, args.force)
    elif args.command == "enqueue":
        cmd_enqueue(config, args.targets, args.limit, args.retry_failed)
    elif args.command == "worker":
        cmd_worker(config)
    elif args.command == "api":
        cmd_api(args.host, args.port, args.worker)
    else:
        cmd_status(config)


if __name__ == "__main__":
    main()
