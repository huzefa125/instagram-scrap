"""Command line interface.

  python -m scraper login                     check each account's proxy IP, log in, save sessions
  python -m scraper login --account NAME      just one account
  python -m scraper run natgeo                collect one influencer now and wait until it's done
  python -m scraper run natgeo @nasa          several usernames (or a CSV file of them)
  python -m scraper enqueue natgeo            add to the shared queue for the workers
  python -m scraper worker                    always-on worker for this server's accounts (a service)
  python -m scraper status                    queue, per-account state and server locks, errors
  python -m scraper warmup                    each account's warm-up: today's limit, day, days to max
  python -m scraper warmup --account NAME --set 400 | --ready | --restart
  python -m scraper activity --account NAME   preview a daily activity plan (no Instagram calls)
  python -m scraper view --account NAME --login | [--count 3]   log in / watch Reels in a visible browser, or --mobile for the phone
  python -m scraper proxy [--account NAME] [--accept-ip]   check proxies; confirm a deliberate IP change
  python -m scraper trace [--account NAME] [--days 1] [--errors]   request timings and failures
  python -m scraper candidates --account NAME [--status pending]   Reels found in the feed
  python -m scraper candidates --account NAME --approve CODE --text "..." | --reject CODE
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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from instagrapi.exceptions import ClientError

from .accounts.activity import load_activity_settings, make_daily_actions, validate_comment
from .accounts.proxy import ProxyMonitor
from .accounts.fingerprint import audit, device_summary, load_session
from .accounts.rules import check_accounts
from .accounts.state import AccountState
from .observability import read_spans, summarize
from .core.config import load_accounts, load_config
from .jobs.queue import WorkQueue
from .collection.runner import CannotStart, daily_limit, resolve_targets, run_collection, run_worker
from .accounts.session import SessionManager, session_file
from .storage.store import open_store


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
        os.environ["SCRAPER_API_WORKER"] = "1"  # read by scraper.api.app at startup
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
        w = state.warmup_info(daily_limit(config, acc), config)
        if w["stage"] == "warming":
            day = f"day {w['day']}" if w["day"] else "not started"
            flags.append(f"warm-up {day} (max {w['max_per_day']} in ~{w['working_days_to_max']} working days)")
        print(f"  {acc.username:24s} {acc.role:10s} {state.requests_today():4d}/{w['limit_today']} today  "
              + "  ".join(flags))

    events = list(store.find("events"))
    if events:
        print(f"Errors:       {len(events)} logged")
        for name, count in Counter(e["exception"] for e in events).most_common():
            print(f"  {count:5d}  {name}")
        print("  by account: " + ", ".join(f"{a} {c}" for a, c in Counter(e["account"] for e in events).most_common()))


def cmd_warmup(config, account: str | None, set_level: int | None, ready: bool, restart: bool) -> None:
    store = open_store(config)
    accounts = [a for a in load_accounts(config) if not account or a.username == account]
    if account and not accounts:
        raise SystemExit(f"No account named {account}")
    if (set_level is not None or ready or restart) and not account:
        raise SystemExit("Use --account NAME with --set / --ready / --restart")
    for acc in accounts:
        state = AccountState(store, acc.username, acc.proxy)
        max_per_day = daily_limit(config, acc)
        if set_level is not None:
            state.set_warmup_level(min(set_level, max_per_day))
        elif ready:
            state.set_warmup_level(max_per_day)
        elif restart:
            state.restart_warmup()
    if not config.warmup_enabled:
        print("Warm-up is OFF (WARMUP=off): every account uses its full daily limit.")
    print(f"{'account':24s} {'stage':8s} {'today':>9s} {'max':>6s} {'day':>4s}  to max   (day '-' = not started)")
    for acc in accounts:
        w = AccountState(store, acc.username, acc.proxy).warmup_info(daily_limit(config, acc), config)
        to_max = f"~{w['working_days_to_max']} working days" if w["stage"] == "warming" else "-"
        day = str(w["day"]) if w["day"] else "-"
        print(f"{acc.username:24s} {w['stage']:8s} {w['limit_today']:>9d} {w['max_per_day']:>6d} {day:>4s}  {to_max}")
    print()
    print(f"Ramp: start {config.warmup_start}/day, +{config.warmup_growth:.0%} per working day, "
          f"cut to {config.warmup_drop:.0%} after a block.")


def cmd_activity(config, account: str, file: Path | None, day: str | None) -> None:
    """Preview a possible plan. The worker persists its own random plan when enabled."""
    try:
        settings = load_activity_settings(file or config.activity_file)
        local_tz = timezone(timedelta(seconds=config.ig_timezone_offset))
        chosen_day = date.fromisoformat(day) if day else datetime.now(local_tz).date()
        actions = make_daily_actions(settings, account, chosen_day, config.ig_timezone_offset)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"Cannot plan activity: {exc}") from exc
    print(f"Activity preview for {account} on {chosen_day} ({settings.actions_per_account} actions):")
    for action in actions:
        when = datetime.fromisoformat(action["due_at"]).astimezone(local_tz).strftime("%H:%M")
        detail = f" | comment: {action['text']}" if action["kind"] == "comment" else ""
        print(f"  {when}  {action['kind']:10s} {action['target']}{detail}")
    print("Preview only. Running workers execute persisted plans when ACTIVITY_ENABLED=on.")


def cmd_proxy(config, account: str | None, accept_ip: bool) -> None:
    """Check each account's proxy now. Proxies are never swapped automatically."""
    accounts = [a for a in load_accounts(config) if not account or a.username == account]
    if not accounts:
        raise SystemExit("No matching accounts.")
    monitor = ProxyMonitor(open_store(config), {a.username: a.proxy for a in accounts}, config.proxy_check_minutes)
    for acc in accounts:
        try:
            doc = monitor.accept_ip(acc.username) if accept_ip and acc.proxy else monitor.check(acc.username, force=True)
        except Exception as exc:  # noqa: BLE001 - report and carry on with the next account
            print(f"{acc.username:24s} cannot reach proxy: {type(exc).__name__}")
            continue
        detail = f"ip {doc.get('ip')} ({doc.get('latency_ms')} ms)" if doc.get("ip") else (doc.get("error") or "")
        print(f"{acc.username:24s} {doc.get('status', '?'):11s} {detail}")


def cmd_rules(config) -> None:
    """Check account/proxy pairing rules without logging in or touching Instagram."""
    problems = check_accounts(load_accounts(config))
    for level, message in problems:
        print(f"{level.upper():5s} {message}")
    errors = sum(1 for level, _ in problems if level == "error")
    print("All rules pass." if not problems else f"{errors} error(s), {len(problems) - errors} warning(s).")
    if errors:
        raise SystemExit(1)


def cmd_fingerprint(config, account: str | None, online: bool) -> None:
    """Show each saved session's device values and flag mismatches; --online also checks the proxy's region."""
    summaries, geo = {}, {}
    for acc in load_accounts(config):
        if account and acc.username != account:
            continue
        settings = load_session(session_file(config, acc.username))
        if settings is None:
            print(f"{acc.username:24s} no saved session yet (run: python -m scraper login)")
            continue
        info = summaries[acc.username] = device_summary(settings)
        print(f"{acc.username:24s} {info['model']}, Android {info['android']}, app {info['app_version']}, "
              f"{info['country']}/{info['locale']}, tz {info['timezone_offset']}")
        if online and acc.proxy:
            try:
                import requests
                data = requests.get("http://ip-api.com/json?fields=status,countryCode,offset",
                                    proxies={"http": acc.proxy, "https": acc.proxy}, timeout=20).json()
                if data.get("status") == "success":
                    geo[acc.username] = {"country": data["countryCode"], "offset": data["offset"]}
            except Exception as exc:  # noqa: BLE001 - report and carry on
                print(f"{'':24s} proxy region lookup failed: {type(exc).__name__}")
    for level, message in audit(summaries, geo):
        print(f"{level.upper():5s} {message}")


def cmd_trace(config, account: str | None, days: int, errors: bool) -> None:
    spans = read_spans(config.data_dir / "traces", days, account)
    if not spans:
        raise SystemExit("No trace data yet. Tracing starts when the worker runs.")
    print(f"{'span':28s} {'count':>6s} {'errors':>6s} {'p50 ms':>8s} {'p95 ms':>8s}")
    for name, row in summarize(spans).items():
        print(f"{name:28s} {row['count']:6d} {row['errors']:6d} {row['p50_ms']:8d} {row['p95_ms']:8d}")
    failed = [s for s in spans if s["status"] == "error"]
    print(f"\nLast {'errors' if errors else 'failures'}:")
    for s in (failed if errors else failed[-10:])[-30:]:
        print(f"  {s['ts'][11:19]}  {s.get('account') or '-':20s} {s['name']:26s} {s['error']}  trace={s['trace']}")


def cmd_view_browser(config, account: str, login: bool, count: int) -> None:
    """Log a browser profile in by hand, or watch a few Reels in a visible browser and save them."""
    from .accounts.viewer import BrowserViewer, ViewerError
    cfg = next((a for a in load_accounts(config) if a.username == account), None)
    if cfg is None:
        raise SystemExit(f"Unknown account {account}")
    viewer = BrowserViewer(cfg.username, cfg.proxy, headless=False, locale=config.ig_locale.replace("_", "-"))
    if login:
        viewer.login()
        open_store(config).upsert("account_state", account, {"browser_needs_login": False})
        return
    try:
        reels = viewer.watch(count, show=True)
    except ViewerError as exc:
        raise SystemExit(str(exc)) from exc
    store = open_store(config)
    for r in reels:
        store.insert_if_missing("reel_candidates", f"{account}:{r.code}", {
            "account": account, "code": r.code, "reel_url": r.url, "author": r.author,
            "caption": r.caption, "like_count": None, "comment_count": None,
            "watched_seconds": r.watched_seconds, "status": "pending", "comment": None,
            "found_at": datetime.now(timezone.utc).isoformat()})
        print(f"watched {r.url} for {r.watched_seconds:.0f}s")


def cmd_view(config, account: str, login: bool, count: int, mobile: bool = False) -> None:
    """Log in by hand / watch a few Reels: in a visible browser, or on the account's phone with --mobile."""
    if not mobile:
        return cmd_view_browser(config, account, login, count)
    from .accounts.mobile_viewer import APP_ACTIVITY, APP_PACKAGE, DEFAULT_APPIUM_URL, MobileViewer, load_devices
    from .accounts.viewer import ViewerError
    try:
        device = load_devices(config.devices_file).get(account)
    except ViewerError as exc:
        raise SystemExit(str(exc)) from exc
    if device is None:
        raise SystemExit(f"{account} has no phone in {config.devices_file.name} (see devices.example.json)")
    viewer = MobileViewer(account, device["udid"], device.get("appium_url", DEFAULT_APPIUM_URL),
                          device.get("package", APP_PACKAGE), device.get("activity", APP_ACTIVITY))
    if login:
        viewer.login()
        open_store(config).upsert("account_state", account, {"browser_needs_login": False})
        return
    try:
        reels = viewer.watch(count)
    except ViewerError as exc:
        raise SystemExit(str(exc)) from exc
    for r in reels:
        print(f"watched a Reel for {r.watched_seconds:.0f}s")


def cmd_candidates(config, account: str, status: str, approve: str | None, reject: str | None,
                   text: str | None) -> None:
    """List feed Reels found by `feed_watch`; approving one with text lets `feed_comment` post it."""
    store = open_store(config)
    if approve or reject:
        code = approve or reject
        candidate = store.get("reel_candidates", f"{account}:{code}")
        if candidate is None or candidate["status"] != "pending":
            raise SystemExit(f"No pending candidate {code} for {account}")
        if reject:
            store.upsert("reel_candidates", candidate["_id"], {"status": "rejected"})
            print(f"Rejected {code}")
            return
        try:
            comment = validate_comment(text or "")
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        store.upsert("reel_candidates", candidate["_id"], {"status": "approved", "comment": comment})
        print(f"Approved {code}; it will be posted at the account's next feed_comment slot.")
        return
    rows = [c for c in store.find("reel_candidates") if c["account"] == account and c["status"] == status]
    for c in sorted(rows, key=lambda c: c["found_at"]):
        print(f"{c['code']}  @{c['author']}  {c['like_count']} likes / {c['comment_count']} comments"
              f"\n    {c['caption'][:120]!r}\n    {c['reel_url']}")
    print(f"{len(rows)} {status} candidate(s) for {account}")


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
    warm = sub.add_parser("warmup", help="show or adjust each account's warm-up ramp")
    warm.add_argument("--account", help="only this account (required to change it)")
    warm.add_argument("--set", type=int, dest="set_level", help="set today's limit for the account")
    warm.add_argument("--ready", action="store_true", help="account is already warm: jump to its max")
    warm.add_argument("--restart", action="store_true", help="start the ramp again from WARMUP_START_PER_DAY")
    activity = sub.add_parser("activity", help="preview five daily account activities")
    activity.add_argument("--account", required=True, help="configured Instagram account username")
    activity.add_argument("--file", type=Path, help="activity JSON file (default: ACTIVITY_FILE)")
    activity.add_argument("--day", help="local day in YYYY-MM-DD format (default: today)")
    view = sub.add_parser("view", help="log in a browser profile, or watch Reels in a visible browser (--mobile: on the phone via Appium)")
    view.add_argument("--account", required=True)
    view.add_argument("--login", action="store_true", help="open a browser (or, with --mobile, the phone app) to log in by hand")
    view.add_argument("--mobile", action="store_true", help="use the account's phone from devices.json instead of a browser")
    view.add_argument("--count", type=int, default=3)
    px = sub.add_parser("proxy", help="check proxy health")
    px.add_argument("--account")
    px.add_argument("--accept-ip", action="store_true", help="confirm the proxy's current IP is the intended one")
    fp = sub.add_parser("fingerprint", help="audit saved device values and region fit (read-only)")
    fp.add_argument("--account")
    fp.add_argument("--online", action="store_true", help="also look up the proxy's country/timezone")
    sub.add_parser("rules", help="check account and proxy rules (no login, no network)")
    tr = sub.add_parser("trace", help="request timings and failures from the worker")
    tr.add_argument("--account")
    tr.add_argument("--days", type=int, default=1)
    tr.add_argument("--errors", action="store_true", help="list every error, not just the last 10")
    cand = sub.add_parser("candidates", help="review Reels found in the account's Explore feed")
    cand.add_argument("--account", required=True)
    cand.add_argument("--status", default="pending", choices=["pending", "approved", "posted", "rejected", "uncertain"])
    cand.add_argument("--approve", metavar="CODE")
    cand.add_argument("--reject", metavar="CODE")
    cand.add_argument("--text", help="exact comment to post when approving")
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
    elif args.command == "warmup":
        cmd_warmup(config, args.account, args.set_level, args.ready, args.restart)
    elif args.command == "activity":
        cmd_activity(config, args.account, args.file, args.day)
    elif args.command == "proxy":
        cmd_proxy(config, args.account, args.accept_ip)
    elif args.command == "fingerprint":
        cmd_fingerprint(config, args.account, args.online)
    elif args.command == "rules":
        cmd_rules(config)
    elif args.command == "trace":
        cmd_trace(config, args.account, args.days, args.errors)
    elif args.command == "view":
        cmd_view(config, args.account, args.login, args.count, args.mobile)
    elif args.command == "candidates":
        cmd_candidates(config, args.account, args.status, args.approve, args.reject, args.text)
    else:
        cmd_status(config)


if __name__ == "__main__":
    main()
