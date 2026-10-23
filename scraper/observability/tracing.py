"""Lightweight tracing written as one JSON line per span in data/traces/YYYY-MM-DD.jsonl.

Spans nest through a context variable, so an Instagram call made during a daily activity or an
influencer job carries that parent's trace id. Only safe fields are written: names, account
usernames, durations, error class names and short identifiers. Never put passwords, proxy
URLs, cookies or comment text into `attrs`. Without `configure()` every span is a no-op.
"""

import contextvars
import json
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

_dir: Path | None = None
_lock = threading.Lock()
_current: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar("span", default=None)


def configure(directory: Path, keep_days: int = 14) -> None:
    """Enable tracing and delete trace files older than `keep_days`."""
    global _dir
    directory.mkdir(parents=True, exist_ok=True)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).date().isoformat()
    for path in directory.glob("*.jsonl"):
        if path.stem < cutoff:
            path.unlink(missing_ok=True)
    _dir = directory


@contextmanager
def span(name: str, account: str | None = None, **attrs):
    if _dir is None:
        yield
        return
    parent = _current.get()
    trace_id = parent[0] if parent else uuid.uuid4().hex[:12]
    span_id = uuid.uuid4().hex[:8]
    token = _current.set((trace_id, span_id))
    started = datetime.now(timezone.utc)
    clock = time.perf_counter()
    status, error = "ok", None
    try:
        yield
    except BaseException as exc:
        status, error = "error", type(exc).__name__
        raise
    finally:
        _current.reset(token)
        record = {"ts": started.isoformat(), "trace": trace_id, "span": span_id,
                  "parent": parent[1] if parent else None, "name": name, "account": account,
                  "ms": round((time.perf_counter() - clock) * 1000), "status": status,
                  "error": error, "attrs": attrs or None}
        try:
            with _lock, (_dir / f"{started.date().isoformat()}.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record) + "\n")
        except OSError:
            pass  # tracing must never break the work it observes


def read_spans(directory: Path, days: int = 1, account: str | None = None) -> list[dict]:
    spans: list[dict] = []
    for offset in range(days - 1, -1, -1):
        day = (datetime.now(timezone.utc) - timedelta(days=offset)).date().isoformat()
        path = directory / f"{day}.jsonl"
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if account is None or item.get("account") == account:
                spans.append(item)
    return spans


def summarize(spans: list[dict]) -> dict[str, dict]:
    """Per span name: count, errors, p50 and p95 duration in milliseconds."""
    groups: dict[str, list[dict]] = {}
    for item in spans:
        groups.setdefault(item["name"], []).append(item)
    out = {}
    for name, items in sorted(groups.items()):
        times = sorted(i["ms"] for i in items)

        def pick(q: float) -> int:
            return times[min(len(times) - 1, int(q * len(times)))]

        out[name] = {"count": len(items), "errors": sum(i["status"] == "error" for i in items),
                     "p50_ms": pick(0.5), "p95_ms": pick(0.95)}
    return out
