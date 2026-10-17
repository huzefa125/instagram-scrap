"""Retry & Checkpoint Manager: remembers which stages finished for each influencer."""

from datetime import datetime, timezone

# Per-influencer stages (engagement accounts). Audience lookups are tracked per user
# (`enriched` on each audience doc) and done by audience accounts.
STAGES = ["profile", "posts", "engagement"]
MAX_ATTEMPTS = 3


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Checkpoints:
    def __init__(self, store):
        self.store = store

    def get(self, username: str) -> dict:
        return self.store.get("checkpoints", username) or {"_id": username, "done": [], "attempts": 0}

    def is_done(self, username: str, stage: str) -> bool:
        return stage in self.get(username).get("done", [])

    def mark_done(self, username: str, stage: str) -> None:
        done = self.get(username).get("done", [])
        if stage not in done:
            done = done + [stage]
        status = "complete" if all(s in done for s in STAGES) else "in_progress"
        self.store.upsert("checkpoints", username, {"done": done, "status": status, "updated_at": _now()})

    def mark_failed(self, username: str, error: str) -> None:
        attempts = self.get(username).get("attempts", 0) + 1
        status = "failed" if attempts >= MAX_ATTEMPTS else "retry"
        self.store.upsert(
            "checkpoints", username,
            {"attempts": attempts, "status": status, "last_error": error, "updated_at": _now()},
        )

    def mark_skipped(self, username: str, reason: str) -> None:
        self.store.upsert("checkpoints", username, {"status": "skipped", "last_error": reason, "updated_at": _now()})

    def should_process(self, username: str) -> bool:
        return self.get(username).get("status") not in ("complete", "failed", "skipped")
