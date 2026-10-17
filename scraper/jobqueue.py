"""Job Queue shared by every worker on every server, kept in the store (MongoDB in production).

Two kinds of jobs, each claimed with a lease so only one account works on it at a time:

  influencer jobs  = `checkpoints` docs, status queued / in_progress / retry
  audience jobs    = `audience` docs with wanted=True and enriched=False

A claim sets `lease_until`. If a server dies mid-job its lease expires and another account picks
the job up; checkpoints make it resume where it stopped. Leases on long influencer jobs are
renewed after every post (heartbeat).
"""

from datetime import datetime, timedelta, timezone


INFLUENCER_LEASE = timedelta(minutes=20)
AUDIENCE_LEASE = timedelta(minutes=30)
CLAIMABLE = ["queued", "in_progress", "retry"]
NO_LEASE = {"lease_until": None, "claimed_by": None}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class WorkQueue:
    def __init__(self, store):
        self.store = store

    # --- influencers --------------------------------------------------------

    def enqueue(self, usernames: list[str], retry_failed: bool = False) -> dict:
        existing = self.store.get_many("checkpoints", usernames)
        counts = {"enqueued": 0, "already_queued": 0, "already_done": 0, "requeued": 0}
        now = _iso(_now())
        for username in usernames:
            cp = existing.get(username)
            if cp is None:
                self.store.insert_if_missing("checkpoints", username, {
                    "status": "queued", "done": [], "attempts": 0, "enqueued_at": now, **NO_LEASE,
                })
                counts["enqueued"] += 1
            elif cp.get("status") in ("failed", "skipped") and retry_failed:
                self.store.upsert("checkpoints", username, {"status": "queued", "attempts": 0, "enqueued_at": now, **NO_LEASE})
                counts["requeued"] += 1
            elif cp.get("status") == "complete" or cp.get("status") in ("failed", "skipped"):
                counts["already_done"] += 1
            else:
                counts["already_queued"] += 1
        return counts

    def claim_influencer(self, owner: str) -> str | None:
        now = _now()
        doc = self.store.claim(
            "checkpoints", {"status": CLAIMABLE},
            {"lease_until": _iso(now + INFLUENCER_LEASE), "claimed_by": owner},
            now=_iso(now), sort_field="enqueued_at",
        )
        return doc["_id"] if doc else None

    def renew_influencer(self, username: str, owner: str) -> None:
        self.store.upsert("checkpoints", username, {"lease_until": _iso(_now() + INFLUENCER_LEASE), "claimed_by": owner})

    def release_influencer(self, username: str) -> None:
        self.store.upsert("checkpoints", username, NO_LEASE)

    # --- audience -----------------------------------------------------------

    def claim_audience(self, owner: str) -> str | None:
        now = _now()
        doc = self.store.claim(
            "audience", {"wanted": True, "enriched": False},
            {"lease_until": _iso(now + AUDIENCE_LEASE), "claimed_by": owner},
            now=_iso(now),
        )
        return doc["_id"] if doc else None

    def release_audience(self, user_id: str) -> None:
        self.store.upsert("audience", user_id, NO_LEASE)

    # --- status -------------------------------------------------------------

    def counts(self) -> dict:
        influencers = {s: self.store.count_where("checkpoints", {"status": s})
                       for s in ("queued", "in_progress", "retry", "complete", "failed", "skipped")}
        return {
            "influencers": influencers,
            "influencers_pending": sum(influencers[s] for s in CLAIMABLE),
            "audience_waiting": self.store.count_where("audience", {"wanted": True, "enriched": False}),
            "audience_done": self.store.count_where("audience", {"enriched": True}),
        }

    def progress(self, username: str) -> dict | None:
        """Where one influencer is: queued -> collecting_engagement -> looking_up_audience -> complete."""
        cp = self.store.get("checkpoints", username)
        if cp is None:
            return None
        influencer = self.store.get("influencers", username) or {}
        engagers = influencer.get("audience_ids", [])
        docs = self.store.get_many("audience", engagers)
        to_look_up = [d for d in docs.values() if d.get("wanted")]
        looked_up = sum(1 for d in to_look_up if d.get("enriched"))

        status = cp.get("status")
        if status in ("failed", "skipped"):
            state = status
        elif status != "complete":
            state = "queued" if not cp.get("done") and not cp.get("claimed_by") else "collecting_engagement"
        elif looked_up < len(to_look_up):
            state = "looking_up_audience"
        else:
            state = "complete"
        return {
            "username": username,
            "state": state,
            "stages_done": cp.get("done", []),
            "attempts": cp.get("attempts", 0),
            "last_error": cp.get("last_error"),
            "posts": len(influencer.get("post_ids", [])),
            "engagers": len(engagers),
            "audience_to_look_up": len(to_look_up),
            "audience_looked_up": looked_up,
            "percent_done": round(100 * looked_up / len(to_look_up), 1) if to_look_up else (100.0 if state == "complete" else 0.0),
        }

    def active_workers(self) -> list[str]:
        """Servers that currently hold a live account lock, i.e. have a worker running."""
        now = _iso(_now())
        return sorted({d["lock_owner"] for d in self.store.find("account_state")
                       if d.get("lock_owner") and (d.get("lock_until") or "") > now})

    def is_drained(self) -> bool:
        """Nothing left to do anywhere (including jobs other accounts are working on)."""
        return (self.store.count_where("checkpoints", {"status": CLAIMABLE}) == 0
                and self.store.count_where("audience", {"wanted": True, "enriched": False}) == 0)
