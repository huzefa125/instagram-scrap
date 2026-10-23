"""The work itself, split the way the accounts are split.

InfluencerWorker (engagement accounts), per influencer, resumable via checkpoints:
  profile     Influencer Profile
  posts       Recent Posts (POSTS_PER_INFLUENCER)
  engagement  Post Engagement -> Likers (LIKERS_PER_POST) + Commenters -> Unique Profile Deduplication

AudienceWorker (audience accounts), per unique engager:
  Public Profile Data + Recent Profile Posts (AUDIENCE_RECENT_POSTS)
"""

import logging
import random
from datetime import datetime, timezone
from typing import Callable

from instagrapi.exceptions import ClientError, MediaNotFound, MediaUnavailable, NotFoundError, PrivateAccount

from ..jobs.checkpoint import Checkpoints
from .collectors import Collector
from ..core.config import Config

log = logging.getLogger(__name__)

USER_FIELDS = ("username", "full_name", "is_private", "is_verified", "profile_pic_url")
POST_SUMMARY_FIELDS = (
    "id", "code", "taken_at", "media_type", "product_type",
    "like_count", "comment_count", "play_count", "caption_text",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summary(post: dict) -> dict:
    return {k: post.get(k) for k in POST_SUMMARY_FIELDS}


class InfluencerWorker:
    def __init__(self, config: Config, store, collector: Collector):
        self.config = config
        self.store = store
        self.collector = collector
        self.checkpoints = Checkpoints(store)

    def process(self, username: str, heartbeat: Callable[[], None] = lambda: None) -> None:
        """Run the remaining stages for one influencer. Raises StopRun / ClientError to the caller.
        `heartbeat` is called after each step so the queue lease stays alive on long jobs."""
        self._heartbeat = heartbeat
        cp = self.checkpoints
        if not cp.is_done(username, "profile"):
            self.stage_profile(username)
            cp.mark_done(username, "profile")
        influencer = self.store.get("influencers", username)
        if influencer.get("is_private"):
            cp.mark_skipped(username, "private account")
            return
        for stage in ("posts", "engagement"):
            if not cp.is_done(username, stage):
                getattr(self, f"stage_{stage}")(username, influencer)
                cp.mark_done(username, stage)
                heartbeat()
                influencer = self.store.get("influencers", username)

    def stage_profile(self, username: str) -> None:
        profile = self.collector.influencer_profile(username)
        profile["user_id"] = str(profile.pop("pk"))
        profile["collected_at"] = _now()
        self.store.upsert("influencers", username, profile)

    def stage_posts(self, username: str, influencer: dict) -> None:
        posts = self.collector.recent_posts(influencer["user_id"], self.config.posts_per_influencer)
        existing = self.store.get_many("posts", [p["id"] for p in posts])
        for post in posts:
            if post["id"] not in existing:
                self.store.upsert("posts", post["id"], {
                    **_summary(post),
                    "influencer": username,
                    "engagement_done": False,
                    "collected_at": _now(),
                })
        self.store.upsert("influencers", username, {"post_ids": [p["id"] for p in posts]})
        log.info("@%s: %d posts", username, len(posts))

    def stage_engagement(self, username: str, influencer: dict) -> None:
        audience_ids = set(influencer.get("audience_ids", []))
        posts = self.store.get_many("posts", influencer.get("post_ids", []))
        for post_id in influencer.get("post_ids", []):
            post = posts.get(post_id) or {}
            if post.get("engagement_done"):
                continue
            try:
                likers = self.collector.likers(post_id, self.config.likers_per_post)
                comments = self.collector.commenters(post_id, self.config.comments_per_post)
            except (MediaNotFound, MediaUnavailable) as exc:
                self.store.upsert("posts", post_id, {"engagement_done": True, "error": type(exc).__name__})
                continue

            for user in likers:
                audience_ids.add(self._record_audience(user, username, "like"))
            for comment in comments:
                audience_ids.add(self._record_audience(comment["user"], username, "comment"))

            self.store.upsert("posts", post_id, {
                "liker_ids": [str(u["pk"]) for u in likers],
                "comments": [
                    {"user_id": str(c["user"]["pk"]), "text": c["text"], "created_at": c["created_at"]}
                    for c in comments
                ],
                "engagement_done": True,
            })
            # Save progress after every post so a stop loses at most one post.
            self.store.upsert("influencers", username, {"audience_ids": sorted(audience_ids)})
            self._heartbeat()
            log.info("@%s post %s: %d likers, %d comments", username, post.get("code"), len(likers), len(comments))
        self.store.upsert("influencers", username, {"audience_ids": sorted(audience_ids)})
        self._mark_sample(username, sorted(audience_ids))
        log.info("@%s: %d unique engagers", username, len(audience_ids))

    def _mark_sample(self, username: str, audience_ids: list[str]) -> None:
        """Sample mode (AUDIENCE_PROFILES_PER_INFLUENCER = N > 0): queue N engagers for lookup.
        Same random sample every time for this influencer. In full mode everyone is already queued."""
        cap = self.config.audience_profiles_per_influencer
        if not cap:
            return
        ids = list(audience_ids)
        random.Random(username).shuffle(ids)
        self.store.update_many("audience", ids[:cap], {"wanted": True})

    def _record_audience(self, user: dict, influencer: str, kind: str) -> str:
        """Unique Profile Deduplication: one audience doc per user id, shared across influencers."""
        user_id = str(user["pk"])
        # Atomic: several accounts can meet the same fan at once, and an audience account may
        # already have enriched them; a plain get-then-write would reset `enriched`.
        self.store.insert_if_missing("audience", user_id, {
            **{k: user.get(k) for k in USER_FIELDS},
            "enriched": False,
            # Queued for an audience lookup right away in full mode; sample mode picks later.
            "wanted": not self.config.audience_profiles_per_influencer,
            "lease_until": None,
            "first_seen": _now(),
        })
        self.store.add_to_set("audience", user_id, "engaged_with", influencer)
        self.store.add_to_set("audience", user_id, "liked_on" if kind == "like" else "commented_on", influencer)
        return user_id


class AudienceWorker:
    def __init__(self, config: Config, store, collector: Collector):
        self.config = config
        self.store = store
        self.collector = collector

    def enrich(self, user_id: str) -> None:
        """Public profile + recent posts for one engager. Raises StopRun to the caller."""
        try:
            profile = self.collector.public_profile(user_id)
        except (NotFoundError, PrivateAccount) as exc:
            self.store.upsert("audience", user_id, {"enriched": True, "error": type(exc).__name__, "lease_until": None})
            return
        profile.pop("pk", None)
        fields = {**profile, "enriched": True, "enriched_at": _now(), "lease_until": None, "claimed_by": None}

        # AUDIENCE_RECENT_POSTS=0 skips this request (instagrapi treats amount=0 as "all posts").
        if self.config.audience_recent_posts > 0 and not profile.get("is_private") and profile.get("media_count"):
            try:
                posts = self.collector.recent_posts(user_id, self.config.audience_recent_posts)
                fields["recent_posts"] = [_summary(p) for p in posts]
            except ClientError as exc:
                fields["recent_posts_error"] = type(exc).__name__
        self.store.upsert("audience", user_id, fields)

