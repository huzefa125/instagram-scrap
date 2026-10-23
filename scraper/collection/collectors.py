"""Python Workers: the actual Instagram calls, all routed through RateGuard."""

from instagrapi import Client

from ..accounts.rate_limit import RateGuard


def _dump(model) -> dict:
    return model.model_dump(mode="json")


class Collector:
    def __init__(self, client: Client, guard: RateGuard):
        self.client = client
        self.guard = guard

    def influencer_profile(self, username: str) -> dict:
        user = self.guard.call(self.client.user_info_by_username, username)
        return _dump(user)

    def recent_posts(self, user_id: str, amount: int) -> list[dict]:
        medias = self.guard.call(self.client.user_medias, user_id, amount)
        return [_dump(m) for m in medias]

    def likers(self, media_id: str, limit: int) -> list[dict]:
        # One request returns the liker list; keep the first `limit`.
        users = self.guard.call(self.client.media_likers, media_id)
        return [_dump(u) for u in users[:limit]]

    def commenters(self, media_id: str, amount: int) -> list[dict]:
        comments = self.guard.call(self.client.media_comments, media_id, amount)
        return [
            {"user": _dump(c.user), "comment_id": str(c.pk), "text": c.text, "created_at": str(c.created_at_utc)}
            for c in comments
        ]

    def public_profile(self, user_id: str) -> dict:
        return _dump(self.guard.call(self.client.user_info, user_id))
