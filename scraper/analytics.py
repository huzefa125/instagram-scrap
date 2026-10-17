"""Audience analytics computed from collected data.

Rates over profile fields (business, categories, follower size, activity) use the
enriched sample, since only those users have full profiles. `sample_size` says how many.
"""

from collections import Counter
from datetime import datetime, timedelta, timezone

FOLLOWER_BUCKETS = [
    ("under_1k", 0, 1_000),
    ("1k_10k", 1_000, 10_000),
    ("10k_100k", 10_000, 100_000),
    ("100k_plus", 100_000, None),
]
ACTIVE_DAYS = 30


def _pct(part: int, whole: int) -> float | None:
    return round(100 * part / whole, 1) if whole else None


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _looks_suspicious(profile: dict) -> bool:
    """Heuristic for low-quality / bot-like accounts: no posts, or follows far more than followed."""
    followers = profile.get("follower_count") or 0
    following = profile.get("following_count") or 0
    no_posts = (profile.get("media_count") or 0) == 0
    follow_heavy = following >= 1000 and following > 5 * max(followers, 1)
    return no_posts or follow_heavy


def influencer_analytics(store, username: str) -> dict | None:
    influencer = store.get("influencers", username)
    if influencer is None:
        return None
    audience = list(store.get_many("audience", influencer.get("audience_ids", [])).values())
    enriched = [a for a in audience if a.get("enriched") and not a.get("error")]

    # Posts and engagement
    posts = list(store.get_many("posts", influencer.get("post_ids", [])).values())
    likes = [p.get("like_count") or 0 for p in posts]
    comments = [p.get("comment_count") or 0 for p in posts]
    followers = influencer.get("follower_count") or 0
    avg_likes = sum(likes) / len(likes) if likes else None
    avg_comments = sum(comments) / len(comments) if comments else None
    engagement_rate = (
        round(100 * (avg_likes + avg_comments) / followers, 2)
        if followers and avg_likes is not None else None
    )

    # How many posts each audience member engaged with
    touches = Counter()
    likers, commenters = set(), set()
    for p in posts:
        engaged_here = set(p.get("liker_ids", [])) | {c["user_id"] for c in p.get("comments", [])}
        touches.update(engaged_here)
        likers.update(p.get("liker_ids", []))
        commenters.update(c["user_id"] for c in p.get("comments", []))
    repeat = sum(1 for n in touches.values() if n >= 2)

    # Profile mix (enriched sample)
    n = len(enriched)
    buckets = {name: 0 for name, _, _ in FOLLOWER_BUCKETS}
    for a in enriched:
        count = a.get("follower_count") or 0
        for name, low, high in FOLLOWER_BUCKETS:
            if count >= low and (high is None or count < high):
                buckets[name] += 1
                break
    cutoff = datetime.now(timezone.utc) - timedelta(days=ACTIVE_DAYS)
    active = sum(
        1 for a in enriched
        if any((t := _parse_time(p.get("taken_at"))) and t >= cutoff for p in a.get("recent_posts", []))
    )
    categories = Counter(
        a.get("category_name") or a.get("business_category_name") or a.get("category")
        for a in enriched if a.get("category_name") or a.get("business_category_name") or a.get("category")
    )
    cities = Counter(a["city_name"] for a in enriched if a.get("city_name"))

    return {
        "username": username,
        "full_name": influencer.get("full_name"),
        "follower_count": followers,
        "following_count": influencer.get("following_count"),
        "media_count": influencer.get("media_count"),
        "is_verified": influencer.get("is_verified"),
        "category": influencer.get("category_name") or influencer.get("category"),
        "posts_analyzed": len(posts),
        "avg_likes": round(avg_likes, 1) if avg_likes is not None else None,
        "avg_comments": round(avg_comments, 1) if avg_comments is not None else None,
        "engagement_rate_pct": engagement_rate,
        "audience": {
            "unique_engagers": len(audience),
            "likers": len(likers),
            "commenters": len(commenters),
            "repeat_engagers": repeat,
            "repeat_engagers_pct": _pct(repeat, len(audience)),
            "private_pct": _pct(sum(1 for a in audience if a.get("is_private")), len(audience)),
            "verified_pct": _pct(sum(1 for a in audience if a.get("is_verified")), len(audience)),
            "sample_size": n,
            "business_pct": _pct(sum(1 for a in enriched if a.get("is_business")), n),
            "active_last_30d_pct": _pct(active, n),
            "suspicious_pct": _pct(sum(1 for a in enriched if _looks_suspicious(a)), n),
            "follower_buckets": {k: {"count": v, "pct": _pct(v, n)} for k, v in buckets.items()},
            "top_categories": [{"name": k, "count": v, "pct": _pct(v, n)} for k, v in categories.most_common(10)],
            "top_cities": [{"name": k, "count": v, "pct": _pct(v, n)} for k, v in cities.most_common(10)],
        },
    }


def audience_overlap(store, username: str, top: int = 20) -> dict | None:
    """Influencers whose engaged audience overlaps with this one's."""
    influencer = store.get("influencers", username)
    if influencer is None:
        return None
    mine = set(influencer.get("audience_ids", []))
    rows = []
    for other in store.find("influencers"):
        if other["_id"] == username:
            continue
        theirs = set(other.get("audience_ids", []))
        shared = len(mine & theirs)
        if shared:
            rows.append({
                "username": other["_id"],
                "shared": shared,
                "pct_of_mine": _pct(shared, len(mine)),
                "pct_of_theirs": _pct(shared, len(theirs)),
                "jaccard": round(shared / len(mine | theirs), 4),
            })
    rows.sort(key=lambda r: r["shared"], reverse=True)
    return {"username": username, "audience_size": len(mine), "overlaps": rows[:top]}
