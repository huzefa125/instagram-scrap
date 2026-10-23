"""Stream a Reel's real video over the account's own proxy, at playback speed.

This is what the app's player does on the network: it fetches the video in ranged chunks as
playback moves forward. Nothing is decoded or shown, and Instagram's playback events are not
sent, so this is streaming, not proof that a view was counted. `seconds` is the real time spent
streaming; `fraction` is how much of the video was fetched.
"""

import logging
import random
import re
import time

import requests

from .viewer import watch_seconds

log = logging.getLogger(__name__)
CHUNK_SECONDS = 3.0          # video seconds fetched per request
TIMEOUT = 20


def _total_bytes(session, url: str, proxies) -> int | None:
    r = session.get(url, headers={"Range": "bytes=0-0"}, proxies=proxies, timeout=TIMEOUT, stream=True)
    try:
        match = re.search(r"/(\d+)$", r.headers.get("Content-Range", ""))
        if match:
            return int(match.group(1))
        length = r.headers.get("Content-Length")
        return int(length) if r.status_code == 200 and length else None
    finally:
        r.close()


def stream_reel(video_url: str, duration: float | None, proxy: str = "", rng=None,
                sleep=time.sleep, session=None) -> dict:
    """Fetch the start of a Reel for a human-like share of its length (about 40% to the end,
    35% about half, 25% skipped early). Returns {"seconds", "fraction", "bytes"}."""
    rng = rng or random.SystemRandom()
    session = session or requests.Session()
    proxies = {"http": proxy, "https": proxy} if proxy else None
    if not video_url or not duration or duration <= 0:
        raise ValueError("Reel has no video url or duration")
    total = _total_bytes(session, video_url, proxies)
    if not total:
        raise ValueError("Video size unknown")
    target = min(watch_seconds(duration, duration * 0.5, rng), duration)
    per_second = total / duration
    position = fetched = 0
    started = time.monotonic()
    while position < target:
        step = min(CHUNK_SECONDS, target - position)
        end = min(total - 1, int((position + step) * per_second))
        start = int(position * per_second)
        r = session.get(video_url, headers={"Range": f"bytes={start}-{end}"}, proxies=proxies,
                        timeout=TIMEOUT)
        r.raise_for_status()
        fetched += len(r.content)
        position += step
        sleep(step * rng.uniform(0.95, 1.1))     # the player only fetches ahead as time passes
    return {"seconds": round(time.monotonic() - started, 1) if sleep is time.sleep else round(position, 1),
            "fraction": round(position / duration, 2), "bytes": fetched}
