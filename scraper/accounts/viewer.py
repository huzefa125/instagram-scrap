"""Watch Reels in a real browser (Playwright) with one persistent profile per account.

The browser profile is logged in once by hand (`python -m scraper view --account NAME --login`),
so no password is typed by automation and any challenge is solved by a person. It uses the same
proxy as the account's API session so both present one network identity.
"""

import hashlib
import logging
import random
import re
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from ..core.config import ROOT

log = logging.getLogger(__name__)
HOME_URL = "https://www.instagram.com/"
REELS_URL = "https://www.instagram.com/reels/"
REELS_LINK = "a[href='/reels/'], a[href^='/reels/'][role='link']"
CODE_IN_URL = re.compile(r"/(?:reel|reels)/([A-Za-z0-9_-]+)/?")
MIN_WATCH, MAX_WATCH = 5.0, 40.0
PLAY_VISIBLE_VIDEO = """() => {
  const mid = window.innerHeight / 2;
  const v = [...document.querySelectorAll('video')].find(el => {
    const r = el.getBoundingClientRect(); return r.height > 0 && r.top < mid && r.bottom > mid; });
  if (!v) return null;
  v.muted = true; v.play().catch(() => {});
  return v.duration;
}"""


# Android phone profiles from Playwright's built-in device list. Each account is pinned to one
# of them for good (hash of its username), like a person who keeps the same phone.
ANDROID_DEVICES = ("Pixel 5", "Pixel 7", "Galaxy S9+", "Galaxy S8", "Galaxy A55", "Galaxy S III")


def device_for(username: str, available) -> str:
    names = [n for n in ANDROID_DEVICES if n in available] or [n for n in available if "Pixel" in n or "Galaxy" in n]
    if not names:
        raise ViewerError("Playwright has no Android device profiles; update playwright")
    return names[int(hashlib.sha256(username.encode()).hexdigest(), 16) % len(names)]


def _mobile_identity(user_agent: str) -> dict:
    """Android version, model and Chrome version read from the profile's own user agent."""
    android = re.search(r"Android (\d+(?:\.\d+)*)", user_agent)
    model = re.search(r"Android [\d.]+; ([^;)]+?)(?: Build[^)]*)?\)", user_agent)
    chrome = re.search(r"Chrome/(\d+)", user_agent)
    return {"android": android.group(1) if android else "13", "model": model.group(1) if model else "",
            "chrome": chrome.group(1) if chrome else "120"}


def mobile_init_script(ram_gb: int = 4, cores: int = 8) -> str:
    """Make the JS-visible fingerprint match a phone: ARM Linux platform, touch points, phone-class
    memory/cores, no webdriver flag and a mobile GPU string instead of the desktop one."""
    return f"""(() => {{
  const def = (o, k, v) => {{ try {{ Object.defineProperty(o, k, {{ get: () => v, configurable: true }}); }} catch (e) {{}} }};
  def(Navigator.prototype, 'platform', 'Linux armv81');
  def(Navigator.prototype, 'maxTouchPoints', 5);
  def(Navigator.prototype, 'hardwareConcurrency', {cores});
  def(Navigator.prototype, 'deviceMemory', {ram_gb});
  def(Navigator.prototype, 'webdriver', false);
  def(Navigator.prototype, 'vendor', 'Google Inc.');
  const gpu = (proto) => {{
    const orig = proto.getParameter;
    proto.getParameter = function (p) {{
      if (p === 37445) return 'Qualcomm';
      if (p === 37446) return 'Adreno (TM) 650';
      return orig.call(this, p);
    }};
  }};
  if (window.WebGLRenderingContext) gpu(WebGLRenderingContext.prototype);
  if (window.WebGL2RenderingContext) gpu(WebGL2RenderingContext.prototype);
  def(screen, 'orientation', screen.orientation);
}})();"""


def apply_client_hints(context, page, user_agent: str) -> None:
    """Send Android client hints (`sec-ch-ua-mobile: ?1`, platform "Android", model) like real Chrome."""
    info = _mobile_identity(user_agent)
    try:
        session = context.new_cdp_session(page)
        session.send("Emulation.setUserAgentOverride", {
            "userAgent": user_agent,
            "platform": "Linux armv81",
            "userAgentMetadata": {
                "brands": [{"brand": "Chromium", "version": info["chrome"]},
                           {"brand": "Google Chrome", "version": info["chrome"]},
                           {"brand": "Not.A/Brand", "version": "99"}],
                "fullVersionList": [{"brand": "Chromium", "version": f"{info['chrome']}.0.0.0"},
                                    {"brand": "Google Chrome", "version": f"{info['chrome']}.0.0.0"}],
                "platform": "Android", "platformVersion": info["android"], "architecture": "",
                "model": info["model"], "mobile": True}})
    except Exception as exc:  # noqa: BLE001 - hints are best effort; the user agent is still mobile
        log.warning("could not set mobile client hints: %s", exc)


class ViewerError(Exception):
    pass


class NotLoggedIn(ViewerError):
    pass


@dataclass
class WatchedReel:
    code: str
    author: str | None
    caption: str
    watched_seconds: float

    @property
    def url(self) -> str:
        return f"https://www.instagram.com/reel/{self.code}/"


def _proxy_settings(proxy: str) -> dict | None:
    if not proxy:
        return None
    parsed = urlparse(proxy)
    settings = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        settings["username"], settings["password"] = parsed.username, parsed.password or ""
    return settings


def watch_durations(count: int, rng=None) -> list[float]:
    """Uneven watch times: most reels are watched a while, a few are skipped early."""
    rng = rng or random.SystemRandom()
    return [round(rng.uniform(MIN_WATCH, 8.0) if rng.random() < 0.25 else rng.uniform(10.0, MAX_WATCH), 1)
            for _ in range(count)]


def watch_seconds(duration: float | None, fallback: float, rng=None) -> float:
    """How long to stay on one Reel, relative to its real length: about 40% are watched to the
    end, 35% about half, 25% skipped early. Falls back to a plain random time if the length is unknown."""
    rng = rng or random.SystemRandom()
    if not duration or duration != duration or duration < 3:
        return fallback
    roll = rng.random()
    fraction = 1.0 if roll < 0.40 else rng.uniform(0.4, 0.7) if roll < 0.75 else rng.uniform(0.1, 0.3)
    return round(min(max(duration * fraction, 3.0), 90.0), 1)


class BrowserViewer:
    def __init__(self, username: str, proxy: str = "", headless: bool = True, locale: str = "en-US"):
        self.locale = locale
        self.username = username
        self.proxy = proxy
        self.headless = headless
        self.profile_dir = ROOT / "browser_profiles" / username

    def _launch(self, playwright, headless: bool):
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        # Mobile web: Android user agent, phone screen size, touch input and device pixel ratio.
        device = {k: v for k, v in playwright.devices[device_for(self.username, playwright.devices)].items()
                  if k != "default_browser_type"}   # launch_persistent_context rejects this key
        context = playwright.chromium.launch_persistent_context(
            str(self.profile_dir), headless=headless, proxy=_proxy_settings(self.proxy),
            locale=self.locale, **device)
        context.add_init_script(mobile_init_script())
        agent = device.get("user_agent", "")
        for page in context.pages:
            apply_client_hints(context, page, agent)
        context.on("page", lambda page: apply_client_hints(context, page, agent))
        return context

    def _open_reels(self, page, rng) -> None:
        """Behave like a person: land on the home feed, scroll it a little, then tap the Reels tab.
        Opens the Reels URL directly only if the tab cannot be found."""
        page.goto(HOME_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(rng.uniform(3000, 5000))
        if not self._logged_in(page):
            return
        for _ in range(rng.randint(2, 4)):
            page.mouse.wheel(0, rng.uniform(350, 800))
            page.wait_for_timeout(rng.uniform(1200, 3000))
        tab = page.locator(REELS_LINK).first
        try:
            tab.wait_for(state="visible", timeout=5000)
            tab.click()
        except Exception:  # noqa: BLE001 - layout change: fall back to the direct URL
            log.warning("[%s] Reels tab not found on the feed; opening the Reels URL directly", self.username)
            page.goto(REELS_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(rng.uniform(2500, 4500))

    def login(self) -> None:
        """Open a visible browser so a person can log in; the profile keeps the session."""
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            context = self._launch(pw, headless=False)
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(HOME_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            if self._logged_in(page):
                print(f"{self.username} is already logged in; nothing to do.")
                context.close()
                return
            input(f"Log in as {self.username} in the browser, then press Enter here to save and close... ")
            context.close()

    def watch(self, count: int, rng=None, show: bool = False) -> list[WatchedReel]:
        """Play `count` Reels from the account's Reels feed and return what was watched."""
        from playwright.sync_api import sync_playwright
        rng = rng or random.SystemRandom()
        watched: list[WatchedReel] = []
        with sync_playwright() as pw:
            context = self._launch(pw, headless=self.headless and not show)
            try:
                page = context.pages[0] if context.pages else context.new_page()
                self._open_reels(page, rng)
                if not self._logged_in(page):
                    raise NotLoggedIn(f"{self.username}: log in first with `view --account {self.username} --login`")
                seen: set[str] = set()
                for fallback in watch_durations(count, rng):
                    page.wait_for_selector("video", state="attached", timeout=20000)
                    try:   # play the video that is on screen (muted) and read its real length
                        length = page.evaluate(PLAY_VISIBLE_VIDEO)
                    except Exception:  # noqa: BLE001 - unknown length falls back to a random time
                        length = None
                    seconds = watch_seconds(length, fallback, rng)
                    match = CODE_IN_URL.search(page.url)
                    code = match.group(1) if match else None
                    if code and code not in seen:
                        seen.add(code)
                        watched.append(WatchedReel(code, self._author(page), self._caption(page), seconds))
                        log.info("[%s] watching %s for %.0fs", self.username, code, seconds)
                    page.wait_for_timeout(seconds * 1000)
                    self._next_reel(page, rng)
                    page.wait_for_timeout(rng.uniform(800, 2000))
            finally:
                context.close()
        if not watched:
            raise ViewerError("No Reel codes were found in the feed; the page layout may have changed")
        return watched

    @staticmethod
    def _next_reel(page, rng) -> None:
        """Scroll one screen like a swipe up; if the Reel did not change, try the keyboard."""
        before = page.url
        height = page.evaluate("window.innerHeight") or 800
        page.mouse.move(rng.uniform(150, 300), height * rng.uniform(0.5, 0.7))
        page.mouse.wheel(0, height * rng.uniform(0.9, 1.05))
        page.wait_for_timeout(1200)
        if page.url == before:
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(1200)

    @staticmethod
    def _logged_in(page) -> bool:
        # The public Reels page opens without a login, so the URL alone proves nothing: require the
        # session cookie that only a logged-in account has.
        if "/accounts/login" in page.url or page.locator("input[name='username']").count():
            return False
        return any(c["name"] == "sessionid" and c.get("value")
                   for c in page.context.cookies("https://www.instagram.com"))

    @staticmethod
    def _author(page) -> str | None:
        """Best effort: the first profile link on the playing Reel."""
        try:
            href = page.locator("main a[href^='/'][role='link']").first.get_attribute("href", timeout=1500)
            name = (href or "").strip("/").split("/")[0]
            return name if re.fullmatch(r"[A-Za-z0-9._]+", name or "") else None
        except Exception:  # noqa: BLE001 - layout changes must not stop watching
            return None

    @staticmethod
    def _caption(page) -> str:
        try:
            return (page.locator("meta[property='og:description']").get_attribute("content", timeout=1000) or "")[:500]
        except Exception:  # noqa: BLE001
            return ""
