"""Watch Reels in the real Instagram app on a real phone or emulator (Appium).

Passive only: it scrolls the Reels feed and waits; it never likes, comments, follows or sends DMs.
One device per account (see `devices.example.json`), logged in by hand once so no password is typed
by automation. The device keeps its own genuine identity; nothing here spoofs or rewrites it.
"""

import json
import logging
import random
import time
from pathlib import Path

from .viewer import NotLoggedIn, ViewerError, WatchedReel, watch_durations

log = logging.getLogger(__name__)
APP_PACKAGE = "com.instagram.android"
APP_ACTIVITY = "com.instagram.mainactivity.LauncherActivity"   # use "com.instagram.lite" + its activity for Lite
DEFAULT_APPIUM_URL = "http://127.0.0.1:4723"


def load_devices(path: Path) -> dict[str, dict]:
    """{username: {"udid": "...", "appium_url": "..."(optional)}}"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ViewerError(f"Cannot read device map {path}: {exc}") from exc
    if not isinstance(data, dict) or not all(isinstance(v, dict) and v.get("udid") for v in data.values()):
        raise ViewerError(f"{path} must map each username to an object with a 'udid'")
    return data


class MobileViewer:
    def __init__(self, username: str, udid: str, appium_url: str = DEFAULT_APPIUM_URL,
                 package: str = APP_PACKAGE, activity: str = APP_ACTIVITY):
        self.package = package
        self.activity = activity
        self.username = username
        self.udid = udid
        self.appium_url = appium_url

    def _driver(self):
        from appium import webdriver
        from appium.options.android import UiAutomator2Options
        options = UiAutomator2Options()
        options.udid = self.udid
        options.app_package = self.package
        options.app_activity = self.activity
        options.no_reset = True            # keep the app logged in between runs
        options.new_command_timeout = 300
        return webdriver.Remote(self.appium_url, options=options)

    @staticmethod
    def _logged_in(driver) -> bool:
        return not driver.find_elements("xpath", "//*[contains(@text,'Log in') or contains(@text,'Log In')]")

    @staticmethod
    def _swipe_up(driver, rng) -> None:
        size = driver.get_window_size()
        x = int(size["width"] * rng.uniform(0.45, 0.55))
        driver.swipe(x, int(size["height"] * rng.uniform(0.75, 0.8)),
                     x, int(size["height"] * rng.uniform(0.2, 0.28)), int(rng.uniform(250, 450)))

    def login(self) -> None:
        """Open the app so a person can log in on the device; the app keeps the session."""
        driver = self._driver()
        try:
            input(f"Log in as {self.username} on the device ({self.udid}), then press Enter here... ")
        finally:
            driver.quit()

    def watch(self, count: int, rng=None, show: bool = False) -> list[WatchedReel]:
        """Scroll `count` Reels with uneven watch times. The app exposes no Reel link, so codes are None."""
        rng = rng or random.SystemRandom()
        driver = self._driver()
        watched: list[WatchedReel] = []
        try:
            driver.implicitly_wait(5)
            if not self._logged_in(driver):
                raise NotLoggedIn(f"{self.username}: log in on device {self.udid} first (python -m scraper view --account NAME --mobile --login)")
            tabs = driver.find_elements("xpath", "//*[contains(@content-desc,'Reels')]")
            if not tabs:
                raise ViewerError("No Reels tab found in the Instagram app; the layout may have changed")
            tabs[0].click()
            driver.implicitly_wait(0)
            for seconds in watch_durations(count, rng):
                log.info("[%s] mobile watching for %.0fs", self.username, seconds)
                time.sleep(seconds)
                watched.append(WatchedReel(None, None, "", seconds))
                self._swipe_up(driver, rng)
                time.sleep(rng.uniform(0.8, 2.0))
        finally:
            driver.quit()
        return watched
