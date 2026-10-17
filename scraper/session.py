"""Session Manager: log in once per account, save the session, reuse the same device every run."""

import logging
from pathlib import Path

from instagrapi import Client
from instagrapi.exceptions import TwoFactorRequired

from .config import AccountConfig, Config

log = logging.getLogger(__name__)

IP_CHECK_URL = "https://api.ipify.org/"


def session_file(config: Config, username: str) -> Path:
    return config.session_dir / f"{username or 'default'}.json"


class SessionManager:
    def __init__(self, config: Config, account: AccountConfig):
        self.config = config
        self.account = account
        self.settings_path = session_file(config, account.username)
        self.client = self._new_client()

    def _new_client(self) -> Client:
        client = Client()
        if self.account.proxy:
            client.set_proxy(self.account.proxy)
        # Request pacing is handled by RateGuard; keep instagrapi's own delay off.
        client.delay_range = None
        return client

    def login(self, interactive: bool = True) -> Client:
        if not self.account.username or not self.account.password:
            raise SystemExit("Account needs a username and password (accounts.json or .env).")

        self.client = self._new_client()
        if self.settings_path.exists():
            # Saved device, locale and cookies; login() checks the session and only
            # logs in again (same device) if Instagram rejects it.
            self.client.load_settings(self.settings_path)
        else:
            # First login only: match the account's normal region. These are stored in
            # the session file and never changed afterwards.
            self.client.set_country(self.config.ig_country)
            self.client.set_country_code(self.config.ig_country_code)
            self.client.set_locale(self.config.ig_locale)
            self.client.set_timezone_offset(self.config.ig_timezone_offset)

        try:
            self.client.login(self.account.username, self.account.password)
        except TwoFactorRequired:
            if not interactive:
                raise
            code = input(f"Two-factor code for {self.account.username}: ").strip()
            self.client.login(self.account.username, self.account.password, verification_code=code)
        log.info("Logged in as %s", self.account.username)
        self._save()
        return self.client

    def relogin(self) -> Client:
        """Log in again with the same saved device, then confirm with a cheap call."""
        self.client.relogin()
        self.client.account_info()
        self._save()
        return self.client

    def check_proxy(self) -> tuple[str, str | None]:
        """Return (IP without proxy, IP through proxy) so you can confirm the proxy is used."""
        probe = Client()
        direct = probe._send_public_request(IP_CHECK_URL).strip()
        if not self.account.proxy:
            return direct, None
        probe.set_proxy(self.account.proxy)
        return direct, probe._send_public_request(IP_CHECK_URL).strip()

    def _save(self) -> None:
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.dump_settings(self.settings_path)
