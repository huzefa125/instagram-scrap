# Daily Instagram activity

The optional activity worker schedules **five activities per account per local day**, regardless
of whether the account is used for engagement or audience collection. The example mix is one
profile lookup, two Reel metadata checks, one Reel like, and one approved comment. Times and
target choices are random within 09:00–21:00, with at least 30 minutes between activities.
These are activities, not Instagram request counts: a like or comment first checks the Reel and
therefore uses at least two guarded calls. All calls share the account's existing request budget.

To configure it:

1. Copy `activity.example.json` to `activity.json` (the latter is ignored by Git).
2. Fill `profiles` with usernames, `reels` and `likes` with specific HTTPS Reel URLs, and
   `comments_by_account` with an entry for **each** configured account username and exact comment
   text approved for its target Reel. Provide enough unused like/comment Reel URLs for each day;
   the worker does not automatically repeat a successful like or comment on the same Reel.
3. Preview with `python -m scraper activity --account YOUR_USERNAME --file activity.json`.
   A preview makes no Instagram calls and does not reserve targets.
4. Set `ACTIVITY_ENABLED=on` and `ACTIVITY_FILE=activity.json` in `.env`, then run
   `python -m scraper worker`. Activity is off by default. If the file is missing or invalid,
   the worker logs an error and disables activity while collection continues.

For each account/day, the worker persists a plan in `activity_plans`; successful likes/comments
are recorded in `activity_used`. After an interruption during a write, the action becomes
`uncertain` instead of being automatically repeated. Inspect that action manually before
deciding whether to retry it. Request blocking and freezes are handled by the existing guard.

`reel_check` fetches Reel metadata; it **does not play the video or prove a view**. There is no
video-playback implementation in this worker. If real watching is required, it needs a distinct
approved integration and verification strategy. The daily window uses `IG_TIMEZONE_OFFSET`.
The worker has one thread per account and configured account numbers can exceed 200; performance
at that scale still requires load testing and multi-server operational hardening.

## Explore/Reels feed mode (human-approved comments)

Two extra activity kinds use the account's own Explore Reels feed, so no target URLs are needed
(see `activity.feed.example.json`):

- `feed_watch` fetches a few Explore Reels, marks them as seen (`clip_seen`, the same call the app
  sends when you scroll past) and saves them as `pending` candidates. This is not video playback.
- `feed_comment` posts one **approved** candidate comment. With none approved it is `skipped`.

Review candidates yourself; nothing is commented until you approve exact text:

    python -m scraper candidates --account NAME
    python -m scraper candidates --account NAME --approve CODE --text "Love the transitions!"
    python -m scraper candidates --account NAME --reject CODE

Comments are 1-150 characters with no links or @mentions. A comment interrupted mid-post is marked
`uncertain` (`--status uncertain`) and never retried automatically. Start with a single account.

### Browser playback (optional)

Set `ACTIVITY_BROWSER=on` to make `feed_watch` actually play Reels in Chromium (Playwright) instead
of only sending the API "seen" signal. Setup, per account:

    pip install playwright && playwright install chromium
    python -m scraper view --account NAME --login     # log in by hand once; profile saved
    python -m scraper view --account NAME --count 3   # visible test run, saves candidates

Profiles live in `browser_profiles/NAME/` (git-ignored) and use the account's proxy from
`accounts.json`. Watch times vary from 5 to 40 seconds. Reel codes are read from the page URL, and
the author and caption are best effort, so page changes by Instagram can break this. If the profile
is logged out the action fails with a clear message. Browser views do not use the API request budget.

### Phone playback (Appium, optional)

Set `ACTIVITY_MOBILE=on` to make `feed_watch` play Reels in the real Instagram app on a phone, driven
by Appium, instead of only sending the API "seen" signal. Setup, per account:

    pip install Appium-Python-Client        # plus an Appium 2 server with the UiAutomator2 driver
    cp devices.example.json devices.json    # map each account username to its phone's udid
    python -m scraper view --account NAME --mobile --login     # log in by hand on the phone once
    python -m scraper view --account NAME --mobile --count 3   # test run

Use one phone per account, on the account's own proxy or network. Watch times vary from 5 to 40
seconds. The app shows no Reel link, so phone viewing saves no review candidates and sees no authors.
An account with no phone in `devices.json` gets no feed watching. If the app is logged out, the action
fails with a clear message and is not retried. Phone views do not use the API request budget.
