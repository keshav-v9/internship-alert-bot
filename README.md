# Internship Alert Bot

Polls company career pages, job boards and GitHub internship lists, and sends you a Discord DM (and optionally an email) the moment a new posting appears. It remembers what it has already seen in `state.json`, so you only get new listings. The first time a source is seen it is snapshotted silently.

## Quick start

```bash
git clone <this-repo> && cd <this-repo>
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium      # only needed for browser-based sources
cp .env.example .env                       # then fill it in (see key below)
python bot.py
```

To run it 24/7, use a process manager (systemd on Linux, launchd on macOS) or any always-on host. The bot only makes outgoing connections, so no ports need opening.

## Key: `.env`

| Variable | Required | Meaning |
|---|---|---|
| `DISCORD_TOKEN` | yes | Bot token. Create an application at discord.com/developers, add a Bot, copy its token. |
| `DISCORD_USER_ID` | yes | Numeric ID of who gets the DMs. Enable Developer Mode in Discord, right-click the user, Copy User ID. The user must share a server with the bot. |
| `GITHUB_TOKEN` | no | Raises the GitHub API limit from 60 to 5000 requests/hour. |
| `EMAIL_TO` | no | Address to email alerts to. |
| `SMTP_USER` | no | Account used to send the email. |
| `SMTP_PASSWORD` | no | Its password (Gmail needs an App Password). Email is sent only if all three email variables are set. |
| `SMTP_HOST` / `SMTP_PORT` | no | Defaults `smtp.gmail.com` / `465` (SSL). |

Invite the bot to a server you are in with:
`https://discord.com/oauth2/authorize?client_id=<APPLICATION_ID>&scope=bot&permissions=0`

## Key: `config.yaml`

Global settings:

| Key | Meaning |
|---|---|
| `poll_interval_seconds` | How often every source is checked. |
| `off_peak_poll_interval_seconds` | Optional slower interval outside peak hours. |
| `peak_start_hour` / `peak_end_hour` | Peak window in the machine's local time (end hour is exclusive). |
| `state_path` | Where seen listings are stored. |

Each entry under `sources:` has a `type`, a `name` (shown in alerts), a `url`, an optional `board_url` (makes the alert title a link) and optional `enabled: false` to skip it.

| `type` | Reads | Extra fields |
|---|---|---|
| `greenhouse`, `lever`, `ashby` | A company's public job-board API | |
| `workday` | A Workday career site | `search_text`, `max_results` |
| `github_repo` | A GitHub repo's README table | `owner`, `repo` |
| `linkedin_jobs`, `google_jobs`, `bloomberg_jobs`, `meta_jobs`, `snowflake_jobs`, `uber_jobs`, `tesla_jobs`, `amazon_jobs` | Site-specific scrapers | `search_text` (Uber, Workday), `max_results` (Amazon) |
| `wp_job_manager` | WordPress "WP Job Manager" boards | |
| `webpage` | Any server-rendered page; alerts when its text changes | `selector` (CSS, optional) |

To watch a new company, copy an existing entry of the right type, change `name`, `url` and `board_url`, and restart the bot.

## Files

- `bot.py`: Discord (and email) delivery.
- `alerter.py`: polling and diffing, with no Discord awareness.
- `config.yaml`: which sources to watch.
- `.env`: your secrets (git-ignored). `state.json`: seen listings (git-ignored).

## Notes

- Some sites (LinkedIn, Tesla, certain career pages) rate-limit or block automated requests, especially from cloud IPs; those sources may log errors.
- Respect each site's terms of service and keep the poll interval reasonable.
