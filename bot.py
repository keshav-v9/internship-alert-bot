"""Discord bot: DMs you when Alerter reports new content on a watched source."""

import asyncio
import faulthandler
import logging
import os
import signal
import smtplib
from email.message import EmailMessage

import certifi

# Diagnostics for "the bot appears to hang" — no root needed, just
# `kill -USR1 <pid>` or `kill -USR2 <pid>`:
#   SIGUSR1 -> faulthandler dumps each OS thread's raw stack. Useful for
#              confirming the event loop itself is alive (sitting in
#              selectors.select() is NORMAL/idle, not stuck), but it does
#              NOT show where an individual suspended coroutine is frozen —
#              asyncio tasks don't show up as separate OS threads.
#   SIGUSR2 -> _dump_async_tasks (registered in on_ready, once the event
#              loop exists) walks asyncio.all_tasks() and logs each one's
#              exact suspended coroutine + line number — this is the one
#              that actually answers "what is on_ready/the alerter
#              currently awaiting", which a raw thread dump can't show.
faulthandler.register(signal.SIGUSR1)

# python.org's macOS Python doesn't use the system certificate trust store,
# which breaks discord.py's own SSL connections. Point OpenSSL at certifi's
# bundle before discord.py (or anything else) opens a connection.
os.environ.setdefault("SSL_CERT_FILE", certifi.where())

import discord
import yaml
from dotenv import load_dotenv

from alerter import Alerter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("bot")

load_dotenv()

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
DISCORD_USER_ID = int(os.environ["DISCORD_USER_ID"])
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Optional email alerts, sent in addition to the Discord DM. Enabled only
# when EMAIL_TO, SMTP_USER and SMTP_PASSWORD are all set in .env.
EMAIL_TO = os.environ.get("EMAIL_TO")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
EMAIL_ENABLED = bool(EMAIL_TO and SMTP_USER and SMTP_PASSWORD)

with open("config.yaml") as f:
    config = yaml.safe_load(f)

# source name -> its board_url from config.yaml, so an alert's embed title
# can link straight to the actual job board it came from. This is purely a
# presentation lookup, kept here in bot.py rather than threaded through
# alerter.py's on_update callback — alerter.py has no business knowing
# "board_url" is a concept, it's a Discord-display detail, not a
# polling/diffing one. A source with no board_url configured just gets a
# non-clickable title (see format_alerts).
SOURCE_BOARD_URLS = {s["name"]: s.get("board_url") for s in config["sources"]}

# Discord embed descriptions cap at 4096 chars; leave headroom for the
# "- " bullet prefixes and a safety margin.
MAX_EMBED_CHARS = 3800

intents = discord.Intents.default()
client = discord.Client(intents=intents)

# Holds the running Alerter.run() task. Two things this guards against:
#
# 1. asyncio.create_task()'s result MUST be kept referenced somewhere, or
#    the task can be silently garbage-collected mid-execution at any time —
#    this is explicitly called out in the asyncio docs as a common footgun,
#    and it's exactly what was happening here: the very first version of
#    this code did `asyncio.create_task(alerter.run())` without saving the
#    result, and the polling loop would intermittently vanish anywhere from
#    immediately to a few cycles in, with no error logged (a GC'd task just
#    stops, silently) — sometimes surviving a full poll, sometimes not,
#    which is exactly the nondeterministic "works, then doesn't" pattern
#    that made this bug so confusing to track down.
# 2. discord.py fires on_ready() again on every reconnect (confirmed in
#    testing: a "session has been invalidated" gateway event mid-run
#    re-triggered it), not just once at startup. Without a guard, each
#    reconnect would spin up a whole second concurrent Alerter instance —
#    a second polling loop, hitting every source's API a second time, both
#    writing to the same state.json out from under each other.
_alerter_task: asyncio.Task | None = None


def _on_alerter_task_done(task: asyncio.Task) -> None:
    """Attached to _alerter_task via add_done_callback so a crash inside
    alerter.run() is never silent.

    alerter.run() is an infinite loop — it should never finish on its own,
    so this firing at all means something killed it. Without this, since
    _alerter_task now holds a permanent reference (that's the whole point
    of the fix above), the task's exception would just sit there
    unretrieved forever: no crash, no traceback, no log line — the bot
    would simply stop polling and give no indication why. task.exception()
    re-raises inside this callback if the task was cancelled, which is
    also useful signal (it shouldn't be, under normal operation).
    """
    if task.cancelled():
        logger.error("alerter task was cancelled — polling has stopped")
        return
    exc = task.exception()
    if exc is not None:
        logger.error("alerter task died with an unhandled exception — polling has stopped", exc_info=exc)
    else:
        logger.error("alerter task returned normally, which should never happen — polling has stopped")


def _chunk_lines(lines: list[str], max_chars: int = MAX_EMBED_CHARS) -> list[list[str]]:
    """Split a list of alert lines into groups that will each fit inside a
    single Discord embed description.

    This exists so that a source with a big batch of new postings (say, 120
    new roles found in one poll) still gets ALL of them delivered — split
    across several DMs — rather than being silently truncated to whatever
    fits in one message. Discord hard-caps an embed description at 4096
    characters; MAX_EMBED_CHARS (3800) leaves headroom below that cap for
    the per-line "- " bullet prefix and newline this function doesn't add
    itself (format_alerts does, after chunking) plus a small safety margin.

    Greedy packing: lines are added to the current chunk until the next one
    would push it over max_chars, at which point that chunk is sealed off
    and a new one starts. A single line longer than max_chars on its own
    would still end up alone in its own (oversized) chunk rather than being
    split mid-line or dropped — that's an accepted edge case since no
    current source ever produces a line anywhere near that long.

    Returns a list of chunks, where each chunk is itself a list of the
    original line strings (unmodified — no bullet/newline formatting
    applied here).
    """
    chunks: list[list[str]] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        entry_len = len(line) + 3  # "- " prefix + newline
        if current and current_len + entry_len > max_chars:
            chunks.append(current)
            current = []
            current_len = 0
        current.append(line)
        current_len += entry_len
    if current:
        chunks.append(current)
    return chunks


def format_alerts(source_name: str, new_lines: list[str]) -> list[discord.Embed]:
    """Turn one Alerter `on_update` callback's worth of new lines into one
    or more ready-to-send Discord embeds.

    Almost always this is a single embed. It becomes multiple only when
    `_chunk_lines` had to split the batch to stay under Discord's per-embed
    character limit — in that case each embed's title is suffixed with
    "[i/total]" so they read as an obviously-related sequence of DMs rather
    than looking like separate unrelated alerts.

    The title always states how many new items were found in total (not
    just how many are in that particular embed/chunk), so a quick glance at
    any one of the DMs tells you the overall scale of the update.

    The title is also a hyperlink straight to the source's job board, via
    Embed's `url` param (that's the actual mechanism Discord uses to make
    an embed title clickable — there's no separate "link" field). Every
    embed in a multi-part alert gets the same link, not just the first.
    """
    chunks = _chunk_lines(new_lines)
    total = len(chunks)
    board_url = SOURCE_BOARD_URLS.get(source_name)
    embeds = []
    for i, chunk in enumerate(chunks, start=1):
        title = f"Update: {source_name} ({len(new_lines)} new)"
        if total > 1:
            title += f" [{i}/{total}]"
        body = "\n".join(f"- {line}" for line in chunk)
        embed = discord.Embed(title=title, description=body, color=discord.Color.green())
        if board_url:
            embed.url = board_url
        embeds.append(embed)
    return embeds


async def send_alert(user: discord.User, source_name: str, new_lines: list[str]) -> None:
    """Format and DM the user every embed for one source's update.

    This is the function passed to Alerter as its `on_update` callback
    (wrapped in a closure in on_ready, below) — it's what actually bridges
    alerter.py's Discord-agnostic "here's what's new" callback into a real
    Discord action.

    Each embed is sent in its own try/except: if a particular DM fails to
    send (e.g. a transient Discord API hiccup), it's logged and the loop
    moves on to the next embed rather than aborting the whole batch — so a
    single failed message in a multi-part alert doesn't take the rest of
    that alert down with it.
    """
    for embed in format_alerts(source_name, new_lines):
        try:
            await user.send(embed=embed)
        except discord.HTTPException:
            logger.exception("failed to DM alert for %r", source_name)


def _send_email_sync(source_name: str, new_lines: list[str]) -> None:
    msg = EmailMessage()
    msg["Subject"] = f"Update: {source_name} ({len(new_lines)} new)"
    msg["From"] = SMTP_USER
    msg["To"] = EMAIL_TO
    body = "\n".join(f"- {line}" for line in new_lines)
    board_url = SOURCE_BOARD_URLS.get(source_name)
    if board_url:
        body += f"\n\n{board_url}"
    msg.set_content(body)
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)


async def send_email(source_name: str, new_lines: list[str]) -> None:
    """Email one source's update. Runs the blocking SMTP call in a thread
    and swallows/logs failures so email problems never break Discord alerts."""
    if not EMAIL_ENABLED:
        return
    try:
        await asyncio.to_thread(_send_email_sync, source_name, new_lines)
    except Exception:
        logger.exception("failed to email alert for %r", source_name)


def _dump_async_tasks() -> None:
    """SIGUSR2 handler (registered below, once the event loop exists).
    Logs every currently-alive asyncio task and exactly which line its
    coroutine is suspended at — unlike faulthandler's OS-thread dump, this
    can actually show "on_ready is stuck awaiting client.fetch_user() at
    bot.py:225" instead of just "the event loop is in selectors.select()",
    which is what every suspended coroutine looks like from the outside.
    """
    tasks = asyncio.all_tasks()
    logger.error("=== SIGUSR2: dumping %d asyncio task(s) ===", len(tasks))
    for task in tasks:
        logger.error("%r", task)
        for frame in task.get_stack(limit=8):
            logger.error("    %s:%d in %s", frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name)


@client.event
async def on_ready():
    """Fires every time discord.py (re)establishes a ready gateway
    connection — that includes the initial startup, but ALSO every
    reconnect (a dropped connection, an invalidated session, etc.), which
    is easy to miss since discord.py doesn't call this out loudly. This is
    where the actual polling gets wired up and started — nothing before
    this point does any source-checking.

    Steps:
      1. If an alerter task from a previous on_ready() call is already
         running, do nothing — see the _alerter_task docstring above for
         why this guard exists (short version: without it, every reconnect
         would spin up a second concurrent polling loop).
      2. Resolve DISCORD_USER_ID (just an integer from .env) into an actual
         discord.User object we can call .send() on — fetch_user() does an
         API round-trip to look this user up.
      3. Define `on_update` as a closure over `user`, so Alerter can call it
         with just (source_name, new_lines) without needing to know
         anything about Discord users/sessions itself.
      4. Construct the Alerter with the sources/interval/state path read
         from config.yaml at import time (see module-level `config` above),
         and hand it that `on_update` closure.
      5. Schedule `alerter.run()` as a background asyncio task via
         `asyncio.create_task` — NOT awaited directly — so it runs
         concurrently with (rather than blocking) discord.py's own event
         loop, which needs to keep processing gateway events (heartbeats,
         reconnects, etc.) the whole time this bot is up. The task is
         assigned to the module-level `_alerter_task` (not a local
         variable) — see that variable's docstring for why keeping a real
         reference to it matters.
    """
    global _alerter_task
    logger.info("logged in as %s", client.user)
    asyncio.get_running_loop().add_signal_handler(signal.SIGUSR2, _dump_async_tasks)

    if _alerter_task is not None and not _alerter_task.done():
        logger.info("alerter already running, skipping duplicate start (reconnect)")
        return

    logger.info("fetching target user %s...", DISCORD_USER_ID)
    user = await client.fetch_user(DISCORD_USER_ID)
    logger.info("resolved target user: %s", user)

    async def on_update(source_name: str, new_lines: list[str]) -> None:
        await send_alert(user, source_name, new_lines)
        await send_email(source_name, new_lines)

    async def on_notice(message: str) -> None:
        await user.send(embed=discord.Embed(title="Welcome back", description=message, color=discord.Color.blue()))

    alerter = Alerter(
        sources=config["sources"],
        on_update=on_update,
        poll_interval_seconds=config.get("poll_interval_seconds", 600),
        off_peak_poll_interval_seconds=config.get("off_peak_poll_interval_seconds"),
        peak_start_hour=config.get("peak_start_hour", 6),
        peak_end_hour=config.get("peak_end_hour", 17),
        state_path=config.get("state_path", "state.json"),
        github_token=GITHUB_TOKEN,
        max_concurrency=config.get("max_concurrency", 8),
        on_notice=on_notice,
    )
    _alerter_task = asyncio.create_task(alerter.run())
    _alerter_task.add_done_callback(_on_alerter_task_done)


if __name__ == "__main__":
    # client.run() blocks forever: it opens the Discord gateway connection,
    # runs discord.py's whole internal event loop, and only returns if the
    # bot is stopped (Ctrl+C, crash, or explicit logout). Everything else
    # in this file (on_ready, the polling loop it starts) happens as a
    # consequence of this one call.
    client.run(DISCORD_TOKEN)
