"""Source-watching logic. No Discord awareness — just polls sources and
calls on_update() when something new shows up."""

import asyncio
import base64
import datetime
import json
import logging
import os
import re
import math
import ssl
import time
from pathlib import Path
from typing import Awaitable, Callable

from urllib.parse import urljoin

import aiohttp
import certifi
from bs4 import BeautifulSoup
from playwright.async_api import Browser, Page, async_playwright

# python.org's macOS Python doesn't use the system certificate trust store,
# which causes SSL verification failures. Pin to certifi's bundle explicitly.
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

logger = logging.getLogger("alerter")

OnUpdate = Callable[[str, list[str]], Awaitable[None]]

# Source types that need a real browser rather than a plain HTTP request —
# see each one's _fetch_*_lines docstring for the specific reason (a
# CSRF-protected API, a client-side-only search box, etc.). Checked once
# per poll cycle so a single shared Chromium instance can be launched only
# when actually needed, and reused across all of them rather than starting
# a fresh browser process per source (expensive) or leaving one running
# permanently between polls (wasteful — see poll_once).
_BROWSER_SOURCE_TYPES = {"meta_jobs", "snowflake_jobs", "uber_jobs", "tesla_jobs"}

_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _format_role(company: str, title: str, url: str) -> str:
    """Build the single display line used for every job posting, from every
    source type, so alerts look the same regardless of which ATS they came
    from. Format is always "**Company** — Role — [Apply](url)".

    Args:
        company: the hiring company's name, e.g. "Stripe". Every caller is
            expected to always supply a real value here (falling back to
            "Unknown Company" if one genuinely can't be determined) — the
            company name being visible on every single line, not just
            implied by the embed title, is a deliberate requirement: on a
            source like SimplifyJobs that aggregates postings from
            hundreds of different companies, "which embed title was this
            bullet under again?" is exactly the kind of thing that's easy
            to lose track of scrolling through Discord.
        title: job title, e.g. "Software Engineer Intern". May be "".
        url: link to the posting (job board or direct apply link). May be "".

    Returns:
        A Discord-markdown-formatted string like
        "**Stripe** — Software Engineer Intern — [Apply](https://...)".
        Location is deliberately never included here — every posting
        reaching this function has already passed _is_us_location, so
        there's nothing left to communicate beyond "it's US-based", which
        the filtering itself guarantees without needing to say so per line.
        This exact string is also what gets stored in state.json and
        compared next poll — so two polls that produce the identical string
        for the same posting are treated as "no change", and a never-seen
        string is treated as "new".
    """
    line = f"**{company or 'Unknown Company'}**"
    if title:
        line += f" — {title}"
    if url:
        line += f" — [Apply]({url})"
    return line


# Word-boundary match so "Internal Product Manager" and "International Sales"
# don't false-positive on the "intern" substring — this bit us for real: an
# early version naively used `"intern" in title.lower()` and it matched
# Adobe's "Internal Product Manager 5". Co-op is included (with or without a
# hyphen/space) since the user considers co-ops internships for their
# purposes; "s" endings are optional so "Interns"/"Internships"/"Co-ops" all
# match too.
_INTERN_TITLE_RE = re.compile(
    r"\b(intern(ship)?s?|co[\s-]?ops?)\b", re.IGNORECASE
)


def _is_internship_title(title: str) -> bool:
    """True if `title` looks like an internship/co-op posting rather than a
    full-time, part-time, or new-grad role. See _INTERN_TITLE_RE for the
    exact matching rule and why it's word-boundary based."""
    return bool(_INTERN_TITLE_RE.search(title or ""))


# Substrings (checked against a lowercased location string) that indicate a
# posting is NOT US-based. This is deliberately a blocklist, not an
# allowlist: real-world location strings from these job boards are wildly
# inconsistent — some spell out ", USA" or ", California, US", but plenty of
# genuinely US postings are just a bare city name ("San Francisco", "New
# York - 300 Vesey Street") with no state/country at all. An allowlist that
# required an explicit US marker would incorrectly exclude all of those. A
# blocklist instead assumes US by default and only excludes when a location
# clearly points elsewhere — matching real country names, and a set of
# major non-US tech-hub cities that (like US cities) often appear bare
# without a country suffix.
#
# This is a best-effort heuristic, not a geocoding lookup. The country list
# is the standard set of ~195 short English country names (everything
# except the US itself) — deliberately exhaustive rather than "just the
# common tech-hiring countries", because a first pass with only ~50
# common-sounding countries missed Visa postings in Azerbaijan and Morocco
# (a payments company genuinely has offices close to everywhere). "Georgia"
# is deliberately NOT in this list even though it's a real country: it's
# also a US state (Atlanta is a major tech hub), and there's no way to
# disambiguate "Atlanta, Georgia" from "Tbilisi, Georgia" by name alone —
# excluding real Atlanta postings is a worse failure than occasionally
# including a Georgia-the-country one. The city list below the country list
# is a much shorter, best-effort supplement for major non-US hubs that
# often appear bare, without any country name attached at all
# (e.g. Greenhouse just returning "London") — extend it if you spot one
# slipping through.
_NON_US_COUNTRIES = frozenset(
    {
        "afghanistan", "albania", "algeria", "andorra", "angola",
        "antigua and barbuda", "argentina", "armenia", "australia", "austria",
        "azerbaijan", "bahamas", "bahrain", "bangladesh", "barbados",
        "belarus", "belgium", "belize", "benin", "bhutan", "bolivia",
        "bosnia and herzegovina", "botswana", "brazil", "brunei", "bulgaria",
        "burkina faso", "burundi", "cabo verde", "cambodia", "cameroon",
        "canada", "central african republic", "chad", "chile", "china",
        "colombia", "comoros", "costa rica", "croatia", "cuba", "cyprus",
        "czechia", "czech republic", "democratic republic of the congo",
        "denmark", "djibouti", "dominica", "dominican republic", "ecuador",
        "egypt", "el salvador", "equatorial guinea", "eritrea", "estonia",
        "eswatini", "ethiopia", "fiji", "finland", "france", "gabon",
        "gambia", "germany", "ghana", "greece", "grenada",
        "guatemala", "guinea", "guinea-bissau", "guyana", "haiti",
        "honduras", "hong kong", "hungary", "iceland", "india", "indonesia",
        "iran", "iraq", "ireland", "israel", "italy", "ivory coast",
        "jamaica", "japan", "jordan", "kazakhstan", "kenya", "kiribati",
        "kosovo", "kuwait", "kyrgyzstan", "laos", "latvia", "lebanon",
        "lesotho", "liberia", "libya", "liechtenstein", "lithuania",
        "luxembourg", "macau", "madagascar", "malawi", "malaysia",
        "maldives", "mali", "malta", "marshall islands", "mauritania",
        "mauritius", "mexico", "micronesia", "moldova", "monaco", "mongolia",
        "montenegro", "morocco", "mozambique", "myanmar", "namibia",
        "nauru", "nepal", "netherlands", "new zealand", "nicaragua",
        "niger", "nigeria", "north korea", "north macedonia", "norway",
        "oman", "pakistan", "palau", "palestine", "panama",
        "papua new guinea", "paraguay", "peru", "philippines", "poland",
        "portugal", "puerto rico", "qatar", "republic of the congo",
        "romania", "russia", "rwanda", "saint kitts and nevis",
        "saint lucia", "saint vincent and the grenadines", "samoa",
        "san marino", "sao tome and principe", "saudi arabia", "senegal",
        "serbia", "seychelles", "sierra leone", "singapore", "slovakia",
        "slovenia", "solomon islands", "somalia", "south africa",
        "south korea", "south sudan", "spain", "sri lanka", "sudan",
        "suriname", "sweden", "switzerland", "syria", "taiwan", "tajikistan",
        "tanzania", "thailand", "timor-leste", "togo", "tonga",
        "trinidad and tobago", "tunisia", "turkey", "turkmenistan", "tuvalu",
        "uganda", "ukraine", "united arab emirates", "united kingdom",
        "uruguay", "uzbekistan", "vanuatu", "vatican", "venezuela",
        "vietnam", "yemen", "zambia", "zimbabwe",
        # common short/alt forms and region codes that show up verbatim.
        # Deliberately NOT including "uk" alone here — as a bare substring
        # it false-positives on plenty of US city names (e.g. "Milwaukee"),
        # and "united kingdom" above already covers the real cases.
        "uae", "emea", "apac", "korea",
    }
)

_NON_US_CITIES = frozenset(
    {
        "london", "toronto", "vancouver", "montreal", "ontario",
        "british columbia", "dublin", "bangalore", "bengaluru", "hyderabad",
        "mumbai", "delhi", "pune", "tokyo", "beijing", "shanghai",
        "shenzhen", "krakow", "warsaw", "gdansk", "hanoi", "ho chi minh",
        "mexico city", "paris", "amsterdam", "berlin", "munich", "sydney",
        "melbourne", "zurich", "geneva", "tel aviv", "sao paulo",
        "seoul", "milan", "madrid", "barcelona", "stockholm", "prague",
        "baku", "casablanca",
    }
)

_NON_US_LOCATION_MARKERS = _NON_US_COUNTRIES | _NON_US_CITIES

# Word-boundary matching, not plain substring — a naive `marker in lowered`
# check was a real bug: "uk" matched inside "Milwaukee", "mali" matched
# inside "Malibu", "chad" matched inside "Chadron, NE", each wrongly
# excluding a genuinely-US posting. \b works fine for the multi-word
# entries too (e.g. "united kingdom", "ho chi minh") since \b anchors on
# the whole phrase's start/end, not each word individually.
_JOBRIGHT_URL_RE = re.compile(r"https://jobright\.ai/jobs/info/[0-9a-f]+")
_PUBLISH_TIME_RE = re.compile(r'"publishTime":"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"')

_NON_US_LOCATION_RE = re.compile(
    r"\b(" + "|".join(re.escape(m) for m in _NON_US_LOCATION_MARKERS) + r")\b",
    re.IGNORECASE,
)


def _is_us_location(location: str) -> bool:
    """True unless `location` clearly names a non-US country/city (see
    _NON_US_LOCATION_MARKERS). An empty/missing location also returns True
    — with no information either way, defaulting to exclude would drop a
    lot of genuinely-US postings that just don't bother stating a country
    (see the blocklist-vs-allowlist reasoning above)."""
    if not location:
        return True
    return not _NON_US_LOCATION_RE.search(location)


def _extract_greenhouse(data: dict, company: str) -> list[str]:
    """Turn a Greenhouse `GET /v1/boards/{token}/jobs` JSON response into
    formatted lines, one per open posting.

    Expected shape (per https://developers.greenhouse.io/job-board.html):
        {"jobs": [{"title": ..., "location": {"name": ...}, "absolute_url": ...}, ...]}

    Any posting missing a field just gets an empty string for it (via
    dict.get(..., "")) rather than raising — a malformed/partial entry
    should degrade gracefully, not take down the whole poll.

    Only US-based internship/co-op postings make it into the returned
    lines — see _is_internship_title and _is_us_location. Everything else
    (full-time roles, non-US offices) is filtered out here, before the line
    ever reaches state.json or a diff, so it never triggers an alert.

    Greenhouse's job objects don't reliably include a company name field of
    their own (each board is already scoped to one company), so `company`
    is passed in by the caller — it's just the source's configured `name`
    from config.yaml (e.g. "Stripe").
    """
    lines = []
    for job in data.get("jobs", []):
        title = job.get("title", "")
        if not _is_internship_title(title):
            continue
        loc = (job.get("location") or {}).get("name", "")
        if not _is_us_location(loc):
            continue
        lines.append(_format_role(company, title, job.get("absolute_url", "")))
    return lines


def _extract_lever(data: list, company: str) -> list[str]:
    """Turn a Lever `GET /v0/postings/{token}?mode=json` JSON response into
    formatted lines, one per open posting.

    Lever's response is a bare top-level JSON array (not wrapped in an
    object), and — unlike every other ATS handled here — the job title
    field is called "text", not "title". Location lives under
    categories.location.

    Filtered to US internships/co-ops only — see _extract_greenhouse's
    docstring for why. `company` is the source's configured name, same
    reasoning as _extract_greenhouse.
    """
    lines = []
    for job in data:
        title = job.get("text", "")
        if not _is_internship_title(title):
            continue
        loc = (job.get("categories") or {}).get("location", "")
        if not _is_us_location(loc):
            continue
        lines.append(_format_role(company, title, job.get("hostedUrl", "")))
    return lines


def _extract_ashby(data: dict, company: str) -> list[str]:
    """Turn an Ashby `GET /posting-api/job-board/{token}` JSON response into
    formatted lines, one per open posting.

    Expected shape: {"jobs": [{"title": ..., "location": ..., "jobUrl": ...}, ...]}
    Ashby's "location" field is already a plain display string (unlike
    Greenhouse's nested {"name": ...}), so no unwrapping is needed here.

    Filtered to US internships/co-ops only — see _extract_greenhouse's
    docstring for why. `company` is the source's configured name, same
    reasoning as _extract_greenhouse.
    """
    lines = []
    for job in data.get("jobs", []):
        title = job.get("title", "")
        loc = job.get("location", "")
        if not _is_internship_title(title) or not _is_us_location(loc):
            continue
        lines.append(_format_role(company, title, job.get("jobUrl", "")))
    return lines


def _extract_workday(data: dict, base_url: str, company: str) -> list[str]:
    """Turn a Workday `POST /wday/cxs/{tenant}/{site}/jobs` JSON response
    into formatted lines, one per posting matching the search.

    Expected shape:
        {"jobPostings": [{"title": ..., "locationsText": ..., "externalPath": ...}, ...]}

    Workday only gives each posting a *relative* path (e.g.
    "/job/San-Jose/Some-Role_R12345"), not a full URL — the caller derives
    `base_url` from the polled API URL and passes it in here so we can
    build a clickable absolute apply link. `company` is the source's
    configured name (Workday job objects don't include one either).

    Filtered to US internships/co-ops only — this matters especially here,
    since Workday's own `searchText: "intern"` query param does plain
    substring matching server-side and happily returns "Internal Product
    Manager" alongside real internships; this filter is what actually
    excludes those.
    """
    lines = []
    for job in data.get("jobPostings", []):
        title = job.get("title", "")
        if not _is_internship_title(title):
            continue
        loc = job.get("locationsText", "")
        if not _is_us_location(loc):
            continue
        path = job.get("externalPath", "")
        lines.append(_format_role(company, title, f"{base_url}{path}"))
    return lines


def _extract_amazon_jobs(data: dict, company: str) -> list[str]:
    """Turn a page of amazon.jobs's `search.json` response into formatted
    lines, one per posting.

    This is Amazon's own undocumented internal search API (not a
    third-party ATS), reverse-engineered by inspecting real responses.
    Expected shape: {"jobs": [{"title": ..., "normalized_location": ...,
    "job_path": ...}, ...]}. `job_path` is relative (e.g.
    "/en/jobs/12345/some-role"), so it's prefixed with the fixed
    amazon.jobs origin to make a clickable link. `company` is the source's
    configured name ("Amazon" or "AWS" — both hit the same underlying API,
    just with different query filters).

    Filtered to US internships/co-ops only, same as every other ATS
    extractor — redundant with the source's own `normalized_country_code[]
    =USA` query param and `base_query=intern` (already narrowing results
    server-side), but kept here too for consistency/safety in case that
    query param ever gets dropped from config.
    """
    lines = []
    for job in data.get("jobs", []):
        title = job.get("title", "")
        if not _is_internship_title(title):
            continue
        loc = job.get("normalized_location", "")
        if not _is_us_location(loc):
            continue
        path = job.get("job_path", "")
        lines.append(_format_role(company, title, f"https://www.amazon.jobs{path}"))
    return lines


_GOOGLE_DS1_RE = re.compile(
    r"AF_initDataCallback\(\{key:\s*'ds:1'.*?data:(\[.*?\]), sideChannel", re.DOTALL
)


def _extract_google_jobs(html: str) -> tuple[list[str], int, int]:
    """Parse one page of Google's careers search results into formatted
    lines, one per posting.

    Google's careers site (careers.google.com / google.com/about/careers)
    is a client-rendered Angular app with no conventional job-listing API —
    but like several other Google products, the page is still
    server-rendered with its data pre-loaded into the HTML via
    `AF_initDataCallback({key: 'ds:1', ..., data: [...]})`, a hydration
    payload the Angular app reads on load rather than fetching separately.
    That means the real job data is sitting in the plain HTML response
    already, just serialized as nested unlabeled arrays (positional, not
    named JSON keys) instead of a normal API shape — found by loading the
    real page in a browser and inspecting both its network traffic (no
    separate XHR fires — confirming there's no simpler API to hit instead)
    and its page source (confirming `ds:1` is present in the raw HTML
    before any JS runs, not injected afterward), then reverse-engineering
    the field layout by comparing known values (title, location) against
    their positions.

    Relevant fields per job entry (0-indexed, everything else ignored):
        [0]  job ID, e.g. "91436104816698054"
        [1]  title
        [9]  locations: list of [display_name, address_lines, city, zip,
             state, country_code, ...] — country_code is a clean ISO code
             ("US", "CA", "PL", ...), which is more reliable than the
             text-heuristic _is_us_location has to use for every other
             source (those don't give a real structured country field).

    The apply link is built from the job ID rather than using the
    "signin?jobId=..." URL embedded in the data (index [2]) — that one
    leads into a sign-in flow, whereas
    "https://www.google.com/about/careers/applications/jobs/results/{id}"
    (confirmed working, no session needed) is the plain public job detail
    page, matching the "Apply" link style used for every other source. The
    SEO title slug Google normally appends after the ID in that URL
    (confirmed) is cosmetic — the ID alone resolves to the same page.

    Args:
        html: the raw HTML of one
            google.com/about/careers/applications/jobs/results page.

    Returns:
        (formatted lines already filtered to US internships/co-ops, total
        job count reported by this response, raw job count on this page
        before filtering). `total` is the running-total target the caller
        (_fetch_google_jobs_lines) pages toward; `raw_count` is how many
        actual entries this specific page contained — needed separately
        from `len(lines)` because that's already been shrunk by filtering,
        which would make pagination look like it's not making progress
        and could loop forever if the stop condition were judged by it
        instead.
    """
    match = _GOOGLE_DS1_RE.search(html)
    if not match:
        return [], 0, 0
    data = json.loads(match.group(1))
    jobs = data[0] or []
    total = data[2] if len(data) > 2 and isinstance(data[2], int) else len(jobs)

    lines = []
    for job in jobs:
        job_id = job[0]
        title = job[1]
        if not _is_internship_title(title):
            continue
        locations = job[9] or []
        country_codes = {loc[5] for loc in locations if len(loc) > 5}
        if "US" not in country_codes:
            continue
        url = f"https://www.google.com/about/careers/applications/jobs/results/{job_id}"
        lines.append(_format_role("Google", title, url))
    return lines, total, len(jobs)


def _extract_linkedin_jobs(html: str, company: str) -> list[str]:
    """Turn one page of LinkedIn's public (no-login) job search results into
    formatted lines, one per posting.

    LinkedIn's job search marketplace (linkedin.com/jobs) is normally
    behind a login wall for anything beyond the first few results, but its
    "guest" search view — the same one search engines can crawl — is
    plain server-rendered HTML with no login required, found by loading it
    in a browser and confirming no XHR fires for the job cards (they're
    already in the initial GET response, confirmed again via plain curl
    with no session/cookies at all).

    `f_C={company_id}` in the source URL scopes results to postings from
    one specific company (LinkedIn's own numeric company ID — 1337 for
    LinkedIn itself) rather than the whole marketplace; without it this
    would return internships at every company that posts to LinkedIn,
    which isn't what a per-company source in this bot means anywhere else.

    Card structure (class names confirmed stable across a fresh fetch):
        div.base-search-card
          h3.base-search-card__title          -> title
          span.job-search-card__location       -> location (free text)
          a.base-card__full-link[href]         -> apply link
    """
    soup = BeautifulSoup(html, "html.parser")
    lines = []
    for card in soup.select("div.base-search-card"):
        title_el = card.select_one(".base-search-card__title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not _is_internship_title(title):
            continue
        loc_el = card.select_one(".job-search-card__location")
        location = loc_el.get_text(strip=True) if loc_el else ""
        if not _is_us_location(location):
            continue
        link_el = card.select_one("a.base-card__full-link")
        url = link_el["href"].split("?")[0] if link_el and link_el.get("href") else ""
        lines.append(_format_role(company, title, url))
    return lines


def _extract_bloomberg_jobs(html: str, company: str) -> list[str]:
    """Turn one page of Bloomberg's Avature-hosted job search into
    formatted lines, one per posting.

    Like LinkedIn, this is plain server-rendered HTML (confirmed via
    curl with the exact query params a browser search uses) — found by
    loading the real search page in a browser, seeing no separate XHR
    fires for the results, and confirming the same job titles are present
    in a bare curl fetch of that URL.

    Card structure:
        article.article--result
          h3.article__header__text__title a[href]  -> title + apply link
          span.list-item-location                   -> location (free text)
    """
    soup = BeautifulSoup(html, "html.parser")
    lines = []
    for article in soup.select("article.article--result"):
        title_el = article.select_one("h3 a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not _is_internship_title(title):
            continue
        loc_el = article.select_one(".list-item-location")
        location = loc_el.get_text(strip=True) if loc_el else ""
        if not _is_us_location(location):
            continue
        url = title_el.get("href", "")
        lines.append(_format_role(company, title, url))
    return lines


def _extract_meta_jobs(graphql_data: dict, company: str) -> list[str]:
    """Turn Meta's `job_search_with_featured_jobs_v2` GraphQL response into
    formatted lines, one per posting.

    Meta's careers site (metacareers.com) requires a real browser: its
    GraphQL endpoint rejects a bare, session-less POST outright with a
    CSRF/session error page (confirmed by testing — a plain aiohttp POST
    with the same query params gets Facebook's generic "sorry, something
    went wrong" error page, not JSON), so the containing fetch method
    (_fetch_meta_jobs_lines) loads the real page with Playwright and
    intercepts this response as the browser's own JS makes the call with a
    valid session token attached — no request-forging needed once a real
    browser is doing the asking.

    Expected shape:
        {"data": {"job_search_with_featured_jobs_v2":
            {"all_jobs": [{"id": ..., "title": ..., "locations": [...]}]}}}
    `locations` is a flat list of "City, ST"/"City, Country" strings (no
    structured country code, unlike Google) — a job counts as US-based if
    ANY of its listed locations pass _is_us_location, matching how every
    other multi-location source in this file is treated.

    The apply link is built from the job ID as
    "https://www.metacareers.com/profile/job_details/{id}" — found by
    inspecting the real rendered page's job card links (not obvious from
    the GraphQL response itself, which only includes a different,
    sign-in-flow URL).
    """
    jobs = graphql_data.get("data", {}).get("job_search_with_featured_jobs_v2", {}).get("all_jobs", [])
    lines = []
    for job in jobs:
        title = job.get("title", "")
        if not _is_internship_title(title):
            continue
        locations = job.get("locations") or []
        if not any(_is_us_location(loc) for loc in locations):
            continue
        url = f"https://www.metacareers.com/profile/job_details/{job.get('id', '')}"
        lines.append(_format_role(company, title, url))
    return lines


def _extract_snowflake_jobs(raw_jobs: list[dict], company: str) -> list[str]:
    """Turn Snowflake's rendered job-card data into formatted lines.

    Snowflake's careers site (Phenom People platform) makes 25+ small AJAX
    calls per search — mostly UI label/config data — with the actual
    results seemingly not exposed as one clean fetchable response; after
    checking the most likely candidates without finding it, the practical
    path is to just load the real page with Playwright and read the
    rendered result, same principle as Meta but simpler here since no
    specific network response needs to be intercepted — the fetch method
    (_fetch_snowflake_jobs_lines) pulls `raw_jobs` via a `page.evaluate()`
    that reads each job link's own data attributes directly:
        data-ph-at-job-title-text, data-ph-at-job-location-text, href
    which conveniently means no HTML-diffing/reparsing is needed here —
    Snowflake's own markup already labels every field explicitly.
    """
    lines = []
    for job in raw_jobs:
        title = job.get("title", "")
        if not _is_internship_title(title):
            continue
        location = job.get("location", "")
        if not _is_us_location(location):
            continue
        lines.append(_format_role(company, title, job.get("url", "")))
    return lines


def _extract_uber_jobs(raw_jobs: list[dict], company: str) -> list[str]:
    """Turn Uber's rendered job-card data (post-search) into formatted
    lines.

    Uber's careers site (jobs.uber.com) has real job data server-rendered
    in the HTML, but its `?query=` URL parameter is silently ignored —
    confirmed by testing: loading the URL with any query string, or none
    at all, returns the identical default set of ~10-20 jobs, and there's
    no working `page=`/offset pagination via plain HTTP either (higher
    pages return empty regardless of query). The keyword search is
    genuinely client-side only: the fetch method
    (_fetch_uber_jobs_lines) drives it with Playwright — dismissing the
    cookie-consent overlay, typing into the actual search input, and
    pressing Enter — then reads whatever the search actually renders,
    since that's the only way to get real filtered results out of this
    site at all.

    `raw_jobs` comes from a `page.evaluate()` reading each
    `div[data-slot="card"]`: title text, the location "pill" text, and the
    real `href` of the card's own `a.js-view-job` link (resolved to an
    absolute URL by the evaluate script itself via the DOM's own href
    property, not reconstructed from the job ID — that's a real link
    already present on the page, not a guessed URL pattern).
    """
    lines = []
    for job in raw_jobs:
        title = job.get("title", "")
        if not _is_internship_title(title):
            continue
        location = job.get("location", "")
        if not _is_us_location(location):
            continue
        lines.append(_format_role(company, title, job.get("url", "")))
    return lines


def _extract_wp_job_manager(html_fragment: str) -> list[str]:
    """Turn one page of WP Job Manager's `job_manager_get_listings` AJAX
    response (a WordPress plugin many small/mid-size org career boards run
    on) into formatted lines, one per listing.

    The AJAX response wraps a raw HTML fragment (a `<ul>` of `<li
    class="job-listing">` blocks) in its "html" JSON field rather than
    returning structured JSON directly, so this still needs an HTML parse
    — but it's the plugin's own already-clean markup, not a full page, so
    there's no nav/footer/cookie-banner noise to strip.

    Each listing's <h3> contains the title as a link followed by the
    company name as trailing text (no wrapping tag), e.g.
    `<h3><a href="...">Some Role</a>Some Company</h3>` — splitting
    get_text(separator="|") on the first "|" cleanly separates the two
    without needing to inspect DOM node types directly.

    Filtered to internship/co-op titles only — this board mixes full-time
    roles in with internships (e.g. "Senior Project Engineer", "Machine
    Learning Engineer" alongside "AI Innovation Intern"). No location
    filter is applied here (unlike the ATS extractors above): every listing
    on a typical single-organization board is for an on-site role at that
    organization's own location, and the listing view doesn't expose a
    separate location field to filter on anyway.
    """
    soup = BeautifulSoup(html_fragment, "html.parser")
    lines = []
    for li in soup.find_all("li", class_="job-listing"):
        h3 = li.find("h3")
        if not h3:
            continue
        a = h3.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        if not _is_internship_title(title):
            continue
        full_text = h3.get_text(separator="|", strip=True)
        company = full_text.split("|", 1)[1] if "|" in full_text else ""
        lines.append(_format_role(company, title, a["href"]))
    return lines


_APPLICATION_COLUMN_NAMES = {"application", "apply", "link"}
_ROLE_COLUMN_NAMES = {"role", "title", "position"}
_LOCATION_COLUMN_NAMES = {"location", "locations"}
_COMPANY_COLUMN_NAMES = {"company"}

# SimplifyJobs-style tables use one of these in the Company cell to mean
# "same company as the row above" instead of repeating the name (usually
# paired with a logo/link for the real company on the first row only). If a
# company cell is empty or is just one of these, the row's real company
# name has to be carried forward from the most recent row that had one —
# see _extract_table_rows.
_CONTINUATION_MARKERS = {"↳", "└", "->", "→"}


# Apple's search page ships its results as escaped JSON inside the HTML; each
# posting object has these fields in this order.
_APPLE_JOB_RE = re.compile(
    r'"locations":(\[[^\]]*\]),"positionId":"(\d+)","postingDate":"[^"]*",'
    r'"postingTitle":"((?:[^"\\]|\\.)*)".*?"transformedPostingTitle":"([^"]*)"',
    re.S,
)


def _extract_apple_jobs(html: str, company: str) -> tuple[list[str], int]:
    """Parse one page of jobs.apple.com search results into formatted lines,
    keeping only US internship/co-op postings. Returns (lines, postings seen on
    the page) — the count lets the caller stop paginating on an empty page."""
    text = html.replace('\\"', '"')
    lines: list[str] = []
    total = 0
    for locations, position_id, raw_title, slug in _APPLE_JOB_RE.findall(text):
        total += 1
        try:
            title = json.loads(f'"{raw_title}"')
            locs = json.loads(locations)
        except ValueError:
            continue
        if not _is_internship_title(title):
            continue
        if not any(
            str(loc.get("countryID", "")).endswith("USA")
            or str(loc.get("countryName", "")).startswith("United States")
            for loc in locs
        ):
            continue
        url = f"https://jobs.apple.com/en-us/details/{position_id}/{slug}"
        lines.append(_format_role(company, title, url))
    return lines, total


def _extract_radancy_jobs(html: str, company: str, base_url: str) -> tuple[list[str], int]:
    """Parse a Radancy/TalentBrew-style results list (e.g. capitalonecareers.com):
    `#search-results-list` holds `<a href="/job/...">` rows with an `<h2>` title
    and a `.job-location` span. Keeps only US internship/co-op postings and
    returns (lines, postings seen on the page)."""
    soup = BeautifulSoup(html, "html.parser")
    lines: list[str] = []
    anchors = soup.select('#search-results-list a[href*="/job/"]')
    for a in anchors:
        title_el = a.select_one("h2, h3")
        title = (title_el or a).get_text(" ", strip=True)
        loc_el = a.select_one(".job-location")
        location = loc_el.get_text(" ", strip=True) if loc_el else ""
        if not _is_internship_title(title) or not _is_us_location(location):
            continue
        lines.append(_format_role(company, title, urljoin(base_url, a["href"])))
    return lines, len(anchors)


_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")


def _extract_markdown_table_rows(content: str) -> list[str] | None:
    """Parse a GitHub-flavored Markdown pipe table (as used by Jobright's
    internship lists) into its data rows, keeping only US internship/co-op
    postings. Rows are returned as the raw table lines, unchanged, so state
    already stored for a source stays comparable.

    The title and location columns are found from the header row (a cell named
    "Job Title"/"Role"/"Position" and one named "Location"). Returns None when
    no such table exists so the caller can fall back to plain lines."""
    title_idx = loc_idx = None
    rows: list[str] = []
    for line in content.splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        lowered = [c.lower() for c in cells]
        if title_idx is None:
            for i, c in enumerate(lowered):
                if c in ("job title", "role", "position", "title"):
                    title_idx = i
                elif c == "location":
                    loc_idx = i
            continue  # the header line itself (or a line before it)
        if set("".join(cells)) <= set("-: "):
            continue  # the |---|---| separator row
        if title_idx >= len(cells):
            continue
        title = _MD_LINK_RE.sub(r"\1", cells[title_idx]).replace("*", "").strip()
        location = cells[loc_idx] if loc_idx is not None and loc_idx < len(cells) else ""
        if _is_internship_title(title) and _is_us_location(location):
            rows.append(line)
    return rows if title_idx is not None else None


def _extract_table_rows(html_content: str) -> list[str] | None:
    """Parse HTML <table> markup embedded in a GitHub repo README (common in
    SimplifyJobs-style internship lists, which use a raw <table><tr><td>
    block rather than a pipe-delimited markdown table) into one clean line
    per job row.

    Why this exists: the naive approach — just diffing the README text
    line-by-line — was flagging things as "new" that weren't real postings:
    the README also contains summary text like "Browse 258 Internship Roles
    by Category" whose count changes every time *any* row is added, and an
    "Age" column ("0d", "1d", ...) that changes for *every* row *every day*
    regardless of whether the posting itself changed. Both would make the
    bot spam near-constant false alerts if treated as ordinary lines.

    This function fixes that by actually understanding the table structure:
      1. Find each <table> and read its <th> header row to learn column
         names and positions (works for any column order, not just
         SimplifyJobs's specific layout).
      2. Identify the Application, Role, Location, and Company columns by
         name. Only these four are ever used — unlike an earlier version,
         this does NOT join "every column that isn't obviously volatile"
         into the output; that was how junk like a lone "↳" or a stray Age
         value used to leak into alerts. Every other column (Age, category
         icons, whatever else a repo's table happens to have) is ignored
         entirely.
      3. Pull the real apply link out of the Application cell specifically
         (not just "the first <a> in the row" — the Company cell usually
         has its own link too, to the company's simplify.jobs profile,
         which we don't want here). Some rows have two links (a direct
         company link and a simplify.jobs redirect); the direct one is
         preferred when both are present.
      4. Resolve the Company cell's actual name, carrying it forward across
         "↳"/continuation rows (see _CONTINUATION_MARKERS) — this has to
         happen for every row in document order regardless of whether that
         row ends up filtered out below, since a filtered-out row (e.g. a
         non-internship posting) can still be the row that establishes the
         real company name for a later internship row that continues from
         it.
      5. Run the Role/Location cells through the same _is_internship_title
         / _is_us_location filters every other source uses, so a repo like
         this one — which lists both internships and full-time roles,
         across every country — only ever surfaces US internships/co-ops.
         If a column can't be identified by name at all (a repo with a
         differently-named or restructured table), that particular filter
         is simply skipped rather than dropping every row.
      6. Format via the same _format_role("Company — Role — Apply") every
         other source uses.

    Args:
        html_content: the raw README source (markdown text with embedded
            HTML table tags — this is NOT the GitHub-rendered HTML page).

    Returns:
        A list of formatted row strings (one per <tr> across all tables
        found), or None if the README contains no <table> at all — the
        caller falls back to plain line-diffing in that case, so this stays
        backward compatible with READMEs that use ordinary markdown tables
        or no tables at all.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return None

    lines: list[str] = []
    for table in tables:
        # Read the header row once per table to map column name -> index,
        # so this works regardless of exactly which columns a given repo's
        # README table has or what order they're in.
        header_cells = [th.get_text(strip=True) for th in table.find_all("th")]
        if not header_cells:
            continue
        app_idx = next(
            (i for i, h in enumerate(header_cells) if h.lower() in _APPLICATION_COLUMN_NAMES),
            None,
        )
        role_idx = next(
            (i for i, h in enumerate(header_cells) if h.lower() in _ROLE_COLUMN_NAMES),
            None,
        )
        location_idx = next(
            (i for i, h in enumerate(header_cells) if h.lower() in _LOCATION_COLUMN_NAMES),
            None,
        )
        company_idx = next(
            (i for i, h in enumerate(header_cells) if h.lower() in _COMPANY_COLUMN_NAMES),
            None,
        )

        last_company = ""
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if not cells:
                continue  # this <tr> is the <th> header row itself, skip it

            # Resolve the company name FIRST, unconditionally — this has to
            # run for every row regardless of the filters below, so a
            # continuation row further down still inherits the right name
            # even if the row that established it gets filtered out.
            company = last_company
            if company_idx is not None and company_idx < len(cells):
                company_text = cells[company_idx].get_text(" ", strip=True)
                if company_text and company_text not in _CONTINUATION_MARKERS:
                    company = company_text
            last_company = company or last_company

            role_text = (
                cells[role_idx].get_text(" ", strip=True)
                if role_idx is not None and role_idx < len(cells)
                else ""
            )
            if role_idx is not None and not _is_internship_title(role_text):
                continue
            if location_idx is not None and location_idx < len(cells):
                location_text = cells[location_idx].get_text(" ", strip=True)
                if not _is_us_location(location_text):
                    continue

            link = None
            if app_idx is not None and app_idx < len(cells):
                a_tags = cells[app_idx].find_all("a", href=True)
                if a_tags:
                    link = next(
                        (a["href"] for a in a_tags if "simplify.jobs" not in a["href"]),
                        a_tags[0]["href"],
                    )

            line = _format_role(company, role_text, link or "")
            if line.strip(" —*"):
                lines.append(line)

    return lines if lines else None


class Alerter:
    """Polls a list of configured sources on a timer and calls `on_update`
    whenever one of them has new content since the last poll.

    This class knows nothing about Discord — `on_update` is just a callback
    handed in by whatever's consuming it (bot.py). That separation is
    deliberate: alerter.py could be tested, reused, or driven by a totally
    different notifier (email, Slack, a CLI printout) without touching any
    of this polling/diffing logic.
    """

    def __init__(
        self,
        sources: list[dict],
        on_update: OnUpdate,
        poll_interval_seconds: int = 600,
        off_peak_poll_interval_seconds: int | None = None,
        peak_start_hour: int = 6,
        peak_end_hour: int = 17,
        state_path: str = "state.json",
        github_token: str | None = None,
        max_concurrency: int = 8,
    ):
        """
        Args:
            sources: list of source config dicts straight out of
                config.yaml (each has at least "type" and "name", plus
                type-specific keys like "url" — see README/config.example.yaml
                for the full shape per type).
            on_update: async callback invoked as `on_update(source_name,
                new_lines)` whenever a poll finds lines that weren't present
                in the previous poll of that source.
            poll_interval_seconds: how long `run()` sleeps between full
                poll cycles (a "cycle" = checking every source once) during
                the "peak" window (weekdays, `peak_start_hour` to
                `peak_end_hour`, local system time — default 6am-5pm).
            off_peak_poll_interval_seconds: the sleep duration used at all
                other times (weekday evenings/nights, and all of Saturday
                and Sunday) — the idea being new postings mostly land
                during business hours, so there's little to gain from
                polling as aggressively overnight or on weekends, and
                slowing down there cuts most of the bot's network/CPU cost
                for basically no lost responsiveness. If left as `None`
                (the default), the schedule is effectively disabled: every
                poll uses `poll_interval_seconds`, exactly like before this
                option existed — an explicit opt-in, not a behavior change
                for anyone who hasn't set it.
            peak_start_hour: hour (0-23, local time) the peak window opens.
            peak_end_hour: hour (0-23, local time) the peak window closes
                (exclusive — 17 means "up to but not including 5pm").
            state_path: where the last-seen-lines-per-source snapshot is
                persisted as JSON, so a restart doesn't forget what it's
                already alerted on (and doesn't re-alert on everything).
            github_token: optional GitHub PAT. Only affects github_repo
                sources — raises the API rate limit from 60/hr
                (unauthenticated) to 5000/hr. Not needed for a handful of
                repos polled every few minutes.
        """
        self.sources = sources
        self.on_update = on_update
        self.poll_interval_seconds = poll_interval_seconds
        self.off_peak_poll_interval_seconds = off_peak_poll_interval_seconds
        self.peak_start_hour = peak_start_hour
        self.peak_end_hour = peak_end_hour
        self.state_path = Path(state_path)
        self.github_token = github_token
        # Max sources fetched at once in a poll cycle. A cycle takes roughly
        # as long as its slowest source instead of the sum of all of them.
        self.max_concurrency = max(1, max_concurrency)
        # url -> ETag from the last 200 response, for conditional GETs.
        # In-memory only: the first poll after a restart is always a full
        # fetch, later ones send If-None-Match and usually get a 304.
        self._etags: dict[str, str] = {}
        # job page url -> published datetime (UTC), so each page is fetched once.
        self._posted_cache: dict[str, datetime.datetime] = {}
        # self.state maps source name -> the full list of lines seen for
        # that source as of the last successful poll. This is the "memory"
        # that makes diffing possible across restarts.
        self.state: dict[str, list[str]] = self._load_state()

    def _load_state(self) -> dict[str, list[str]]:
        """Read the persisted state file, if any. Returns {} on first ever
        run (file doesn't exist yet) — every source will then be treated as
        brand new and silently snapshotted on the first poll."""
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {}

    def _save_state(self) -> None:
        """Overwrite the state file with the current in-memory state.
        Called after every single source check (not just at the end of a
        full poll cycle) so a crash partway through a cycle doesn't lose
        progress on the sources already checked."""
        # Write to a temp file then rename, so a crash mid-write can never
        # leave a truncated state.json (os.replace is atomic on POSIX).
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.state, separators=(",", ":")))
        os.replace(tmp, self.state_path)

    async def _fetch_github_repo_lines(
        self, session: aiohttp.ClientSession, source: dict
    ) -> list[str]:
        """Fetch a GitHub repo's README and turn it into a list of lines to
        diff.

        Uses GET /repos/{owner}/{repo}/readme, which — unlike raw.githubusercontent.com
        — resolves whatever the repo's default branch happens to be (main,
        dev, master, ...) without us having to know or hardcode it. The
        response wraps the file content as base64, hence the decode step.

        If the README contains an HTML <table> (as SimplifyJobs-style repos
        do), _extract_table_rows() parses it into clean per-row lines
        (see that function's docstring for why this matters). Otherwise
        falls back to plain content.splitlines() — one line of the raw
        markdown per diffable unit, same as before that parser existed.
        """
        url = f"https://api.github.com/repos/{source['owner']}/{source['repo']}/readme"
        # The raw media type returns the file body directly and has no size cap;
        # the default JSON form returns an empty `content` for READMEs over
        # 1 MB (SimplifyJobs' list is ~1.1 MB), which looked like "no jobs".
        headers = {"Accept": "application/vnd.github.raw+json"}
        if self.github_token:
            headers["Authorization"] = f"Bearer {self.github_token}"
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            content = await resp.text(encoding="utf-8", errors="replace")
        table_rows = _extract_table_rows(content)
        if table_rows is not None:
            return table_rows
        md_rows = _extract_markdown_table_rows(content)
        if md_rows is not None:
            return md_rows
        return content.splitlines()

    async def _fetch_apple_jobs_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Apple's search shows 20 postings per page (`&page=N`); page until one
        comes back empty or `max_results` is reached."""
        base = source["url"]
        sep = "&" if "?" in base else "?"
        lines: list[str] = []
        for page in range(1, math.ceil(source.get("max_results", 400) / 20) + 1):
            url = base if page == 1 else f"{base}{sep}page={page}"
            async with session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()
            page_lines, seen = _extract_apple_jobs(html, source["name"])
            if seen == 0:
                break
            lines.extend(l for l in page_lines if l not in lines)
        return lines

    async def _fetch_radancy_jobs_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch a Radancy/TalentBrew results list, following `data-total-pages`
        (`?p=N`) for up to 5 pages."""
        lines: list[str] = []
        async with session.get(source["url"]) as resp:
            resp.raise_for_status()
            html = await resp.text()
            final_url = resp.url
        pages = re.search(r'data-total-pages="(\d+)"', html)
        total_pages = min(int(pages.group(1)) if pages else 1, 5)
        page = 1
        while True:
            page_lines, _ = _extract_radancy_jobs(html, source["name"], str(final_url))
            lines.extend(l for l in page_lines if l not in lines)
            page += 1
            if page > total_pages:
                break
            async with session.get(final_url.update_query(p=page)) as resp:
                resp.raise_for_status()
                html = await resp.text()
        return lines

    async def _fetch_webpage_lines(
        self, session: aiohttp.ClientSession, source: dict
    ) -> list[str]:
        """Fetch an arbitrary URL's HTML and turn its visible text into a
        list of lines to diff. Used for career pages that are plain
        server-rendered HTML (no JS needed to see the job listings) and
        don't have a structured JSON API — see Apple/Capital One in
        config.yaml for real examples.

        Steps:
          1. GET the page.
          2. Parse with BeautifulSoup and delete every <script>/<style> tag
             *before* extracting text — otherwise inline JS (which often
             embeds build hashes, timestamps, or analytics payloads that
             change on every single request) would make nearly every poll
             look like "the page changed", flooding you with false alerts.
          3. Optionally narrow to a sub-element via a CSS `selector` from
             config, to further cut out page chrome (nav/footer/cookie
             banners) that isn't part of the actual job listings.
          4. Get all visible text, split into lines, and drop blank/
             whitespace-only ones.

        Raises ValueError if a `selector` is configured but matches nothing
        on the page — this is treated as a real error (not silently
        ignored) since it usually means the site's HTML structure changed
        and the source needs to be reconfigured.
        """
        async with session.get(source["url"]) as resp:
            resp.raise_for_status()
            html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")
        # Strip script/style content — otherwise inline JS (build hashes,
        # timestamps, telemetry) makes the diff noisy on almost every poll.
        for tag in soup(["script", "style"]):
            tag.decompose()
        selector = source.get("selector")
        scope = soup.select_one(selector) if selector else soup
        if scope is None:
            raise ValueError(f"selector {selector!r} matched nothing on {source['url']}")
        text = scope.get_text(separator="\n")
        return [line.strip() for line in text.splitlines() if line.strip()]

    async def _fetch_amazon_jobs_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch job postings from amazon.jobs's search API, paginating to
        get more than the first page.

        amazon.jobs's `search.json` endpoint always caps a single response
        at 10 hits no matter what `limit` you pass — but it does honor an
        `offset` parameter for pagination, so this loops through pages of
        10 (offset=0, 10, 20, ...) up to `source["max_results"]` (default
        100), stopping early if a page comes back with zero jobs (meaning
        we've reached the end of the result set).
        """
        company = source["name"]
        base_url = source["url"]
        sep = "&" if "?" in base_url else "?"
        lines: list[str] = []
        for offset in range(0, source.get("max_results", 100), 10):
            async with session.get(f"{base_url}{sep}offset={offset}") as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            jobs = data.get("jobs", [])
            if not jobs:
                break
            lines.extend(_extract_amazon_jobs({"jobs": jobs}, company))
        return lines

    async def _fetch_workday_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch job postings from a Workday `cxs` jobs API, paginating to
        get more than the first page.

        This exists because of a real miss: NVIDIA's Workday board matches
        938 total postings against `searchText: "intern"` (Workday matches
        loosely against description text too, not just titles — most of
        those 938 aren't real internships at all), but a single request is
        hard-capped at 20 results (confirmed by testing — anything above
        `limit: 20` gets an HTTP 400, not more results), and Workday's
        default sort isn't strictly chronological. A genuinely new
        internship posting can easily land outside that first page of 20,
        which is exactly what happened — a real "Software Engineer Intern"
        posting existed but wasn't being seen because nothing paginated
        past page one. This loops through pages of 20 via `offset`, up to
        `source["max_results"]` (default 200 — comfortably past where real
        internship-titled postings have been observed to cluster, without
        paginating through all ~900+ mostly-irrelevant matches on a source
        that large), stopping early once `offset` reaches the response's
        own reported `total`.
        """
        company = source["name"]
        url = source["url"]
        base = re.sub(r"/wday/cxs/[^/]+/", "/", url).removesuffix("/jobs")
        lines: list[str] = []
        total = None
        for offset in range(0, source.get("max_results", 200), 20):
            if total is not None and offset >= total:
                break
            payload = {
                "limit": 20,
                "offset": offset,
                "searchText": source.get("search_text", "intern"),
            }
            async with session.post(url, json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            total = data.get("total", 0)
            postings = data.get("jobPostings", [])
            if not postings:
                break
            lines.extend(_extract_workday({"jobPostings": postings}, base, company))
        return lines

    async def _fetch_google_jobs_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch every internship posting from Google's careers site,
        paginating through result pages.

        Each page (`?employment_type=INTERN&page=N`, `page` omitted or 1
        for the first page) returns up to 20 postings server-rendered
        directly into the HTML — see _extract_google_jobs for how that
        data is actually embedded and found. This pages from 1 upward,
        accumulating results, until either the running total reaches the
        `total` the page itself reports or a page comes back with zero
        parseable jobs (a defensive stop in case Google's page layout ever
        changes enough that _GOOGLE_DS1_RE stops matching).
        """
        base_url = source["url"]
        sep = "&" if "?" in base_url else "?"
        lines: list[str] = []
        page = 1
        seen = 0
        while True:
            url = base_url if page == 1 else f"{base_url}{sep}page={page}"
            async with session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()
            page_lines, total, raw_count = _extract_google_jobs(html)
            if raw_count == 0:
                break  # empty page: either genuinely done, or (page 1 only)
                # the page layout changed enough that _GOOGLE_DS1_RE no
                # longer matches — either way, nothing left to gain by
                # continuing.
            lines.extend(page_lines)
            # `total` counts ALL postings this query matched, not just the
            # internship-titled ones that made it into page_lines, so
            # tracking `seen` against it (via raw_count, the actual number
            # of entries on THIS page) — rather than len(lines), which
            # filtering has already shrunk — is what actually tells us
            # whether another page remains.
            seen += raw_count
            if seen >= total:
                break
            page += 1
        return lines

    async def _fetch_linkedin_jobs_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch every posting from a LinkedIn company-scoped job search,
        paginating via `start` (LinkedIn's own offset param, 25 results per
        page in normal use). Stops once a page returns zero cards — this
        endpoint doesn't report a total count anywhere accessible, so an
        empty page is the only stop signal available (same situation as
        WP Job Manager's `found_jobs` sentinel, just without a named
        field for it here).
        """
        company = source["name"]
        base_url = source["url"]
        sep = "&" if "?" in base_url else "?"
        lines: list[str] = []
        for start in range(0, source.get("max_results", 100), 25):
            url = base_url if start == 0 else f"{base_url}{sep}start={start}"
            async with session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()
            page_lines = _extract_linkedin_jobs(html, company)
            if "base-search-card" not in html:
                break
            lines.extend(page_lines)
        return lines

    async def _fetch_bloomberg_jobs_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch every posting from Bloomberg's Avature job search,
        paginating via `jobOffset` (12 results per page, confirmed by
        testing). Stops once a page returns zero result articles.
        """
        company = source["name"]
        base_url = source["url"]
        sep = "&" if "?" in base_url else "?"
        lines: list[str] = []

        async def fetch_page(offset: int) -> str:
            url = base_url if offset == 0 else f"{base_url}{sep}jobOffset={offset}"
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.text()

        # Pages are independent, so fetch a window of them at once instead of
        # one at a time; stop after the first window that contains an empty
        # page (pages come back in order, so everything after it is empty too).
        offsets = list(range(0, source.get("max_results", 200), 12))
        window = 6
        for i in range(0, len(offsets), window):
            pages = await asyncio.gather(*(fetch_page(o) for o in offsets[i : i + window]))
            done = False
            for html in pages:
                if "article--result" not in html:
                    done = True
                    break
                lines.extend(_extract_bloomberg_jobs(html, company))
            if done:
                break
        return lines

    async def _fetch_wp_job_manager_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Fetch every listing from a WP Job Manager career board by
        calling its own frontend's AJAX endpoint directly, paginating
        through all result pages.

        Unlike the ATS APIs (Greenhouse, Lever, ...), this isn't a
        documented public API — it's the same internal endpoint
        (`admin-ajax.php?action=job_manager_get_listings`) the plugin's own
        "Load more listings" button calls, found by reading the page's
        source for its JS config rather than from any API docs. It's a POST
        with form-encoded fields (not JSON), and conveniently requires no
        auth/nonce despite looking like an admin-only endpoint by its URL.

        The response tells us `max_num_pages` and `found_jobs`, so we page
        from 1 upward until either the server says there are no more pages
        or explicitly reports no jobs found — same "keep going until the
        source itself says stop" pattern as _fetch_amazon_jobs_lines, just
        driven by an explicit page count instead of an empty-page sentinel.
        """
        url = source["url"]
        lines: list[str] = []
        page = 1
        while True:
            payload = {
                "action": "job_manager_get_listings",
                "search_keywords": source.get("search_keywords", ""),
                "search_location": source.get("search_location", ""),
                "per_page": 100,
                "orderby": "featured",
                "order": "DESC",
                "page": page,
            }
            async with session.post(url, data=payload) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            if not data.get("found_jobs"):
                break
            lines.extend(_extract_wp_job_manager(data.get("html", "")))
            if page >= data.get("max_num_pages", 1):
                break
            page += 1
        return lines

    async def _fetch_meta_jobs_lines(self, browser: Browser, source: dict) -> list[str]:
        """Fetch Meta's internship postings by loading the real careers
        search page in a browser and intercepting the GraphQL response its
        own JS makes — see _extract_meta_jobs for why this needs a real
        browser at all (a session-bound CSRF token, not just a JS-rendering
        problem).

        Waits specifically for the `job_search_with_featured_jobs_v2`
        response containing `all_jobs` (the page fires several other
        `/graphql` calls for filter metadata and job counts that aren't
        useful here) rather than a fixed sleep, so this doesn't race the
        page's own loading time.
        """
        page = await browser.new_page(user_agent=_BROWSER_USER_AGENT)
        try:
            captured: dict = {}

            async def on_response(resp):
                if "/graphql" not in resp.url or resp.request.method != "POST":
                    return
                try:
                    body = await resp.json()
                except Exception:
                    return
                if "all_jobs" in (body.get("data") or {}).get("job_search_with_featured_jobs_v2", {}):
                    captured["data"] = body

            page.on("response", on_response)
            await page.goto(source["url"], timeout=30000)
            for _ in range(15):
                if "data" in captured:
                    break
                await page.wait_for_timeout(1000)
            if "data" not in captured:
                logger.warning("meta_jobs: never saw the job_search GraphQL response")
                return []
            return _extract_meta_jobs(captured["data"], source["name"])
        finally:
            await page.close()

    async def _fetch_snowflake_jobs_lines(self, browser: Browser, source: dict) -> list[str]:
        """Fetch Snowflake's internship postings by loading the real search
        page and reading rendered job cards' own data attributes — see
        _extract_snowflake_jobs for why this needs a real browser (the
        actual results aren't cleanly exposed via any single fetchable
        response among the many small AJAX calls the page makes).
        """
        page = await browser.new_page(user_agent=_BROWSER_USER_AGENT)
        try:
            await page.goto(source["url"], timeout=30000)
            try:
                await page.wait_for_selector('a[data-ph-at-id="job-link"]', timeout=15000)
            except Exception:
                logger.warning("snowflake_jobs: no job links appeared within timeout")
                return []
            raw_jobs = await page.evaluate(
                """() => Array.from(document.querySelectorAll('a[data-ph-at-id="job-link"]')).map(a => ({
                    title: a.getAttribute('data-ph-at-job-title-text') || '',
                    location: a.getAttribute('data-ph-at-job-location-text') || '',
                    url: a.href,
                }))"""
            )
            return _extract_snowflake_jobs(raw_jobs, source["name"])
        finally:
            await page.close()

    async def _fetch_uber_jobs_lines(self, browser: Browser, source: dict) -> list[str]:
        """Fetch Uber's internship postings by actually driving their
        search box with a real browser — see _extract_uber_jobs for why
        (the `?query=` URL param is silently ignored; this is the only way
        to get real filtered results at all).

        Steps: load the base careers URL, dismiss whatever consent/promo
        dialog is in the way (best-effort — clicks common "Accept" button
        text if present, then force-removes any lingering dialog overlay
        via JS regardless, since the exact dialog varies and blocking on
        one specific button risks getting stuck if it doesn't appear),
        type the configured search term into the actual search input and
        press Enter, then read whatever cards render.
        """
        page = await browser.new_page(user_agent=_BROWSER_USER_AGENT)
        try:
            await page.goto(source["url"], timeout=30000)
            await page.wait_for_timeout(2000)
            for text in ("Accept All", "Accept"):
                btn = page.get_by_text(text, exact=False)
                if await btn.count() > 0:
                    try:
                        await btn.first.click(timeout=2000)
                        break
                    except Exception:
                        pass
            await page.evaluate(
                """() => document.querySelectorAll('[data-slot="dialog-overlay"], [role="dialog"]')
                    .forEach(el => el.remove())"""
            )
            search_input = page.locator("input").first
            await search_input.click(timeout=5000, force=True)
            await search_input.fill(source.get("search_text", "intern"))
            await search_input.press("Enter")
            await page.wait_for_timeout(3000)
            raw_jobs = await page.evaluate(
                """() => Array.from(document.querySelectorAll('div[data-slot="card"]')).map(c => {
                    const titleEl = c.querySelector('a.js-view-job');
                    const pills = c.querySelectorAll('[data-slot="card-description"] > div > div');
                    return {
                        title: titleEl ? titleEl.textContent.trim() : '',
                        url: titleEl ? titleEl.href : '',
                        location: pills[0] ? pills[0].textContent.trim() : '',
                    };
                })"""
            )
            return _extract_uber_jobs(raw_jobs, source["name"])
        finally:
            await page.close()

    async def _fetch_tesla_jobs_lines(self, browser: Browser, source: dict) -> list[str]:
        """Attempt to fetch Tesla's internship postings via a real browser.

        Included for completeness, but expectations should be low: Tesla's
        edge (Akamai Bot Manager) returned HTTP 403 on *every* attempt made
        while building this — including a real Chromium browser, a
        residential IP, a realistic user-agent, stealth patches
        (navigator.webdriver masked, spoofed plugins), and even just
        loading the plain homepage first. This is a deeper fingerprinting
        block than the JS-rendering problem every other browser-based
        source here has, and it's not clear a personal bot like this one
        should keep escalating further (e.g. paid anti-detect browser
        services) to get past it. This method exists so that IF Tesla's
        block behaves differently from wherever this actually runs, it'll
        start working with no further changes needed — check bot.log for
        the warning below if it's still blocked.
        """
        page = await browser.new_page(user_agent=_BROWSER_USER_AGENT)
        try:
            resp = await page.goto(source["url"], timeout=20000)
            if resp is not None and resp.status >= 400:
                logger.warning(
                    "tesla_jobs: still blocked (HTTP %d) — see this method's docstring", resp.status
                )
                return []
            # Never observed real markup to build a verified extractor
            # against (every attempt was blocked) — if this ever starts
            # getting through, check what the page actually looks like
            # and fill in real selectors here.
            logger.warning("tesla_jobs: got past the block but no extraction logic exists yet — "
                            "inspect the real page now and implement selectors")
            return []
        finally:
            await page.close()

    async def _line_age_hours(
        self, session: aiohttp.ClientSession, line: str
    ) -> float | None:
        """Hours since the Jobright posting linked in `line` was published, read
        from the job page's own `publishTime` (UTC). Jobright's README "Date
        Posted" column is unreliable — it shows today's date for jobs whose
        page says they were published weeks ago. Returns None if the line has
        no Jobright link or the page can't be read/parsed (caller keeps it)."""
        match = _JOBRIGHT_URL_RE.search(line)
        if not match:
            return None
        url = match.group(0)
        posted = self._posted_cache.get(url)
        if posted is None:
            # Jobright throttles bursts (it answers with a redirect to a
            # security check), so retry once after a short pause.
            for attempt in range(2):
                try:
                    async with session.get(url) as resp:
                        html = await resp.text()
                except Exception:
                    html = ""
                published = _PUBLISH_TIME_RE.search(html)
                if published:
                    posted = datetime.datetime.strptime(
                        published.group(1), "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=datetime.timezone.utc)
                    self._posted_cache[url] = posted
                    break
                if attempt == 0:
                    await asyncio.sleep(2)
            else:
                logger.warning("could not read posting date for %s", url)
                return None
        return (datetime.datetime.now(datetime.timezone.utc) - posted).total_seconds() / 3600

    async def _drop_stale(
        self, session: aiohttp.ClientSession, lines: list[str], max_age_hours: float
    ) -> list[str]:
        """Keep only lines posted within `max_age_hours`. Lines whose age can't
        be determined are kept, so a parsing failure never hides a real alert."""
        sem = asyncio.Semaphore(2)

        async def age(line: str) -> float | None:
            async with sem:
                return await self._line_age_hours(session, line)

        ages = await asyncio.gather(*(age(line) for line in lines))
        fresh = [l for l, a in zip(lines, ages) if a is None or a <= max_age_hours]
        if len(fresh) != len(lines):
            logger.info("dropped %d stale posting(s) older than %sh", len(lines) - len(fresh), max_age_hours)
        return fresh

    async def _get_json_conditional(
        self, session: aiohttp.ClientSession, url: str, name: str
    ):
        """GET `url` as JSON, sending If-None-Match when we hold an ETag from
        a previous 200 and still have that source's last result in state.
        Returns the parsed JSON, or None on 304 Not Modified (the caller then
        reuses the stored lines — nothing downloaded, nothing to re-parse)."""
        headers = {}
        etag = self._etags.get(url)
        if etag and name in self.state:
            headers["If-None-Match"] = etag
        async with session.get(url, headers=headers) as resp:
            if resp.status == 304:
                return None
            resp.raise_for_status()
            if resp.headers.get("ETag"):
                self._etags[url] = resp.headers["ETag"]
            return await resp.json(content_type=None)

    async def _fetch_ats_lines(self, session: aiohttp.ClientSession, source: dict) -> list[str]:
        """Dispatch to the right fetch/extraction logic for a structured
        job-board API source (greenhouse / lever / ashby / workday /
        amazon_jobs), and return the formatted lines.

        amazon_jobs and workday are both handled separately (delegated to
        their own paginated fetch methods), since both need to page through
        multiple requests to see past a single-page cap — see
        _fetch_amazon_jobs_lines and _fetch_workday_lines for why each one
        specifically needs that. Greenhouse/Lever/Ashby are simpler: one
        GET returns everything, no pagination needed.

        `company` — used for every extractor's `_format_role(company, ...)`
        call — is just the source's own configured `name` from
        config.yaml, since a Greenhouse/Lever/Ashby board (or a Workday
        site) is always scoped to exactly one company already.

        `resp.json(content_type=None)` is used instead of plain
        `resp.json()` because some of these APIs don't reliably set an
        `application/json` Content-Type header, which aiohttp would
        otherwise treat as an error and refuse to parse.
        """
        source_type = source["type"]
        url = source["url"]
        company = source["name"]

        if source_type == "amazon_jobs":
            return await self._fetch_amazon_jobs_lines(session, source)
        if source_type == "workday":
            return await self._fetch_workday_lines(session, source)

        data = await self._get_json_conditional(session, url, company)
        if data is None:
            return self.state[company]

        if source_type == "greenhouse":
            return _extract_greenhouse(data, company)
        elif source_type == "lever":
            return _extract_lever(data, company)
        elif source_type == "ashby":
            return _extract_ashby(data, company)
        else:
            raise ValueError(f"unknown ATS source type {source_type!r}")

    async def _check_source(
        self, session: aiohttp.ClientSession, source: dict, browser: Browser | None = None
    ) -> None:
        """Check a single source once: fetch its current content, compare
        against what was seen last poll, and fire `on_update` if there's
        anything new.

        `browser` is a live Playwright browser, passed in by `poll_once`
        only when this poll cycle actually contains a source that needs
        one (see `_BROWSER_SOURCE_TYPES`) — it's `None` for the vast
        majority of sources, which never touch it.

        This is the heart of the whole bot's alerting logic, and the
        diffing rule is deliberately simple: a "line" (whatever that means
        for this source type — a README line, a formatted job posting,
        a chunk of page text) counts as "new" if it is not byte-for-byte
        equal to any line seen in the previous poll of this same source.
        There's no fuzzy matching — if a posting's title or location text
        changes even slightly, it will look like a "new" line. In practice
        this is fine because postings rarely get silently edited, and the
        alternative (fuzzy/partial matching) risks either missing genuinely
        new postings or suppressing real ones — better to occasionally
        re-alert on an edited posting than to ever silently drop a new one.

        Special case — first time seeing a source: if `name` isn't in
        `self.state` yet (a brand new source, or state.json was just
        cleared/deleted), there is nothing to diff against, so the current
        content is stored as the baseline WITHOUT calling `on_update`. This
        is what stops a newly-added source (or a fresh install) from
        immediately dumping its entire current listing on you as if every
        single existing posting were "new".

        State is saved after every source (not batched until the end of a
        full poll cycle) so that if a later source in the list throws an
        error, or the process gets killed mid-cycle, the sources already
        checked don't lose their progress and get needlessly re-diffed
        (or worse, re-alerted) next time.
        """
        name = source["name"]
        source_type = source["type"]
        # Logged at DEBUG (not INFO) so normal operation stays quiet, but if
        # a poll ever hangs again, bumping the log level shows exactly
        # which source it stalled on without needing a separate diagnostic
        # script — we hit this exact wall once already (see the timeout
        # added to poll_once) and want the next occurrence to be
        # immediately diagnosable from bot.log alone.
        logger.debug("checking source %r (%s)", name, source_type)

        if source_type == "github_repo":
            current_lines = await self._fetch_github_repo_lines(session, source)
        elif source_type == "webpage":
            current_lines = await self._fetch_webpage_lines(session, source)
        elif source_type == "apple_jobs":
            current_lines = await self._fetch_apple_jobs_lines(session, source)
        elif source_type == "radancy_jobs":
            current_lines = await self._fetch_radancy_jobs_lines(session, source)
        elif source_type in ("greenhouse", "lever", "ashby", "workday", "amazon_jobs"):
            current_lines = await self._fetch_ats_lines(session, source)
        elif source_type == "wp_job_manager":
            current_lines = await self._fetch_wp_job_manager_lines(session, source)
        elif source_type == "google_jobs":
            current_lines = await self._fetch_google_jobs_lines(session, source)
        elif source_type == "linkedin_jobs":
            current_lines = await self._fetch_linkedin_jobs_lines(session, source)
        elif source_type == "bloomberg_jobs":
            current_lines = await self._fetch_bloomberg_jobs_lines(session, source)
        elif source_type in _BROWSER_SOURCE_TYPES:
            if browser is None:
                raise RuntimeError(f"{name!r} needs a browser but none was started for this poll")
            if source_type == "meta_jobs":
                current_lines = await self._fetch_meta_jobs_lines(browser, source)
            elif source_type == "snowflake_jobs":
                current_lines = await self._fetch_snowflake_jobs_lines(browser, source)
            elif source_type == "uber_jobs":
                current_lines = await self._fetch_uber_jobs_lines(browser, source)
            else:
                current_lines = await self._fetch_tesla_jobs_lines(browser, source)
        else:
            raise ValueError(f"unknown source type {source_type!r} for {name!r}")

        previous_lines = self.state.get(name)

        if previous_lines is None:
            # First time seeing this source — snapshot silently, don't alert
            # on the entire pre-existing content.
            logger.info("first snapshot for %r (%d lines)", name, len(current_lines))
        else:
            # Order-preserving set difference: keep current_lines' order
            # (so alerts read top-to-bottom as the source presents them)
            # while using a set for O(1) "have I seen this before" lookups
            # rather than an O(n) scan of previous_lines per line.
            previous_set = set(previous_lines)
            new_lines = [line for line in current_lines if line not in previous_set]
            if new_lines and source.get("max_age_hours"):
                new_lines = await self._drop_stale(session, new_lines, source["max_age_hours"])
            if new_lines:
                logger.info("%r changed: %d new line(s)", name, len(new_lines))
                await self.on_update(name, new_lines)

        if previous_lines != current_lines:
            self.state[name] = current_lines
            self._save_state()

    async def poll_once(self) -> None:
        """Check every configured source exactly once, in order, using one
        shared HTTP session/connection pool for the whole pass.

        Each source is wrapped in its own try/except — an error fetching
        one source (a site down, a changed API, a network blip) is logged
        and skipped, not allowed to crash the entire poll and take every
        other source down with it. That source simply gets checked again
        next cycle.

        Without an explicit timeout, a single unresponsive endpoint (seen in
        practice: a Cloudflare-fronted site that accepts the TCP connection
        but never finishes responding, likely after a bot-detection
        challenge) hangs forever — and because sources are checked
        sequentially with `await`, that one hung request blocks every
        source after it too, and since `run()` awaits a full poll_once()
        before sleeping, it blocks all future polling as well, silently.
        The per-source try/except alone doesn't help with that: it only
        catches errors *raised*, not a request that's simply never
        returning. `timeout=30` gives every individual request (not the
        whole poll cycle) a hard ceiling — past that, aiohttp raises
        asyncio.TimeoutError, which the try/except below does catch, so a
        stuck source gets logged and skipped instead of freezing the bot.

        A source with `enabled: false` in config.yaml is skipped entirely
        — not fetched, not diffed, no state.json entry touched. This is
        deliberately just a flag someone can flip in config.yaml and
        restart, not a separate CLI/removal step: it's the easy way to
        temporarily quiet one noisy source without losing its config
        (name, URL, board_url, ...) or its accumulated state.json history,
        both of which stay untouched and pick right back up if re-enabled
        later. Missing the key entirely defaults to enabled (`True`), so
        every source from before this option existed keeps working
        unchanged.

        A real Chromium browser (via Playwright) is launched for this poll
        cycle ONLY if at least one enabled source actually needs one (type
        in `_BROWSER_SOURCE_TYPES` — Meta, Snowflake, Uber, Tesla; see each
        one's fetch method for why a plain HTTP request isn't enough for
        it). One shared browser instance is reused across all of them,
        rather than launching a fresh Chromium process per source
        (expensive) or leaving one running between poll cycles (wasteful —
        a real browser sitting idle for 5 minutes gains nothing). It's
        closed again before this method returns either way. This is also
        why those 4 sources are checked LAST in practice: the loop below
        just goes through `self.sources` in config order, so put them near
        the end of config.yaml if you want the (cheap, fast) HTTP-based
        sources to always finish even if the browser step hangs or a
        Playwright dependency is missing.
        """
        connector = aiohttp.TCPConnector(ssl=_SSL_CONTEXT)
        # aiohttp's default User-Agent ("Python/3.x aiohttp/x.x") gets
        # flagged and blocked by basic bot-detection (Cloudflare, etc.) on
        # some career sites even for perfectly legitimate polling — a
        # realistic browser UA avoids that without changing any actual
        # request behavior.
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        timeout = aiohttp.ClientTimeout(total=30)

        needs_browser = any(
            source.get("type") in _BROWSER_SOURCE_TYPES and source.get("enabled", True)
            for source in self.sources
        )
        playwright_ctx = None
        browser: Browser | None = None
        if needs_browser:
            try:
                playwright_ctx = await async_playwright().start()
                browser = await playwright_ctx.chromium.launch(headless=True)
            except Exception:
                logger.exception(
                    "failed to launch Playwright/Chromium — browser-based sources will be "
                    "skipped this cycle (is `playwright install chromium` done?)"
                )

        try:
            async with aiohttp.ClientSession(
                connector=connector, headers=headers, timeout=timeout
            ) as session:
                sem = asyncio.Semaphore(self.max_concurrency)

                async def check(source: dict) -> None:
                    async with sem:
                        try:
                            await self._check_source(session, source, browser)
                        except Exception:
                            logger.exception("error checking source %r", source.get("name"))

                enabled = []
                for source in self.sources:
                    if source.get("enabled", True):
                        enabled.append(source)
                    else:
                        logger.debug("skipping disabled source %r", source.get("name"))
                await asyncio.gather(*(check(source) for source in enabled))
        finally:
            if browser is not None:
                await browser.close()
            if playwright_ctx is not None:
                await playwright_ctx.stop()

    def _is_peak_time(self, now: datetime.datetime) -> bool:
        """True if `now` (local system time) falls inside the configured
        peak polling window — weekdays only (Monday=0 through Friday=4;
        `now.weekday()` returns 5 for Saturday and 6 for Sunday, so `< 5`
        is exactly "not the weekend"), between `peak_start_hour` and
        `peak_end_hour`.
        """
        is_weekday = now.weekday() < 5
        in_hours = self.peak_start_hour <= now.hour < self.peak_end_hour
        return is_weekday and in_hours

    def _next_peak_start(self, now: datetime.datetime) -> datetime.datetime:
        """The next moment (today or a future day) the peak window opens.

        Used so that sleeping through off-peak hours never overshoots past
        the instant peak hours actually begin — see `run()`. Without this,
        a bot that went to sleep for a full hour at, say, 5:30am on a
        Tuesday would sleep straight through the first 30 minutes of that
        day's peak window, which defeats the entire point of having a
        peak window tuned for responsiveness in the first place.
        """
        candidate = now.replace(hour=self.peak_start_hour, minute=0, second=0, microsecond=0)
        if candidate <= now:
            candidate += datetime.timedelta(days=1)
        while candidate.weekday() >= 5:  # skip forward over Sat/Sun
            candidate += datetime.timedelta(days=1)
        return candidate

    def _next_sleep_seconds(self) -> float:
        """How long `run()` should sleep before the next poll, given the
        current moment.

        If no off-peak interval is configured, the schedule feature is
        simply off and this always returns `poll_interval_seconds` — the
        exact behavior from before this option existed.

        Otherwise: during peak hours, sleep the (short) peak interval, same
        as always. Outside peak hours, sleep the (long) off-peak interval
        — UNLESS the peak window is due to open sooner than that, in which
        case sleep only until then instead. That comparison is what
        guarantees zero lag entering peak hours regardless of how long the
        off-peak interval is: the wait is always capped at "whichever comes
        first, the configured interval or peak hours actually starting".
        """
        if self.off_peak_poll_interval_seconds is None:
            return self.poll_interval_seconds

        now = datetime.datetime.now()
        if self._is_peak_time(now):
            return self.poll_interval_seconds

        seconds_until_peak = (self._next_peak_start(now) - now).total_seconds()
        return max(1.0, min(self.off_peak_poll_interval_seconds, seconds_until_peak))

    async def run(self) -> None:
        """The main loop: poll every source, wait for the next scheduled
        poll (see `_next_sleep_seconds`), repeat forever. This is what
        bot.py schedules as a background asyncio task once it's connected
        to Discord — see bot.py's on_ready handler.
        """
        while True:
            started = time.monotonic()
            await self.poll_once()
            elapsed = time.monotonic() - started
            # Fixed-rate scheduling: the interval is measured start-to-start,
            # so a slow cycle doesn't stretch the period (sleeping the full
            # interval *after* each cycle made the real period interval +
            # cycle time, i.e. worst-case alert latency grew with it).
            sleep_for = max(0.0, self._next_sleep_seconds() - elapsed)
            logger.info("poll cycle took %.1fs; sleeping %.0fs", elapsed, sleep_for)
            await asyncio.sleep(sleep_for)
