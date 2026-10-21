import asyncio
import json
import time

import aiohttp
from aiohttp import web

from alerter import Alerter


def make_alerter(tmp_path, sources, on_update=None, **kw):
    async def noop(name, lines):
        pass

    return Alerter(
        sources=sources,
        on_update=on_update or noop,
        state_path=str(tmp_path / "state.json"),
        **kw,
    )


async def serve(handler):
    app = web.Application()
    app.router.add_get("/", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}/"


def test_state_save_is_atomic_and_compact(tmp_path):
    a = make_alerter(tmp_path, [])
    a.state = {"x": ["a", "b"]}
    a._save_state()
    assert json.loads((tmp_path / "state.json").read_text()) == {"x": ["a", "b"]}
    assert not (tmp_path / "state.json.tmp").exists()
    assert " " not in (tmp_path / "state.json").read_text()


def test_first_snapshot_is_silent_then_only_new_lines_alert(tmp_path):
    body = {"text": "<p>one</p><p>two</p>"}

    async def handler(request):
        return web.Response(text=body["text"], content_type="text/html")

    async def run():
        runner, url = await serve(handler)
        alerts = []

        async def on_update(name, lines):
            alerts.append((name, lines))

        a = make_alerter(tmp_path, [{"type": "webpage", "name": "S", "url": url}], on_update)
        await a.poll_once()
        assert alerts == []  # first sight: silent baseline
        body["text"] = "<p>one</p><p>two</p><p>three</p>"
        await a.poll_once()
        await runner.cleanup()
        return alerts

    assert asyncio.run(run()) == [("S", ["three"])]


def test_state_only_written_when_content_changes(tmp_path):
    async def handler(request):
        return web.Response(text="<p>same</p>", content_type="text/html")

    async def run():
        runner, url = await serve(handler)
        a = make_alerter(tmp_path, [{"type": "webpage", "name": "S", "url": url}])
        saves = []
        orig = a._save_state
        a._save_state = lambda: (saves.append(1), orig())
        await a.poll_once()
        await a.poll_once()
        await a.poll_once()
        await runner.cleanup()
        return len(saves)

    assert asyncio.run(run()) == 1


def test_conditional_get_uses_304_and_reuses_state(tmp_path):
    hits = {"full": 0, "304": 0}

    async def handler(request):
        if request.headers.get("If-None-Match") == '"v1"':
            hits["304"] += 1
            return web.Response(status=304, headers={"ETag": '"v1"'})
        hits["full"] += 1
        return web.json_response({"jobs": []}, headers={"ETag": '"v1"'})

    async def run():
        runner, url = await serve(handler)
        a = make_alerter(tmp_path, [])
        async with aiohttp.ClientSession() as s:
            first = await a._get_json_conditional(s, url, "S")
            a.state["S"] = ["line"]
            second = await a._get_json_conditional(s, url, "S")
        await runner.cleanup()
        return first, second

    first, second = asyncio.run(run())
    assert first == {"jobs": []}
    assert second is None  # 304: caller reuses stored lines
    assert hits == {"full": 1, "304": 1}


def test_no_conditional_header_without_stored_state(tmp_path):
    seen = []

    async def handler(request):
        seen.append(request.headers.get("If-None-Match"))
        return web.json_response({}, headers={"ETag": '"v1"'})

    async def run():
        runner, url = await serve(handler)
        a = make_alerter(tmp_path, [])
        async with aiohttp.ClientSession() as s:
            await a._get_json_conditional(s, url, "S")
            await a._get_json_conditional(s, url, "S")  # no state yet -> still full fetch
        await runner.cleanup()

    asyncio.run(run())
    assert seen == [None, None]


def test_sources_are_polled_concurrently(tmp_path):
    sources = [{"type": "webpage", "name": f"s{i}", "url": "x"} for i in range(8)]

    async def run(limit):
        a = make_alerter(tmp_path, sources, max_concurrency=limit)

        async def slow(session, source, browser=None):
            await asyncio.sleep(0.2)

        a._check_source = slow
        t0 = time.perf_counter()
        await a.poll_once()
        return time.perf_counter() - t0

    assert asyncio.run(run(8)) < 0.6
    assert asyncio.run(run(1)) > 1.5


def test_one_failing_source_does_not_stop_the_others(tmp_path):
    sources = [{"type": "webpage", "name": n, "url": "x"} for n in ("bad", "good")]
    done = []

    async def run():
        a = make_alerter(tmp_path, sources)

        async def check(session, source, browser=None):
            if source["name"] == "bad":
                raise RuntimeError("boom")
            done.append(source["name"])

        a._check_source = check
        await a.poll_once()

    asyncio.run(run())
    assert done == ["good"]


def test_stale_postings_are_dropped_and_unknown_ages_kept(tmp_path, monkeypatch):
    import datetime
    import re

    import alerter as alerter_mod

    now = datetime.datetime.now(datetime.timezone.utc)
    fmt = "%Y-%m-%d %H:%M:%S"
    pages = {
        "fresh": f'{{"publishTime":"{(now - datetime.timedelta(hours=2)).strftime(fmt)}"}}',
        "old": f'{{"publishTime":"{(now - datetime.timedelta(hours=500)).strftime(fmt)}"}}',
        "nodate": "<html>security check</html>",
    }

    async def handler(request):
        return web.Response(text=pages[request.match_info["job"]])

    monkeypatch.setattr(alerter_mod, "_JOBRIGHT_URL_RE", re.compile(r"http://127\.0\.0\.1:\d+/job/\w+"))

    async def run():
        app = web.Application()
        app.router.add_get("/job/{job}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/job"
        a = make_alerter(tmp_path, [])
        lines = [f"| {n} | {base}/{n} |" for n in ("fresh", "old", "nodate")] + ["no link here"]
        async with aiohttp.ClientSession() as s:
            kept = await a._drop_stale(s, lines, 24)
        await runner.cleanup()
        return [l.split("|")[1].strip() if "|" in l else l for l in kept]

    # fresh kept; old dropped; undeterminable (no date / no link) kept so nothing real is hidden
    assert asyncio.run(run()) == ["fresh", "nodate", "no link here"]


def test_markdown_table_keeps_only_us_internships():
    from alerter import _extract_markdown_table_rows

    md = """
Some intro text
| Company | Job Title | Location | Work Model | Date Posted |
| ----- | --------- |  --------- | ---- | ------- |
| **[Uber](https://u.com)** | **[2027 Software Engineering Intern, US](https://jobright.ai/jobs/info/aa)** | San Francisco, CA, United States | On Site | Oct 20 |
| ↳ | **[Backend Co-op](https://jobright.ai/jobs/info/bb)** | Remote | Remote | Oct 20 |
| **[Acme](https://a.com)** | **[Senior Software Engineer](https://jobright.ai/jobs/info/cc)** | Austin, TX, United States | On Site | Oct 20 |
| **[Foo](https://f.com)** | **[Software Engineering Intern](https://jobright.ai/jobs/info/dd)** | London, United Kingdom | On Site | Oct 20 |
| **[Bar](https://b.com)** | **[Internal Tools Engineer](https://jobright.ai/jobs/info/ee)** | Seattle, WA, United States | On Site | Oct 20 |
"""
    rows = _extract_markdown_table_rows(md)
    assert [r.split("|")[2].strip() for r in rows] == [
        "**[2027 Software Engineering Intern, US](https://jobright.ai/jobs/info/aa)**",
        "**[Backend Co-op](https://jobright.ai/jobs/info/bb)**",
    ]
    assert _extract_markdown_table_rows("no table here") is None


def _apple_obj(pid, title, country_id, country):
    loc = f'[{{"countryName":"{country}","countryID":"{country_id}","name":"X"}}]'
    return (
        f'\\"locations\\":{loc.replace(chr(34), chr(92) + chr(34))},\\"positionId\\":\\"{pid}\\",'
        f'\\"postingDate\\":\\"Oct 21, 2026\\",\\"postingTitle\\":\\"{title}\\",'
        f'\\"localeInfo\\":{{\\"a\\":1}},\\"postDateInGMT\\":\\"2026-10-21T09:30:18Z\\",'
        f'\\"transformedPostingTitle\\":\\"slug-{pid}\\"'
    )


def test_apple_extractor_keeps_only_us_internships():
    from alerter import _extract_apple_jobs

    html = "<script>" + " ".join(
        _apple_obj(*a)
        for a in [
            (1, "Software Internship", "iso-country-USA", "United States"),
            (2, "Software Internship", "iso-country-GBR", "United Kingdom"),
            (3, "Senior Engineer", "iso-country-USA", "United States"),
        ]
    ) + "</script>"
    lines, seen = _extract_apple_jobs(html, "Apple")
    assert seen == 3
    assert lines == ["**Apple** — Software Internship — [Apply](https://jobs.apple.com/en-us/details/1/slug-1)"]


def test_radancy_extractor_keeps_only_us_internships():
    from alerter import _extract_radancy_jobs

    html = """<section id="search-results-list"><ul>
    <li><a href="/job/a/1"><h2>Data Intern</h2><span class="job-location">McLean, VA</span></a></li>
    <li><a href="/job/b/2"><h2>Data Intern</h2><span class="job-location">London, United Kingdom</span></a></li>
    <li><a href="/job/c/3"><h2>Director</h2><span class="job-location">McLean, VA</span></a></li>
    </ul></section>"""
    lines, seen = _extract_radancy_jobs(html, "Capital One", "https://x.com/search")
    assert seen == 3
    assert lines == ["**Capital One** — Data Intern — [Apply](https://x.com/job/a/1)"]
