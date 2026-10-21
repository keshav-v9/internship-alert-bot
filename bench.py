"""Benchmark one or more full poll cycles and print latency/resource metrics.

Usage: python bench.py [cycles]    (default 3; cycle 1 seeds state, so it is
the "cold" run, later cycles are "warm"). Alerts are discarded and state is
written to a temp file, so running this never sends anything.
"""

import asyncio
import contextvars
import json
import resource
import statistics
import sys
import tempfile
import time
from pathlib import Path

import aiohttp
import yaml

import alerter as alerter_mod
from alerter import Alerter

stats = {"requests": 0, "bytes": 0}
per_source: dict[str, float] = {}
per_bytes: dict[str, int] = {}
current = contextvars.ContextVar("current", default="?")

_orig_check = Alerter._check_source


async def _timed_check(self, session, source, browser=None):
    current.set(source["name"])
    t0 = time.perf_counter()
    try:
        return await _orig_check(self, session, source, browser)
    finally:
        per_source[source["name"]] = per_source.get(source["name"], 0) + time.perf_counter() - t0


Alerter._check_source = _timed_check

_orig_init = aiohttp.ClientSession.__init__


def _traced_init(self, *a, **kw):
    tc = aiohttp.TraceConfig()

    async def on_start(session, ctx, params):
        stats["requests"] += 1

    async def on_chunk(session, ctx, params):
        stats["bytes"] += len(params.chunk)
        per_bytes[current.get()] = per_bytes.get(current.get(), 0) + len(params.chunk)

    tc.on_request_start.append(on_start)
    tc.on_response_chunk_received.append(on_chunk)
    kw["trace_configs"] = [*kw.get("trace_configs", []), tc]
    _orig_init(self, *a, **kw)


aiohttp.ClientSession.__init__ = _traced_init


async def main(cycles: int) -> None:
    config = yaml.safe_load(Path("config.yaml").read_text())
    state_path = Path(tempfile.mkdtemp()) / "state.json"

    async def discard(name, lines):
        pass

    alerter = Alerter(
        sources=config["sources"],
        on_update=discard,
        poll_interval_seconds=config.get("poll_interval_seconds", 300),
        state_path=str(state_path),
    )
    results = []
    for i in range(1, cycles + 1):
        per_source.clear()
        per_bytes.clear()
        stats.update(requests=0, bytes=0)
        t0 = time.perf_counter()
        await alerter.poll_once()
        wall = time.perf_counter() - t0
        slowest = sorted(per_source.items(), key=lambda kv: -kv[1])[:3]
        results.append(
            {
                "cycle": i,
                "wall_s": round(wall, 2),
                "sum_of_sources_s": round(sum(per_source.values()), 2),
                "requests": stats["requests"],
                "mb_downloaded": round(stats["bytes"] / 1e6, 2),
                "slowest": [(n, round(t, 1)) for n, t in slowest],
                "top_mb": [(n, round(b / 1e6, 1)) for n, b in sorted(per_bytes.items(), key=lambda kv: -kv[1])[:5]],
            }
        )
        print(json.dumps(results[-1]))
    warm = [r["wall_s"] for r in results[1:]] or [results[0]["wall_s"]]
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6  # bytes on macOS
    print(
        json.dumps(
            {
                "summary": True,
                "warm_cycle_median_s": round(statistics.median(warm), 2),
                "peak_rss_mb": round(rss_mb, 0),
                "state_kb": round(state_path.stat().st_size / 1024),
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 3))
