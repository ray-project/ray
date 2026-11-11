"""HAProxy RSS before, during and after bursts of large request bodies.

With ingress-router body forwarding HAProxy buffers each request body (up to
its 8 MiB tune.bufsize) before routing. Sends bursts of concurrent POSTs whose
bodies are invalid JSON (rejected by the replica without tokenizing), and
reports the newest HAProxy process's RSS after each burst and after idling.

Usage: python large_body_rss.py --out FILE [--mib 4] [--concurrency 128] [--rounds 3]
"""
import argparse
import asyncio
import json
import subprocess
import time

import aiohttp

parser = argparse.ArgumentParser()
parser.add_argument("--out", required=True)
parser.add_argument("--url", default="http://127.0.0.1:8000/v1/completions")
parser.add_argument("--mib", type=float, default=4.0)
parser.add_argument("--concurrency", type=int, default=128)
parser.add_argument("--rounds", type=int, default=3)
parser.add_argument("--idle-s", type=float, default=30.0)
args = parser.parse_args()


def haproxy_rss_mb():
    newest = None
    for pid in subprocess.run(
        ["pgrep", "-x", "haproxy"], capture_output=True, text=True
    ).stdout.split():
        try:
            start = int(open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[19])
            rss = next(
                int(line.split()[1])
                for line in open(f"/proc/{pid}/status")
                if line.startswith("VmRSS")
            )
        except (OSError, StopIteration):
            continue
        if newest is None or start > newest[0]:
            newest = (start, int(pid), rss / 1024)
    return newest[1], round(newest[2], 1)


async def burst(session, body):
    async def one():
        async with session.post(
            args.url, data=body, headers={"content-type": "application/json"}
        ) as r:
            await r.read()
            return r.status

    return await asyncio.gather(
        *(one() for _ in range(args.concurrency)), return_exceptions=True
    )


async def main():
    body = b"x" * int(args.mib * 1024 * 1024)
    rows = [{"phase": "before", "pid_rss_mb": haproxy_rss_mb()}]
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(
        timeout=timeout, connector=aiohttp.TCPConnector(limit=0)
    ) as session:
        for i in range(args.rounds):
            t0 = time.time()
            statuses = await burst(session, body)
            codes = {}
            for s in statuses:
                key = s if isinstance(s, int) else type(s).__name__
                codes[key] = codes.get(key, 0) + 1
            rows.append(
                {
                    "phase": f"burst{i}",
                    "seconds": round(time.time() - t0, 1),
                    "statuses": codes,
                    "pid_rss_mb": haproxy_rss_mb(),
                }
            )
            print(rows[-1], flush=True)
    await asyncio.sleep(args.idle_s)
    rows.append({"phase": f"idle{args.idle_s:.0f}s", "pid_rss_mb": haproxy_rss_mb()})
    print(rows[0], rows[-1], flush=True)
    with open(args.out, "w") as f:
        json.dump(rows, f, indent=1)


asyncio.run(main())
