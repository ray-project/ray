"""Open-loop load generator with a piecewise time-varying offered rate.

Profile is a comma-separated list of segments "dur:rate_start:rate_end"; the
offered rate is linearly interpolated within each segment. Each request gets a
distinct video_id (real work). Records per-request ack latency + success.
"""

import argparse
import asyncio
import csv
import time

import aiohttp


def parse_profile(s):
    segs = []
    for part in s.split(","):
        d, r0, r1 = part.split(":")
        segs.append((float(d), float(r0), float(r1)))
    return segs


def rate_at(t, segs):
    acc = 0.0
    for d, r0, r1 in segs:
        if t < acc + d:
            frac = (t - acc) / d if d > 0 else 0.0
            return r0 + (r1 - r0) * frac
        acc += d
    return 0.0


async def submit(session, url, vuri, vid, timeout, out):
    t0 = time.time()
    try:
        async with session.post(
            url + "/index",
            json={"video_uri": vuri, "video_id": vid},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as r:
            await r.read()
            ok = r.status == 200
        out.append((vid, t0, round((time.time() - t0) * 1000, 1), ok, ""))
    except Exception as e:
        out.append((vid, t0, round((time.time() - t0) * 1000, 1), False, type(e).__name__))


async def run(args):
    segs = parse_profile(args.profile)
    total = sum(d for d, _, _ in segs)
    results = []
    counter = 0
    connector = aiohttp.TCPConnector(limit=args.max_inflight, limit_per_host=args.max_inflight)
    async with aiohttp.ClientSession(connector=connector) as session:
        pending = set()
        t0 = time.time()
        tick, carry = 0.1, 0.0
        while time.time() - t0 < total:
            rate = rate_at(time.time() - t0, segs)
            want = rate * tick + carry
            launch = int(want)
            carry = want - launch
            for _ in range(launch):
                t = asyncio.create_task(
                    submit(session, args.url, args.video_uri,
                           f"{args.id_prefix}-{counter}", args.timeout, results)
                )
                counter += 1
                pending.add(t)
                t.add_done_callback(pending.discard)
            await asyncio.sleep(tick)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["video_id", "enqueue_time", "ack_latency_ms", "ok", "error"])
        w.writerows(results)
    ok = sum(1 for r in results if r[3])
    print(f"sent {counter} ({ok} ok, {counter - ok} failed) over {total:.0f}s -> {args.out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--video-uri", required=True)
    ap.add_argument("--profile", required=True, help="dur:r0:r1,dur:r0:r1,...")
    ap.add_argument("--id-prefix", default="phased")
    ap.add_argument("--max-inflight", type=int, default=2000)
    ap.add_argument("--timeout", type=float, default=120)
    ap.add_argument("--out", default="phased.csv")
    asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    main()
