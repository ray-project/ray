"""In-cluster client for the KubeRay high-availability test in serve_ha.py.

`load` sends a steady stream of requests to serve_ha_app.py through the
RayService's Serve Service. Like a typical client, it opens a new connection
for each attempt and retries failed attempts with backoff. Every second, it
prints one JSON line counting the requests that finished in that second:

    {"t": 1700000000, "ok": 48, "retried": 1}

`retried` counts failed attempts that were retried, `failed` counts requests
that failed every attempt, and `wrong` counts responses with an unexpected
body. Up to `--max-errors-per-s` failed attempts and wrong responses a second
are also printed as {"t": ..., "error": "..."}.

`details` prints the Serve REST API's response under "serve" and Ray's nodes
under "nodes".
"""

import argparse
import collections
import json
import os
import random
import threading
import time
import urllib.request
from typing import Optional

SERVE_URL = "http://serve-ha-serve-svc:8000/"
DASHBOARD_URL = "http://serve-ha-head-svc:8265"


class Stats:
    """Counts finished requests by the second they finished in."""

    def __init__(self, max_errors_per_s: int):
        self._max_errors_per_s = max_errors_per_s
        self._lock = threading.Lock()
        self._counts = collections.defaultdict(collections.Counter)
        self._errors = collections.defaultdict(list)

    def add(self, key: str, error: Optional[str] = None):
        with self._lock:
            t = int(time.time())
            self._counts[t][key] += 1
            if error is not None and len(self._errors[t]) < self._max_errors_per_s:
                self._errors[t].append(error)

    def report_forever(self):
        while True:
            time.sleep(1)
            lines = []
            with self._lock:
                for t in sorted(t for t in self._counts if t < int(time.time())):
                    lines.append({"t": t, **self._counts.pop(t)})
                    lines.extend({"t": t, "error": e} for e in self._errors.pop(t, []))
            for line in lines:
                print(json.dumps(line), flush=True)


def send_request(args: argparse.Namespace, stats: Stats):
    val = random.randrange(1000)
    expected = str(3 * val + 6)
    for attempt in range(args.attempts):
        if attempt > 0:
            time.sleep(args.backoff_s * 2 ** (attempt - 1))
        try:
            with urllib.request.urlopen(
                f"{SERVE_URL}?val={val}", timeout=args.timeout_s
            ) as response:
                body = response.read().decode()
        except Exception as e:
            last = attempt == args.attempts - 1
            stats.add("failed" if last else "retried", repr(e))
            continue
        if body == expected:
            stats.add("ok")
        else:
            stats.add("wrong", f"val={val}: expected {expected}, got {body!r}")
        return


def send_forever(args: argparse.Namespace, stats: Stats):
    interval_s = 1 / args.qps_per_sender
    while True:
        start = time.monotonic()
        send_request(args, stats)
        time.sleep(max(0, interval_s - (time.monotonic() - start)))


def load(args: argparse.Namespace):
    stats = Stats(args.max_errors_per_s)
    for _ in range(args.senders):
        threading.Thread(target=send_forever, args=(args, stats), daemon=True).start()
    stats.report_forever()


def get_from_dashboard(path: str) -> dict:
    request = urllib.request.Request(
        DASHBOARD_URL + path,
        headers={"Authorization": f"Bearer {os.environ['RAY_AUTH_TOKEN']}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def details(args: argparse.Namespace):
    nodes = get_from_dashboard("/api/v0/nodes?limit=1000")["data"]["result"]["result"]
    serve = get_from_dashboard("/api/serve/applications/")
    print(json.dumps({"serve": serve, "nodes": nodes}))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(required=True)
    load_parser = subparsers.add_parser("load")
    load_parser.set_defaults(func=load)
    load_parser.add_argument("--senders", type=int, default=10)
    load_parser.add_argument("--qps-per-sender", type=float, default=5)
    load_parser.add_argument("--timeout-s", type=float, default=5)
    load_parser.add_argument("--attempts", type=int, default=3)
    load_parser.add_argument("--backoff-s", type=float, default=0.5)
    load_parser.add_argument("--max-errors-per-s", type=int, default=5)
    subparsers.add_parser("details").set_defaults(func=details)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
