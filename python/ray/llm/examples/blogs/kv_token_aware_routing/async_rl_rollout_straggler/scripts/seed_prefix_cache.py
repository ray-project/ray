#!/usr/bin/env python
"""Warm the shared prompt prefix with a finite, unmeasured HTTP request set."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def load_requests(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    if len(rows) != 32:
        raise ValueError(f"expected 32 seed conversations, got {len(rows)}")
    requests = []
    for row in rows:
        turns = row.get("turns")
        if not isinstance(turns, list) or len(turns) != 1:
            raise ValueError("every seed conversation must contain one turn")
        turn = turns[0]
        if not isinstance(turn, dict) or not isinstance(turn.get("messages"), list):
            raise ValueError("seed turn is missing messages")
        requests.append(
            {
                "session_id": str(row["session_id"]),
                "messages": turn["messages"],
            }
        )
    return requests


def send_request(
    item: dict[str, Any], url: str, session_header: str, timeout_s: float
) -> dict[str, Any]:
    payload = json.dumps(
        {
            "model": "openai/gpt-oss-120b",
            "messages": item["messages"],
            "max_tokens": 1,
            "stream": False,
            "temperature": 0,
        }
    ).encode()
    request = urllib.request.Request(
        f"{url.rstrip('/')}/v1/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            session_header: item["session_id"],
        },
        method="POST",
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            body = json.loads(response.read())
            if response.status != 200 or not body.get("choices"):
                raise RuntimeError(
                    f"status={response.status}, choices={bool(body.get('choices'))}"
                )
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{item['session_id']}: {exc}") from exc
    return {"session_id": item["session_id"], "latency_s": time.monotonic() - started}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-file", required=True, type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--session-header", required=True)
    parser.add_argument("--concurrency", required=True, type=int)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be positive")
    if args.out.exists():
        raise SystemExit(f"refusing to overwrite {args.out}")

    requests = load_requests(args.input_file)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        futures = [
            executor.submit(send_request, item, args.url, args.session_header, 300.0)
            for item in requests
        ]
        results = [future.result() for future in futures]
    summary = {
        "request_count": len(results),
        "max_latency_s": round(max(result["latency_s"] for result in results), 3),
    }
    args.out.write_text(json.dumps(summary, indent=2) + "\n")
    print(
        f"[seed] warmed {summary['request_count']} requests; max_latency_s={summary['max_latency_s']}"
    )


if __name__ == "__main__":
    main()
