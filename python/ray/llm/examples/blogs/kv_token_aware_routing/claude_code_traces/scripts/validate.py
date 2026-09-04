#!/usr/bin/env python3
"""Validate one raw CC router benchmark cell."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

ROUTER_VARIANTS = (
    "session-affinity",
    "kv-token-aware-balanced",
    "kv-token-aware-kv-biased",
)


def jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open() as file:
        for line in file:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value


def metric(record: dict[str, Any], name: str) -> Any:
    value = (record.get("metrics") or {}).get(name)
    return value.get("value") if isinstance(value, dict) else value


def profile_path(cell: Path) -> Path | None:
    candidates = list(cell.glob("aiperf_artifacts/**/profile_export.jsonl"))
    return next((path for path in candidates if path.is_file()), None)


def validate_cell(cell: Path, router_variant: str) -> list[str]:
    """Return every failed invariant for a completed cell."""

    failures: list[str] = []
    try:
        metadata = json.loads((cell / "meta.json").read_text())
    except (OSError, json.JSONDecodeError) as error:
        return [f"cannot read metadata: {error}"]

    def require(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    require(metadata.get("aiperf_rc") == 0, "AIPerf did not exit successfully")
    profile = profile_path(cell)
    if profile is None:
        return [*failures, "missing AIPerf profile export"]
    requests = [
        record
        for record in jsonl(profile)
        if (record.get("metadata") or {}).get("benchmark_phase") == "profiling"
    ]
    require(bool(requests), "no profiling requests")
    request_ids: set[str] = set()
    cache_telemetry_seen = False
    for request in requests:
        request_metadata = request.get("metadata") or {}
        require(not request_metadata.get("was_cancelled"), "cancelled request")
        require(not request.get("error"), "failed request")
        input_tokens = metric(request, "input_sequence_length")
        require(
            isinstance(input_tokens, (int, float)) and 0 < input_tokens <= 120000,
            "invalid input length",
        )
        require(
            isinstance(metric(request, "usage_prompt_tokens"), (int, float)),
            "request is missing prompt-token telemetry",
        )
        cache_telemetry_seen |= isinstance(
            metric(request, "usage_prompt_cache_read_tokens"), (int, float)
        )
        request_id = request_metadata.get("x_request_id")
        require(bool(request_id), "request is missing X-Request-ID")
        if request_id:
            request_ids.add(str(request_id))
    require(cache_telemetry_seen, "no request reported prompt-cache telemetry")

    routes = [
        route
        for path in sorted((cell / "routing").glob("routing.*.jsonl"))
        for route in jsonl(path)
        if str(route.get("request_id")) in request_ids
    ]
    covered_ids = {str(route.get("request_id")) for route in routes}
    require(covered_ids == request_ids, "route log does not cover every profiling request")
    require(all(route.get("session_id") for route in routes), "route is missing a session")
    require(all(route.get("replica_id") for route in routes), "route is missing a replica")
    require(
        all(route.get("n_candidates") == 4 for route in routes), "wrong replica candidate count"
    )

    placements: dict[str, set[str]] = defaultdict(set)
    for route in routes:
        session_id, replica_id = route.get("session_id"), route.get("replica_id")
        if session_id and replica_id:
            placements[str(session_id)].add(str(replica_id))
    require(bool(placements), "no routed sessions")
    if router_variant == "session-affinity":
        require(
            all(len(replicas) == 1 for replicas in placements.values()),
            "hash affinity was violated",
        )
    else:
        require(
            all(route.get("kv_tracker_present") is True for route in routes),
            "KV tracker was unavailable",
        )
        require(
            all(
                isinstance(route.get("kv_token_count"), int) and route["kv_token_count"] > 0
                for route in routes
            ),
            "KVAwareRouter did not receive token IDs",
        )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cell", type=Path, required=True)
    parser.add_argument("--router-variant", choices=ROUTER_VARIANTS, required=True)
    args = parser.parse_args()

    failures = validate_cell(args.cell, args.router_variant)
    metadata_path = args.cell / "meta.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["raw_validation_passed"] = not failures
    metadata["raw_validation_errors"] = failures
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    if failures:
        print(f"FAIL {args.cell}")
        print("\n".join(f"- {failure}" for failure in failures))
        return 1
    print(f"PASS {args.cell}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
