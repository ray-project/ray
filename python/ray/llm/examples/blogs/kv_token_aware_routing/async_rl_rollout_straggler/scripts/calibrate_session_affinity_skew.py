#!/usr/bin/env python
"""Calibrate unique straggler keys onto one live ConsistentHash replica."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import pathlib
import time
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any

MODEL = "openai/gpt-oss-120b"
ROLLOUTS_PER_STEP = 8
LONG_ROLLOUTS = {0, 4}


def jsonl_rows(path: pathlib.Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.open() if line.strip()]


def write_jsonl(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def correlation_id(session_id: str) -> str:
    """Mirror FixedScheduleStrategy's stable routing key."""
    return f"fixed-schedule-{session_id}"


def probe(url: str, corr: str, ordinal: int) -> tuple[str, str | None]:
    request_id = f"async-rl-session-affinity-calibration-{ordinal:05d}"
    body = json.dumps(
        {
            "model": MODEL,
            "stream": False,
            "messages": [
                {"role": "system", "content": "routing calibration only"},
                {"role": "user", "content": "return one token"},
            ],
            "max_completion_tokens": 1,
            "min_tokens": 1,
            "ignore_eos": True,
            "temperature": 0,
        }
    ).encode()
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Correlation-ID": corr,
            "X-Request-ID": request_id,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            response.read()
        return corr, None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return corr, str(exc)


def routing_rows(directory: pathlib.Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    for path in directory.glob("routing.*.jsonl"):
        for line in path.open():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            session_id, replica_id = row.get("session_id"), row.get("replica_id")
            if session_id and replica_id:
                rows[str(session_id)] = str(replica_id)
    return rows


def replace_strings(value: Any, old: str, new: str) -> Any:
    if isinstance(value, str):
        return value.replace(old, new)
    if isinstance(value, list):
        return [replace_strings(item, old, new) for item in value]
    if isinstance(value, dict):
        return {key: replace_strings(item, old, new) for key, item in value.items()}
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cell-dir", required=True, type=pathlib.Path)
    parser.add_argument("--workload-dir", required=True, type=pathlib.Path)
    parser.add_argument("--expected-replicas", type=int, default=2)
    parser.add_argument("--candidates-per-straggler", type=int, default=48)
    parser.add_argument("--probe-concurrency", type=int, default=32)
    parser.add_argument("--url", default="http://localhost:8000/v1/chat/completions")
    args = parser.parse_args()

    if args.candidates_per_straggler < 8:
        raise SystemExit("--candidates-per-straggler must be at least 8")
    if args.expected_replicas < 2:
        raise SystemExit("--expected-replicas must be at least 2")
    if not (args.cell_dir / "routing").is_dir():
        raise SystemExit("session-affinity route-log directory is absent")

    workload_dir = args.workload_dir.resolve()
    base_dag = workload_dir / "async_rl_rollouts.base.jsonl"
    base_manifest = workload_dir / "manifest.base.json"
    if not base_dag.exists() or not base_manifest.exists():
        raise SystemExit(
            "calibrated workload requires immutable .base DAG and manifest"
        )
    rows = jsonl_rows(base_dag)
    manifest = json.loads(base_manifest.read_text())
    if manifest.get("schema") not in {
        "async-rl-rollout-load-balance-v1",
        "async-rl-rollout-cache-load-causal-v1",
    }:
        raise SystemExit(
            f"unexpected async RL workload schema={manifest.get('schema')!r}"
        )
    fixed_schedule = (manifest.get("timing_contract") or {}).get(
        "mode"
    ) == "AIPerf fixed_schedule; root turn-0 timestamps are absolute and auto-offset to zero"
    if not fixed_schedule:
        raise SystemExit("session-affinity calibration requires fixed-schedule replay")

    stragglers: list[dict[str, Any]] = []
    for ordinal, row in enumerate(rows):
        session_id = str(row.get("session_id") or "")
        try:
            step_prefix, rollout_text = session_id.rsplit("-rollout-", 1)
            step = int(step_prefix.rsplit("-step-", 1)[1])
            rollout = int(rollout_text)
        except (IndexError, ValueError):
            raise SystemExit(f"malformed rollout session ID {session_id!r}") from None
        if rollout in LONG_ROLLOUTS:
            stragglers.append(
                {
                    "ordinal": ordinal,
                    "step": step,
                    "rollout": rollout,
                    "old_id": session_id,
                }
            )
    steps = int(manifest["steps"])
    if len(rows) != steps * ROLLOUTS_PER_STEP or len(stragglers) != steps * 2:
        raise SystemExit("unexpected rollout/straggler topology in base DAG")

    candidates: list[dict[str, Any]] = []
    for spec in stragglers:
        for candidate_index in range(args.candidates_per_straggler):
            session_id = (
                f"rl-rollout-step-{spec['step']}-rollout-{spec['rollout']}"
                f"-cal-{candidate_index:03d}"
            )
            candidates.append(
                {
                    **spec,
                    "candidate_index": candidate_index,
                    "session_id": session_id,
                    "correlation_id": correlation_id(session_id),
                }
            )

    failures: dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.probe_concurrency
    ) as pool:
        futures = {
            pool.submit(probe, args.url, candidate["correlation_id"], index): candidate
            for index, candidate in enumerate(candidates)
        }
        for future in concurrent.futures.as_completed(futures):
            corr, failure = future.result()
            if failure:
                failures[corr] = failure
    if failures:
        example = next(iter(failures.items()))
        raise SystemExit(
            f"{len(failures)} session-affinity probes failed; example={example}"
        )

    expected_corrs = {candidate["correlation_id"] for candidate in candidates}
    placements: dict[str, str] = {}
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        all_routes = routing_rows(args.cell_dir / "routing")
        placements = {
            corr: replica
            for corr, replica in all_routes.items()
            if corr in expected_corrs
        }
        if len(placements) == len(expected_corrs):
            break
        time.sleep(0.2)
    if len(placements) != len(expected_corrs):
        raise SystemExit(
            f"routing log missed calibration probes: {len(placements)}/{len(expected_corrs)}"
        )

    candidates_by_slot: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        candidate["replica_id"] = placements[candidate["correlation_id"]]
        candidates_by_slot[(candidate["step"], candidate["rollout"])].append(candidate)
    replicas = sorted(set(placements.values()))
    if len(replicas) != args.expected_replicas:
        raise SystemExit(
            "calibration expected "
            f"{args.expected_replicas} session-affinity replicas, got {replicas}"
        )

    # Choose one feasible static-affinity target for all unique stragglers.
    feasible: list[tuple[int, str]] = []
    for replica in replicas:
        per_slot = [
            sum(
                candidate["replica_id"] == replica
                for candidate in candidates_by_slot[slot]
            )
            for slot in sorted(candidates_by_slot)
        ]
        if min(per_slot) > 0:
            feasible.append((sum(per_slot), replica))
    if not feasible:
        raise SystemExit("no live replica has a candidate for every straggler slot")
    target_replica = max(feasible)[1]

    selected: list[dict[str, Any]] = []
    for slot in sorted(candidates_by_slot):
        options = [
            candidate
            for candidate in candidates_by_slot[slot]
            if candidate["replica_id"] == target_replica
        ]
        selected.append(
            min(options, key=lambda candidate: candidate["candidate_index"])
        )

    rewritten = list(rows)
    for candidate in selected:
        ordinal = int(candidate["ordinal"])
        old_id, new_id = str(candidate["old_id"]), str(candidate["session_id"])
        rewritten[ordinal] = replace_strings(rewritten[ordinal], old_id, new_id)
        if rewritten[ordinal].get("session_id") != new_id:
            raise SystemExit("failed to rewrite calibrated session ID")

    output_dag = workload_dir / "async_rl_rollouts.dag.jsonl"
    write_jsonl(output_dag, rewritten)
    plan = {
        "schema": "async-rl-session-affinity-skew-plan-v1",
        "aiperf_routing_key_mode": "fixed-schedule-session-key",
        "candidate_count": len(candidates),
        "candidates_per_straggler": args.candidates_per_straggler,
        "target_replica": target_replica,
        "selected_straggler_count": len(selected),
        "selected": selected,
        "reason": (
            "All selected long rollouts have globally unique stable session IDs, "
            "but their deterministic wire session keys map to one live ConsistentHash replica. "
            "KVAwareRouter consumes the same IDs and can trade one private prefix tail "
            "for lower observed decode-block load."
        ),
    }
    manifest["files"]["dag"]["sha256"] = sha256(output_dag)
    manifest["session_affinity_skew_contract"] = plan
    (workload_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    audit = {
        **plan,
        "candidate_replica_counts": {
            replica: sum(candidate["replica_id"] == replica for candidate in candidates)
            for replica in replicas
        },
        "profile_dag_sha256": sha256(output_dag),
        "base_dag_sha256": sha256(base_dag),
    }
    (args.cell_dir / "session_affinity_skew_calibration.json").write_text(
        json.dumps(audit, indent=2) + "\n"
    )
    print(
        "[async-rl-session-affinity-skew] calibrated "
        f"{len(selected)} globally unique stragglers to replica {target_replica}; "
        f"candidate_counts={audit['candidate_replica_counts']}"
    )


if __name__ == "__main__":
    main()
