#!/usr/bin/env python3
"""Validate the bundled 120K-cap Claude Code trace subset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

MAX_CONTEXT_LENGTH = 120_000
EXCLUDED_TRACE_ID = "259d1cc35e11b72660b8d6a33867cbb93759"


def iter_calls(entries: list[dict]):
    for entry in entries:
        if "in" in entry:
            yield entry
        elif entry.get("type") == "subagent":
            yield from iter_calls(entry.get("requests") or [])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()

    manifest_path = args.data_dir / "MANIFEST.txt"
    if not manifest_path.is_file():
        raise SystemExit(f"missing selection manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    expected_ids = manifest.get("selected_trace_ids")
    if not isinstance(expected_ids, list) or len(expected_ids) != 71:
        raise SystemExit("manifest must contain exactly 71 selected_trace_ids")

    paths = sorted(args.data_dir.glob("*.json"))
    if len(paths) != 71:
        raise SystemExit(f"expected 71 JSON roots, found {len(paths)}")

    actual_ids: list[str] = []
    peak = 0
    request_count = 0
    for path in paths:
        trace = json.loads(path.read_text())
        trace_id = trace.get("id")
        if not isinstance(trace_id, str):
            raise SystemExit(f"{path}: missing trace id")
        if trace.get("block_size") != 64 or trace.get("hash_id_scope") != "local":
            raise SystemExit(f"{path}: expected local 64-token hash blocks")
        actual_ids.append(trace_id)
        for request in iter_calls(trace.get("requests") or []):
            try:
                requested_context = int(request["in"]) + max(1, int(request["out"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise SystemExit(f"{path}: invalid request token counts") from exc
            peak = max(peak, requested_context)
            request_count += 1

    if actual_ids != expected_ids:
        raise SystemExit("JSON trace order/IDs do not match MANIFEST.txt")
    if EXCLUDED_TRACE_ID in actual_ids:
        raise SystemExit("target-incompatible empty-content trace is present")
    if peak > MAX_CONTEXT_LENGTH:
        raise SystemExit(f"trace-wide peak {peak} exceeds {MAX_CONTEXT_LENGTH}")

    print(
        "[inputs] verified "
        f"roots={len(actual_ids)} requests={request_count} peak_in_plus_out={peak}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
