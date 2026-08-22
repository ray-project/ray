#!/usr/bin/env python3
"""Download and select the pinned Weka Claude Code traces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from datasets import load_dataset

from aiperf.dataset.loader.weka_trace import WekaTrace, _trace_peak_context_length


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text())
    dataset = str(manifest["source_dataset"])
    revision = str(manifest["source_revision"])
    max_context = int(manifest["max_context_length"])
    excluded_id = str(manifest["excluded_trace_id"])
    expected_ids = [str(trace_id) for trace_id in manifest["selected_trace_ids"]]
    expected_rows = int(manifest["source_rows"])
    if len(expected_ids) != int(manifest["selected_trace_count"]):
        raise SystemExit("selection manifest has an invalid trace count")
    if args.out.exists():
        raise SystemExit(f"refusing to overwrite existing trace directory: {args.out}")

    source = load_dataset(dataset, split="train", revision=revision)
    selected: list[dict[str, object]] = []
    for row in source:
        trace = WekaTrace.model_validate(row)
        if (
            trace.id == excluded_id
            or _trace_peak_context_length(trace, max_osl=None) > max_context
        ):
            continue
        selected.append(dict(row))

    selected_ids = [str(trace["id"]) for trace in selected]
    if len(source) != expected_rows:
        raise SystemExit(f"expected {expected_rows} source rows, found {len(source)}")
    if selected_ids != expected_ids:
        raise SystemExit("downloaded selection does not match the committed trace IDs")

    args.out.mkdir(parents=True)
    for index, trace in enumerate(selected):
        path = args.out / f"{index:03d}-{trace['id']}.json"
        path.write_text(json.dumps(trace))
    output_manifest = dict(manifest)
    output_manifest.pop("source_revision")
    (args.out / "MANIFEST.txt").write_text(json.dumps(output_manifest, indent=2) + "\n")
    print(
        f"[inputs] downloaded rows={len(source)} selected={len(selected)} revision={revision}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
