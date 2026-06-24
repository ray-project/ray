"""Render benchmark result JSONs into an llm-foundry-style comparison table.

Each run writes <name>_results.json (see core/sinks.py). This aggregates one or
more of them into a single table for eyeballing or pasting into the benchmark
doc. Stdlib-only so it runs anywhere, no torch/ray needed.

Usage:
    # default: every *_results.json under the usual result dirs
    python collect.py
    # explicit files / globs
    python collect.py /mnt/cluster_storage/qwen3_06b_deepspeed_results.json
    python collect.py --format csv > results.csv
"""

import argparse
import csv
import glob
import io
import json
import os
import sys
from typing import Any, Dict, List

# Default locations core/sinks.py writes to.
_DEFAULT_GLOBS = [
    "/mnt/cluster_storage/*_results.json",
    "/tmp/train_benchmark/*_results.json",
]


# (header, metric key, formatter). Columns mirror the llm-foundry benchmark
# tables: identity, parallelism, then throughput / efficiency / memory.
def _g(x: float) -> str:
    return f"{x:.2f}"


_COLUMNS = [
    ("experiment", "experiment", str),
    ("model", "config/model", str),
    ("gpu", "config/gpu", str),
    ("gpus", "world_size", lambda v: str(int(v))),
    ("prec", "config/precision", str),
    ("zero", "config/zero_stage", lambda v: f"z{int(v)}" if v is not None else "-"),
    ("grad_ckpt", "config/gradient_checkpointing", lambda v: "Y" if v else "N"),
    ("seq", "config/seq_len", lambda v: str(int(v))),
    ("mbs", "config/micro_batch_size", lambda v: str(int(v))),
    ("gbs", "config/global_batch_size", lambda v: str(int(v))),
    ("tok/s", "train/global_tokens_per_sec", lambda v: f"{v:,.0f}"),
    ("tok/s/gpu", "train/tokens_per_sec_per_device", lambda v: f"{v:,.0f}"),
    ("MFU%", "train/mfu", lambda v: f"{v * 100:.1f}"),
    ("TFLOP/s/gpu", "train/model_tflops_per_sec_per_device", _g),
    ("util%", "gpu/utilization_mean_pct", _g),
    ("mem_bw%", "gpu/memory_bw_util_mean_pct", _g),
    ("peak_alloc_GB", "gpu/peak_memory_allocated_gb", _g),
    ("peak_resv_GB", "gpu/peak_memory_reserved_gb", _g),
    ("static_GB", "gpu/static_memory_gb", _g),
    ("act_GB", "gpu/activation_memory_gb", _g),
    ("step_s", "train/step_time_mean_s", lambda v: f"{v:.3f}"),
]


def _format_cell(row: Dict[str, Any], key: str, fmt) -> str:
    if key not in row or row[key] is None:
        return "-"
    try:
        return fmt(row[key])
    except (TypeError, ValueError):
        return str(row[key])


def _load(paths: List[str]) -> List[Dict[str, Any]]:
    rows = []
    for path in paths:
        try:
            with open(path) as f:
                rows.append(json.load(f))
        except (OSError, json.JSONDecodeError) as e:
            print(f"skipping {path}: {e}", file=sys.stderr)
    return rows


def _resolve_inputs(inputs: List[str]) -> List[str]:
    globs = inputs or _DEFAULT_GLOBS
    paths: List[str] = []
    for pattern in globs:
        paths.extend(
            sorted(glob.glob(pattern))
            if any(c in pattern for c in "*?[")
            else [pattern]
        )
    # De-dup while preserving order.
    seen = set()
    return [p for p in paths if not (p in seen or seen.add(p)) and os.path.isfile(p)]


def _render_markdown(rows: List[Dict[str, Any]]) -> str:
    headers = [c[0] for c in _COLUMNS]
    table = [[_format_cell(r, key, fmt) for _, key, fmt in _COLUMNS] for r in rows]
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in table))
        if table
        else len(headers[i])
        for i in range(len(headers))
    ]

    def fmt_row(cells: List[str]) -> str:
        return "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells)) + " |"

    sep = "| " + " | ".join("-" * widths[i] for i in range(len(headers))) + " |"
    return "\n".join([fmt_row(headers), sep, *(fmt_row(r) for r in table)])


def _render_csv(rows: List[Dict[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([c[0] for c in _COLUMNS])
    for r in rows:
        writer.writerow([_format_cell(r, key, fmt) for _, key, fmt in _COLUMNS])
    return buf.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", help="Result JSON files or globs.")
    parser.add_argument("--format", choices=["markdown", "csv"], default="markdown")
    args = parser.parse_args()

    paths = _resolve_inputs(args.inputs)
    if not paths:
        print("No result JSON files found.", file=sys.stderr)
        sys.exit(1)

    rows = _load(paths)
    if not rows:
        sys.exit(1)

    if args.format == "csv":
        print(_render_csv(rows), end="")
    else:
        print(_render_markdown(rows))


if __name__ == "__main__":
    main()
