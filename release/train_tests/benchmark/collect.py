"""Render benchmark result JSONs into an llm-foundry-style comparison table.

Each run writes <name>_results.json (see runner.write_results). This aggregates one or
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

# Default locations runner.write_results writes to.
_DEFAULT_GLOBS = [
    "/mnt/cluster_storage/*_results.json",
    "/tmp/train_benchmark/*_results.json",
]


# Columns are (header, metric key, formatter, default). `default` is used when
# the key is absent — e.g. DeepSpeed (pure data-parallel) has no TP/PP/CP, which
# are 1 in Megatron terms. Default None renders as "-".
def _g(x: float) -> str:
    return f"{x:.2f}"


def _int(v) -> str:
    return str(int(v))


# Default "full" view: identity, parallelism, throughput, efficiency, memory.
_COLUMNS = [
    ("experiment", "experiment", str, None),
    ("model", "config/model", str, None),
    ("gpu", "config/gpu", str, None),
    ("gpus", "world_size", _int, None),
    ("prec", "config/precision", str, None),
    ("zero", "config/zero_stage", lambda v: f"z{int(v)}", None),
    ("grad_ckpt", "config/gradient_checkpointing", lambda v: "Y" if v else "N", None),
    ("seq", "config/seq_len", _int, None),
    ("mbs", "config/micro_batch_size", _int, None),
    ("gbs", "config/global_batch_size", _int, None),
    ("tok/s", "train/global_tokens_per_sec", lambda v: f"{v:,.0f}", None),
    ("tok/s/gpu", "train/tokens_per_sec_per_device", lambda v: f"{v:,.0f}", None),
    ("MFU%", "train/mfu", lambda v: f"{v * 100:.1f}", None),
    ("TFLOP/s/gpu", "train/model_tflops_per_sec_per_device", _g, None),
    ("util%", "gpu/utilization_mean_pct", _g, None),
    ("mem_bw%", "gpu/memory_bw_util_mean_pct", _g, None),
    ("peak_alloc_GB", "gpu/peak_memory_allocated_gb", _g, None),
    ("peak_resv_GB", "gpu/peak_memory_reserved_gb", _g, None),
    ("static_GB", "gpu/static_memory_gb", _g, None),
    ("act_GB", "gpu/activation_memory_gb", _g, None),
    ("step_s", "train/step_time_mean_s", lambda v: f"{v:.3f}", None),
]

# "megatron" view: the exact 13 columns of the NVIDIA NeMo/Megatron-bridge
# performance-summary tables, so our rows sit directly next to theirs. DeepSpeed
# ZeRO is pure data-parallel, so TP/PP/CP/VP/EP default to 1 (VP shown as "-").
_MEGATRON_COLUMNS = [
    ("System", "config/model", str, None),
    ("#-GPUs", "world_size", _int, None),
    ("Precision", "config/precision", str, None),
    ("GBS", "config/global_batch_size", _int, None),
    ("MBS", "config/micro_batch_size", _int, None),
    ("Sequence Length", "config/seq_len", _int, None),
    ("TP", "config/tp", _int, 1),
    ("PP", "config/pp", _int, 1),
    ("CP", "config/cp", _int, 1),
    ("VP", "config/vp", _int, "-"),
    ("EP", "config/ep", _int, 1),
    (
        "Tokens / sec / GPU",
        "train/tokens_per_sec_per_device",
        lambda v: f"{v:,.0f}",
        None,
    ),
    ("Model TFLOP / sec / GPU", "train/model_tflops_per_sec_per_device", _g, None),
]

_VIEWS = {"full": _COLUMNS, "megatron": _MEGATRON_COLUMNS}


def _format_cell(row: Dict[str, Any], column) -> str:
    _, key, fmt, default = column
    value = row.get(key, default)
    if value is None:
        return "-"
    if value == default and key not in row:
        # Use the default verbatim if it's already display-ready (e.g. "-").
        if isinstance(default, str):
            return default
    try:
        return fmt(value)
    except (TypeError, ValueError):
        return str(value)


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


def _render_markdown(rows: List[Dict[str, Any]], columns) -> str:
    headers = [c[0] for c in columns]
    table = [[_format_cell(r, col) for col in columns] for r in rows]
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


def _render_csv(rows: List[Dict[str, Any]], columns) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([c[0] for c in columns])
    for r in rows:
        writer.writerow([_format_cell(r, col) for col in columns])
    return buf.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", help="Result JSON files or globs.")
    parser.add_argument("--format", choices=["markdown", "csv"], default="markdown")
    parser.add_argument(
        "--view",
        choices=list(_VIEWS),
        default="full",
        help="'full' (all harness metrics) or 'megatron' (the 13 NVIDIA "
        "NeMo/Megatron-bridge perf-summary columns, for direct comparison).",
    )
    args = parser.parse_args()

    paths = _resolve_inputs(args.inputs)
    if not paths:
        print("No result JSON files found.", file=sys.stderr)
        sys.exit(1)

    rows = _load(paths)
    if not rows:
        sys.exit(1)

    columns = _VIEWS[args.view]
    if args.format == "csv":
        print(_render_csv(rows, columns), end="")
    else:
        print(_render_markdown(rows, columns))


if __name__ == "__main__":
    main()
