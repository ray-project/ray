"""Shared utilities for the regression-repro benchmarks.

Each bench in this directory engages a regime that the cross-version diff
(91513 glia/m6_fixed vs 91797 master) implicates as a regression. Bench
goal: reproduce the wall-time gap on a single 24 vCPU devpod so that
rebasing m6_fixed onto master can be validated by re-running the bench
and confirming the gap closes.

The bench reports:
- wall_seconds_mean over N reps (1 unrecorded warmup)
- per-operator metrics taken from Dataset.stats() and the operator
  hierarchy
- the operator graph (string form) so we can detect op-fusion
  differences directly
"""

from __future__ import annotations

import json
import os
import re
import statistics
import time
from typing import Any, Callable, Dict, List, Optional

import ray


def _extract_op_metrics(stats_text: str) -> Dict[str, Dict[str, Any]]:
    """Parse Dataset.stats() output into per-operator dict.

    `stats_text` looks like:
        Operator 1 ReadParquet: N tasks executed, N blocks produced in T s
        * Remote wall time: ...
        * Operator throughput: ...
    We return a dict keyed by op label with whatever fields we can find.
    """
    out: Dict[str, Dict[str, Any]] = {}
    cur_label: Optional[str] = None
    cur: Dict[str, Any] = {}

    for line in stats_text.splitlines():
        m = re.match(r"^Operator \d+ (.+?):\s+(\d+) tasks executed,\s+(\d+) blocks produced in ([\d.]+)s", line)
        if m:
            if cur_label is not None:
                out[cur_label] = cur
            cur_label = m.group(1).strip()
            cur = {
                "num_tasks_executed": int(m.group(2)),
                "num_blocks": int(m.group(3)),
                "operator_seconds": float(m.group(4)),
            }
            continue
        if cur_label is None:
            continue
        # Remote wall time line
        rwt = re.match(r"^\* Remote wall time: [\d.]+\S+ min, [\d.]+\S+ max, [\d.]+\S+ mean, ([\d.]+)\S+ total", line)
        if rwt:
            cur["remote_wall_total_s"] = float(rwt.group(1))
            continue
        # UDF time
        udf = re.match(r"^\* UDF time: [\d.]+\S+ min, [\d.]+\S+ max, [\d.]+\S+ mean, ([\d.]+)\S+ total", line)
        if udf:
            cur["udf_total_s"] = float(udf.group(1))
            continue
        # Throughput rows/s
        thr = re.match(r"^\s*\* Ray Data throughput: ([\d.]+) rows/s", line)
        if thr:
            cur["throughput_rows_s"] = float(thr.group(1))

    if cur_label is not None:
        out[cur_label] = cur
    return out


def _run_once(
    init_kwargs: Dict[str, Any],
    workload: Callable[[], Any],
) -> Dict[str, Any]:
    """Run workload(); time wall; capture exec_plan + op metrics from stats.

    The workload should call .stats() on its main Dataset (or return one)
    so we can introspect it. By convention we use a global to pass the
    Dataset back out.
    """
    if not ray.is_initialized():
        ray.init(**init_kwargs)

    t0 = time.perf_counter()
    ret = workload()
    wall_s = time.perf_counter() - t0

    # The workload returns either a Dataset (for stats) or a dict {ds: ..., n: ...}
    if isinstance(ret, dict):
        ds = ret.get("ds")
    else:
        ds = ret

    exec_plan = None
    op_metrics: Dict[str, Dict[str, Any]] = {}
    if ds is not None and hasattr(ds, "stats"):
        try:
            stats_text = ds.stats()
            # Operator graph is the first line of each "Operator N <label>:" — assemble.
            ops_in_order = re.findall(r"^Operator \d+ (.+?):", stats_text, re.MULTILINE)
            exec_plan = " -> ".join(ops_in_order)
            op_metrics = _extract_op_metrics(stats_text)
        except Exception as e:
            exec_plan = f"<stats() failed: {e}>"

    return {
        "wall_s": wall_s,
        "exec_plan": exec_plan,
        "op_metrics": op_metrics,
    }


def run_bench(
    name: str,
    init_kwargs: Dict[str, Any],
    workload: Callable[[], Any],
    n_reps: int = 3,
    warmup: bool = True,
) -> Dict[str, Any]:
    """Run a workload under a fresh Ray init, with optional warmup.

    `workload` is a callable that runs the pipeline and returns the
    final Dataset (or dict with key 'ds').
    """
    print(f"\n========== {name} ==========")
    print(f"  ray.__file__ = {ray.__file__}")
    print(f"  ray.__version__ = {ray.__version__}")
    print(f"  ray.__commit__ = {getattr(ray, '__commit__', 'MISSING')[:10]}")
    print(f"  init_kwargs = {init_kwargs}")

    if warmup:
        print("  warmup ...")
        _run_once(init_kwargs, workload)

    reps: List[Dict[str, Any]] = []
    for i in range(n_reps):
        rep = _run_once(init_kwargs, workload)
        reps.append(rep)
        plan_str = (rep["exec_plan"] or "")[:160]
        print(f"  rep {i + 1}/{n_reps}: {rep['wall_s']:.2f}s  plan={plan_str!r}")

    walls = [r["wall_s"] for r in reps]
    summary = {
        "name": name,
        "ray_version": ray.__version__,
        "ray_commit": getattr(ray, "__commit__", "MISSING")[:10],
        "n_reps": n_reps,
        "wall_seconds_mean": statistics.mean(walls),
        "wall_seconds_stdev": statistics.stdev(walls) if len(walls) > 1 else 0.0,
        "wall_seconds_min": min(walls),
        "wall_seconds_max": max(walls),
        "exec_plan": reps[-1]["exec_plan"],
        "op_metrics_last_rep": reps[-1]["op_metrics"],
        "all_walls": walls,
    }

    print(f"\n=== {name} summary ===")
    print(f"  wall_mean: {summary['wall_seconds_mean']:.2f}s  stdev: {summary['wall_seconds_stdev']:.2f}s")
    print(f"  walls: {[f'{w:.2f}' for w in walls]}")
    print(f"  exec_plan: {summary['exec_plan']}")
    print(f"  op_metrics_last_rep:")
    for op, m in summary["op_metrics_last_rep"].items():
        print(f"    [{op[:80]}]")
        for mk, mv in m.items():
            print(f"      {mk}: {mv}")

    out_path = os.environ.get("BENCH_OUT")
    if out_path:
        with open(out_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"  wrote {out_path}")

    return summary


def shutdown_ray() -> None:
    if ray.is_initialized():
        ray.shutdown()
