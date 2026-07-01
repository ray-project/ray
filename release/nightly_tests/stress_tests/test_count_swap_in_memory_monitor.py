#!/usr/bin/env python3
"""Benchmark for the opt-in swap accounting flag (RAY_count_swap_in_memory_monitor).

Runs a memory-allocation task twice — flag off, then flag on — and reports
whether each run completed or got OOM-killed by Ray's monitor.

  flag off: threshold = 0.7 * RAM           -> task killed
  flag on:  threshold = 0.7 * (RAM + swap)  -> task completes

On a host without swap, the flag is a no-op; both cells behave identically.
Resource-isolation mode (--enable-resource-isolation) uses a separate code
path covered by gtests.

Usage:
    python3 swap_benchmark.py [--target-ratio 0.6] [--timeout 300]

--target-ratio is the fraction of host RAM to allocate. It must land
between the two thresholds for the comparison to be meaningful:

    0.7 * RAM < target * RAM < 0.7 * (RAM + swap)
"""

import argparse
import json
import os
import subprocess
import sys
import time

CHUNK_BYTES = 256 * 1024 * 1024
OOM_THRESHOLD = 0.7
MONITOR_REFRESH_MS = 200
RESULT_MARKER = "===RESULT===\n"


def run_worker(target_ratio: float, timeout_s: int) -> None:
    """Runs in a subprocess so the flag env var is set before `ray` imports."""
    import numpy as np
    import psutil
    import ray
    from ray.exceptions import OutOfMemoryError, RayTaskError, WorkerCrashedError

    flag_on = os.environ.get("RAY_count_swap_in_memory_monitor", "0") == "1"

    ray.init(
        num_cpus=1,
        include_dashboard=False,
        log_to_driver=False,
        _system_config={
            "memory_usage_threshold": OOM_THRESHOLD,
            "memory_monitor_refresh_ms": MONITOR_REFRESH_MS,
            "count_swap_in_memory_monitor": flag_on,
        },
    )

    target_bytes = int(psutil.virtual_memory().total * target_ratio)

    @ray.remote(max_retries=0, num_cpus=1)
    def allocate(target: int) -> int:
        buf = []
        allocated = 0
        while allocated < target:
            # fill() forces commit; np.empty alone would lazy-allocate
            # and the monitor wouldn't see real pressure.
            chunk = np.empty(CHUNK_BYTES, dtype=np.uint8)
            chunk.fill(1)
            buf.append(chunk)
            allocated += CHUNK_BYTES
            time.sleep(0.05)
        # Hold the working set long enough for one more monitor tick.
        time.sleep(3.0)
        return allocated

    result = {
        "flag_on": flag_on,
        "target_bytes": target_bytes,
        "completed": False,
        "killed": False,
        "elapsed_s": None,
        "error": None,
    }
    t0 = time.time()
    try:
        ray.get(allocate.remote(target_bytes), timeout=timeout_s)
        result["completed"] = True
    except OutOfMemoryError as e:
        result["killed"] = True
        result["error"] = f"OutOfMemoryError: {e}"
    except (RayTaskError, WorkerCrashedError) as e:
        result["killed"] = True
        result["error"] = f"{type(e).__name__}: {e}"
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        result["elapsed_s"] = round(time.time() - t0, 2)
        ray.shutdown()

    print(f"{RESULT_MARKER}{json.dumps(result)}")


def run_cell(flag_on: bool, target_ratio: float, timeout_s: int) -> dict:
    env = os.environ.copy()
    env["RAY_count_swap_in_memory_monitor"] = "1" if flag_on else "0"
    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "--worker",
        "--target-ratio",
        str(target_ratio),
        "--timeout",
        str(timeout_s),
    ]
    print(f">>> flag={'on' if flag_on else 'off'}", file=sys.stderr, flush=True)

    failure = {
        "flag_on": flag_on,
        "target_bytes": 0,
        "completed": False,
        "killed": False,
        "elapsed_s": None,
        "error": None,
    }
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s + 60,
        )
    except subprocess.TimeoutExpired as e:
        failure["error"] = f"worker timed out after {e.timeout}s"
        return failure

    idx = proc.stdout.find(RESULT_MARKER)
    if idx < 0:
        failure["error"] = (
            "worker emitted no result line; stderr tail:\n" + proc.stderr[-500:]
        )
        return failure
    return json.loads(proc.stdout[idx + len(RESULT_MARKER) :])


def render_report(cells: list[dict]) -> str:
    import psutil

    ram_gb = psutil.virtual_memory().total / 1024**3
    swap_gb = psutil.swap_memory().total / 1024**3

    lines = [
        "## Benchmark: opt-in swap accounting",
        "",
        f"- Host RAM: {ram_gb:.1f} GB",
        f"- Host Swap: {swap_gb:.1f} GB",
        f"- OOM threshold: {int(OOM_THRESHOLD * 100)}% of total",
        "",
        "| flag | target (GB) | completed | killed | elapsed (s) |",
        "|---|---|:-:|:-:|---|",
    ]
    for cell in cells:
        flag = "on" if cell["flag_on"] else "off"
        target_gb = round(cell["target_bytes"] / 1024**3, 1)
        completed = "yes" if cell["completed"] else "no"
        killed = "yes" if cell["killed"] else "no"
        elapsed = cell["elapsed_s"] if cell["elapsed_s"] is not None else "n/a"
        lines.append(f"| {flag} | {target_gb} | {completed} | {killed} | {elapsed} |")

    errs = [(c["flag_on"], c["error"]) for c in cells if c["error"]]
    if errs:
        lines.append("")
        lines.append("Details:")
        for flag_on, msg in errs:
            # First two lines of Ray's OOM dump carry "used / limit".
            short = " ".join(msg.splitlines()[:2])
            lines.append(f"- flag {'on' if flag_on else 'off'}: {short}")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--target-ratio",
        type=float,
        default=0.6,
        help="Fraction of host RAM to allocate. Default 0.6.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Per-cell timeout in seconds. Default 300.",
    )
    args = parser.parse_args()

    if args.worker:
        run_worker(args.target_ratio, args.timeout)
        return 0

    cells = [
        run_cell(flag_on=False, target_ratio=args.target_ratio, timeout_s=args.timeout),
        run_cell(flag_on=True, target_ratio=args.target_ratio, timeout_s=args.timeout),
    ]

    print()
    print("=== RESULTS BEGIN ===")
    print(render_report(cells))
    print("=== RESULTS END ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
