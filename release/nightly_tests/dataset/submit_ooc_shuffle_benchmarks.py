"""Submit out-of-core shuffle benchmarks as independent Ray jobs.

Each (engine, data_size, num_partitions) combination runs as a separate Ray job
so that one OOM or failure does not affect subsequent runs, and the cluster
state (object store, Spark/Daft residuals) is fully reset between jobs.

Requires a running Ray cluster reachable at RAY_ADDRESS (default: http://127.0.0.1:8265).

Usage:
    # Run all engines (actorless, daft, spark):
    python submit_ooc_shuffle_benchmarks.py

    # Run specific engines:
    python submit_ooc_shuffle_benchmarks.py --engines actorless daft

    # Custom matrix:
    python submit_ooc_shuffle_benchmarks.py --data-sizes 25 50 --partitions 100 200

    # Dry run (print commands without submitting):
    python submit_ooc_shuffle_benchmarks.py --dry-run
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

# Default benchmark matrix from the design doc's OOC section.
DEFAULT_DATA_SIZES_GB = [25, 50, 85, 120, 170, 256]
DEFAULT_PARTITIONS = [100, 200, 500, 1000]

ENGINE_SCRIPTS = {
    "actorless": "benchmark_ooc_shuffle.py",
    "daft": "benchmark_ooc_daft_shuffle.py",
    "spark": "benchmark_ooc_spark_shuffle.py",
}

# Engine-specific CLI args beyond --data-size-gb and --num-partitions.
ENGINE_EXTRA_ARGS = {
    "actorless": ["--strategy", "actorless"],
    "daft": [],
    "spark": [],
}

# pip packages each engine needs at runtime.
ENGINE_PIP_DEPS = {
    "actorless": [],
    "daft": ["getdaft[ray]"],
    "spark": ["raydp"],
}


def submit_job(
    engine,
    data_size_gb,
    num_partitions,
    ray_address,
    output_dir,
    script_dir,
    timeout=7200,
    dry_run=False,
):
    """Submit a single benchmark as a Ray job. Returns (status, output_file, elapsed)."""
    script = ENGINE_SCRIPTS[engine]
    script_path = os.path.join(script_dir, script)
    output_file = os.path.join(
        output_dir, f"ooc_{engine}_{data_size_gb}gb_p{num_partitions}.json"
    )

    entrypoint = (
        f"python {script_path}"
        f" --data-size-gb {data_size_gb}"
        f" --num-partitions {num_partitions}"
        f" --output {output_file}"
    )
    for arg in ENGINE_EXTRA_ARGS[engine]:
        entrypoint += f" {arg}"

    # Build the ray job submit command as a shell string.
    # ray job submit expects: ray job submit [opts] -- <entrypoint>
    # where <entrypoint> is parsed by /bin/sh, so we pass the whole thing
    # through shell=True to preserve proper argument splitting.
    cmd_parts = [
        "ray",
        "job",
        "submit",
        "--address",
        ray_address,
        "--no-wait",
    ]

    # Add pip runtime env for engines that need extra packages.
    pip_deps = ENGINE_PIP_DEPS.get(engine, [])
    if pip_deps:
        runtime_env = {"pip": pip_deps}
        cmd_parts += ["--runtime-env-json", f"'{json.dumps(runtime_env)}'"]

    cmd_parts += ["--", entrypoint]
    cmd = " ".join(cmd_parts)

    tag = f"{engine} {data_size_gb}GB p{num_partitions}"

    if dry_run:
        print(f"[DRY RUN] {tag}")
        print(f"  {cmd}")
        return "dry_run", output_file, 0

    print(f"[SUBMIT] {tag}", flush=True)
    print(f"  entrypoint: {entrypoint}", flush=True)

    # Submit with --no-wait, capture the job ID, then poll.
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        print(f"  [FAILED] submission error: {result.stderr.strip()}", flush=True)
        return "submit_error", output_file, 0

    # Parse job ID from output (e.g., "Job 'raysubmit_xxx' submitted successfully").
    job_id = None
    for line in result.stdout.strip().splitlines():
        if "raysubmit_" in line:
            for word in line.split():
                cleaned = word.strip("'\"")
                if cleaned.startswith("raysubmit_"):
                    job_id = cleaned
                    break
        if job_id:
            break

    if not job_id:
        # Fallback: try to get the job id from the last word of the output.
        print(f"  Warning: could not parse job ID from: {result.stdout.strip()}")
        print(f"  stderr: {result.stderr.strip()}")
        return "unknown", output_file, 0

    print(f"  job_id: {job_id}", flush=True)

    # Poll until the job finishes.
    start = time.perf_counter()
    status = wait_for_job(job_id, ray_address, tag, timeout=timeout)
    elapsed = time.perf_counter() - start

    print(f"  [{status.upper()}] {tag} in {elapsed:.1f}s", flush=True)
    return status, output_file, elapsed


def wait_for_job(job_id, ray_address, tag, poll_interval=15, timeout=7200):
    """Poll ray job status until completion or timeout."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        result = subprocess.run(
            ["ray", "job", "status", "--address", ray_address, job_id],
            capture_output=True,
            text=True,
        )
        output = result.stdout.strip().lower()

        if "succeeded" in output:
            return "succeeded"
        elif "failed" in output:
            _print_job_logs(job_id, ray_address, tag)
            return "failed"
        elif "stopped" in output:
            return "stopped"

        time.sleep(poll_interval)

    print(f"  [TIMEOUT] {tag} after {timeout}s — stopping job...", flush=True)
    subprocess.run(
        ["ray", "job", "stop", "--address", ray_address, job_id],
        capture_output=True,
        text=True,
    )
    _print_job_logs(job_id, ray_address, tag)
    return "timeout"


def _print_job_logs(job_id, ray_address, tag):
    """Print the last 30 lines of a job's logs."""
    logs = subprocess.run(
        ["ray", "job", "logs", "--address", ray_address, job_id],
        capture_output=True,
        text=True,
    )
    log_lines = logs.stdout.strip().splitlines()
    tail = log_lines[-30:] if len(log_lines) > 30 else log_lines
    print(f"  [LOGS tail] {tag}:")
    for line in tail:
        print(f"    {line}")


def main():
    parser = argparse.ArgumentParser(
        description="Submit OOC shuffle benchmarks as independent Ray jobs"
    )
    parser.add_argument(
        "--engines",
        nargs="+",
        choices=list(ENGINE_SCRIPTS.keys()),
        default=list(ENGINE_SCRIPTS.keys()),
        help="Engines to benchmark (default: all)",
    )
    parser.add_argument(
        "--data-sizes",
        nargs="+",
        type=int,
        default=DEFAULT_DATA_SIZES_GB,
        help=f"Data sizes in GB (default: {DEFAULT_DATA_SIZES_GB})",
    )
    parser.add_argument(
        "--partitions",
        nargs="+",
        type=int,
        default=DEFAULT_PARTITIONS,
        help=f"Partition counts (default: {DEFAULT_PARTITIONS})",
    )
    parser.add_argument(
        "--ray-address",
        type=str,
        default=os.environ.get("RAY_ADDRESS", "http://127.0.0.1:8265"),
        help="Ray cluster address (default: $RAY_ADDRESS or http://127.0.0.1:8265)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="ooc_benchmark_results",
        help="Directory for result JSON files",
    )
    parser.add_argument(
        "--script-dir",
        type=str,
        default=os.path.dirname(os.path.abspath(__file__)),
        help="Directory containing the benchmark scripts",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=7200,
        help="Per-job timeout in seconds (default: 7200 = 2 hours)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without submitting",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Build the job matrix.
    jobs = []
    for engine in args.engines:
        for data_size_gb in args.data_sizes:
            for num_partitions in args.partitions:
                jobs.append((engine, data_size_gb, num_partitions))

    total = len(jobs)
    print(f"Benchmark matrix: {total} jobs")
    print(f"  Engines:    {args.engines}")
    print(f"  Data sizes: {args.data_sizes} GB")
    print(f"  Partitions: {args.partitions}")
    print(f"  Output dir: {args.output_dir}")
    print()

    summary = []
    start_all = time.perf_counter()

    for i, (engine, data_size_gb, num_partitions) in enumerate(jobs, 1):
        print(f"\n{'='*60}")
        print(f"Job {i}/{total}")
        print(f"{'='*60}")

        status, output_file, elapsed = submit_job(
            engine=engine,
            data_size_gb=data_size_gb,
            num_partitions=num_partitions,
            ray_address=args.ray_address,
            output_dir=args.output_dir,
            script_dir=args.script_dir,
            timeout=args.timeout,
            dry_run=args.dry_run,
        )

        summary.append(
            {
                "engine": engine,
                "data_size_gb": data_size_gb,
                "num_partitions": num_partitions,
                "status": status,
                "elapsed_s": round(elapsed, 1),
                "output_file": output_file,
            }
        )

    total_elapsed = time.perf_counter() - start_all

    # Write summary.
    summary_file = os.path.join(args.output_dir, "summary.json")
    summary_output = {
        "timestamp": datetime.now().isoformat(),
        "total_elapsed_s": round(total_elapsed, 1),
        "jobs": summary,
    }
    with open(summary_file, "w") as f:
        json.dump(summary_output, f, indent=2)

    # Print summary table.
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(
        f"{'Engine':>12} {'Size(GB)':>10} {'Partitions':>12} {'Status':>12} {'Time(s)':>10}"
    )
    print("-" * 60)

    for entry in summary:
        print(
            f"{entry['engine']:>12} {entry['data_size_gb']:>10} "
            f"{entry['num_partitions']:>12} {entry['status']:>12} "
            f"{entry['elapsed_s']:>10.1f}"
        )

    succeeded = sum(1 for e in summary if e["status"] == "succeeded")
    failed = sum(1 for e in summary if e["status"] not in ("succeeded", "dry_run"))
    print(f"\n{succeeded}/{total} succeeded, {failed} failed")
    print(f"Total time: {total_elapsed:.0f}s")
    print(f"Summary written to {summary_file}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
