#! /usr/bin/env python3

"""
Generates Bazel resource flags by cross-referencing cgroup limits with a
RAM-per-job ratio to prevent OOM kills in containerized environments.
"""
import argparse
import math
import os
from pathlib import Path

DEFAULT_RESERVE_MB = 2048
DEFAULT_MB_PER_JOB = 3072


def get_system_ram_mb() -> int:
    # Fallback: os.sysconf reports host RAM, ignoring container quotas.
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        return (pages * page_size) // (1024**2)
    except (ValueError, AttributeError):
        return 8192


def get_container_mem_limit_mb() -> int:
    # Cgroup v2 is preferred because it's more accurate and portable.
    paths = ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"]
    for path in paths:
        p = Path(path)
        if p.exists():
            val = p.read_text().strip()
            if val and val != "max":
                limit_bytes = int(val)
                if limit_bytes < 1024**5:  # Filter unlimited host values
                    return limit_bytes // (1024**2)
    return get_system_ram_mb()


def get_container_cpu_limit() -> int:
    v2_cpu = Path("/sys/fs/cgroup/cpu.max")
    if v2_cpu.exists():
        parts = v2_cpu.read_text().split()
        if len(parts) == 2 and parts[0] != "max":
            return max(1, math.ceil(int(parts[0]) / int(parts[1])))

    quota_p = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period_p = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota_p.exists() and period_p.exists():
        try:
            quota, period = int(quota_p.read_text()), int(period_p.read_text())
            if quota > 0:
                return max(1, math.ceil(quota / period))
        except ValueError:
            pass

    return os.cpu_count() or 1


def main():
    parser = argparse.ArgumentParser(description="Generate Bazel resource flags.")
    parser.add_argument(
        "--reserve-mb",
        type=int,
        default=int(os.getenv("RESERVE_MB", DEFAULT_RESERVE_MB)),
        help="RAM to reserve for the OS/Container overhead",
    )
    parser.add_argument(
        "--mb-per-job",
        type=int,
        default=int(os.getenv("BAZEL_MB_PER_JOB", DEFAULT_MB_PER_JOB)),
        help="Estimated RAM usage per concurrent Bazel job",
    )
    args = parser.parse_args()

    mem_limit = get_container_mem_limit_mb()
    cpu_limit = get_container_cpu_limit()

    usable_mem = max(mem_limit - args.reserve_mb, args.mb_per_job)
    jobs_by_ram = usable_mem // args.mb_per_job
    bazel_jobs = max(1, min(cpu_limit, jobs_by_ram))

    print(
        f"--jobs={bazel_jobs} --local_cpu_resources={cpu_limit} --local_ram_resources={mem_limit}"
    )


if __name__ == "__main__":
    main()
