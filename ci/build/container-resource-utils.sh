#!/usr/bin/env bash

# Generates Bazel resource flags by cross-referencing cgroup limits with a
# RAM-per-job ratio to prevent OOM kills in containerized environments.

# Read a file and return its content, or a default value
_read_sysfs() {
    local file="$1"
    [[ -r "$file" ]] && cat "$file" || echo ""
}

# Detect Memory Limit in MB
_get_container_mem_limit_mb() {
    local mem_bytes

    # 1. Try cgroup v2
    mem_bytes=$(_read_sysfs "/sys/fs/cgroup/memory.max")
    if [[ "$mem_bytes" == "max" ]]; then mem_bytes=""; fi

    # 2. Try cgroup v1
    if [[ -z "$mem_bytes" ]]; then
        mem_bytes=$(_read_sysfs "/sys/fs/cgroup/memory/memory.limit_in_bytes")
        # Ignore values that indicate "unlimited" (larger than 2^60)
        if [[ -n "$mem_bytes" ]] && (( mem_bytes > (1024**5) )); then mem_bytes=""; fi
    fi

    # 3. Fallback to Host MemTotal
    if [[ -z "$mem_bytes" ]]; then
        awk '/MemTotal/ {print int($2) * 1024}' /proc/meminfo
    else
        echo "$mem_bytes"
    fi | awk '{print int($1 / 1024 / 1024)}'
}

# Detect CPU Limit (Cores)
_get_container_cpu_limit() {
    local quota period

    # 1. Try cgroup v2
    if [[ -r /sys/fs/cgroup/cpu.max ]]; then
        read -r quota period < /sys/fs/cgroup/cpu.max
        if [[ "$quota" != "max" ]]; then
            echo "$(( (quota + period - 1) / period ))"
            return
        fi
    fi

    # 2. Try cgroup v1
    quota=$(_read_sysfs "/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period=$(_read_sysfs "/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if [[ -n "$quota" && "$quota" -gt 0 && -n "$period" && "$period" -gt 0 ]]; then
        echo "$(( (quota + period - 1) / period ))"
        return
    fi

    # 3. Fallback
    nproc
}

# Emit Bazel flags tuned to the container's cgroup CPU + memory limits.
#
# Output example:
#   --jobs=4 --local_cpu_resources=4 --local_ram_resources=6144
#
# NOTE(andrew-anyscale): Using --jobs as a heavy-weight metric to
# prevent OOMs. In the future, we should shift to using resource tags in
# BUILD files, or limit the number of concurrent CppCompile and CppLink actions
# to prevent OOMs more granularly.
bazel_container_resource_flags() {
    # --- Configuration ---
    local mb_per_job="${BAZEL_MB_PER_JOB:-3072}"
    local reserve_mb=2048

    # --- Detection ---
    local total_mem_mb
    total_mem_mb=$(_get_container_mem_limit_mb)
    local cpu_limit
    cpu_limit=$(_get_container_cpu_limit)

    # --- Calculation ---
    # Calculate "slots" based on available RAM minus overhead
    local usable_mem_mb=$(( total_mem_mb - reserve_mb ))
    [[ $usable_mem_mb -lt mb_per_job ]] && usable_mem_mb=$mb_per_job

    local jobs_by_ram=$(( usable_mem_mb / mb_per_job ))

    # Final Jobs = the bottleneck between CPU count and RAM availability
    local bazel_jobs=$cpu_limit
    [[ $jobs_by_ram -lt $bazel_jobs ]] && bazel_jobs=$jobs_by_ram
    [[ $bazel_jobs -lt 1 ]] && bazel_jobs=1

    # Output full container limits to Bazel, but throttle via --jobs.
    echo "--jobs=${bazel_jobs} --local_cpu_resources=${cpu_limit} --local_ram_resources=${total_mem_mb}"
}
