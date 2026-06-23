# ABOUTME: Extracts benchmark metrics from Ray Data stats and GPU monitoring output.
# ABOUTME: Returns a dict suitable for benchmark.py's extra metrics (written to result.json).

import glob as globmod
import os


def extract_pipeline_metrics(ds, num_gpus=0, outdir=None):
    """Extract key metrics from a completed dataset and optional GPU output.

    Call after ds.write_parquet() — stats are preserved through the write.

    Args:
        ds: Ray Dataset that has been executed (e.g. via write_parquet).
        num_gpus: Number of GPUs in the cluster (for per-GPU throughput).
        outdir: Shared storage directory containing gpu_usage_*.txt files.

    Returns:
        Dict of metric name -> value, suitable for benchmark.py extra metrics.
    """
    summary = ds.get_stats_summary()
    metrics = {}

    # --- Tier 1: from DatasetStatsSummary ---

    wall_time = summary.get_total_wall_time()

    # Row count: use the Write operator's input rows (= actual data rows
    # processed). The Write operator's output_num_rows counts parquet file
    # blocks, not data rows.
    num_rows = _get_total_rows(summary)
    if num_rows and num_rows > 0:
        metrics["num_rows"] = num_rows
        if wall_time:
            rows_per_sec = num_rows / wall_time
            metrics["rows_per_sec"] = round(rows_per_sec, 1)
            if num_gpus > 0:
                metrics["rows_per_gpu_per_sec"] = round(rows_per_sec / num_gpus, 1)

    # streaming_exec_schedule_s measures total time in the scheduler loop,
    # which overlaps with execution. This is NOT the same as "Infer Scheduling
    # OH" from run summaries (which is derived from py-spy profiling).
    sched_s = summary.streaming_exec_schedule_s
    if sched_s:
        metrics["scheduler_loop_s"] = round(sched_s, 2)

    # Phase timings: each operator's end time relative to pipeline start
    _add_phase_timings(summary, metrics)

    # --- Tier 2: locality from per-operator extra_metrics ---
    _add_locality_metrics(summary, metrics)
    _add_select_actors_metrics(summary, metrics)

    # --- Tier 3: GPU metrics from nvidia-smi output ---
    if outdir:
        gpu_metrics = parse_gpu_utilization(outdir)
        metrics.update(gpu_metrics)
        object_store_metrics = parse_object_store_state(outdir)
        metrics.update(object_store_metrics)
        plasma_metrics = parse_plasma_stats(outdir)
        metrics.update(plasma_metrics)

    return metrics


def _add_phase_timings(summary, metrics):
    """Add per-operator phase end times relative to pipeline start."""
    # Collect all operator summaries by walking the parent chain
    all_summaries = _collect_all_summaries(summary)

    # Find the earliest start time across all operators
    start_times = []
    for s in all_summaries:
        for op in s.operators_stats:
            if op.earliest_start_time is not None:
                start_times.append(op.earliest_start_time)
    if not start_times:
        return
    pipeline_start = min(start_times)

    # Record each operator's end time relative to pipeline start
    for s in all_summaries:
        for op in s.operators_stats:
            if op.latest_end_time is not None and not op.is_sub_operator:
                name = _normalize_op_name(op.operator_name)
                metrics[f"phase_{name}_s"] = round(
                    op.latest_end_time - pipeline_start, 1
                )


def _add_locality_metrics(summary, metrics):
    """Extract locality counters from the Infer operator's extra_metrics."""
    all_summaries = _collect_all_summaries(summary)
    for s in all_summaries:
        em = s.extra_metrics
        if not em:
            continue
        hit = em.get("num_tasks_task_locality_hit", 0)
        miss = em.get("num_tasks_task_locality_miss", 0)
        total = hit + miss
        if total > 0:
            # Use the base_name to identify which operator this belongs to
            name = _normalize_op_name(s.base_name) if s.base_name else "unknown"
            metrics[f"{name}_locality_pct"] = round(100 * hit / total, 1)
            metrics[f"{name}_locality_hit"] = hit
            metrics[f"{name}_locality_miss"] = miss


# Counter keys exported by ActorPoolMapOperator._extra_metrics from the underlying
# _ActorPool.get_select_actors_metrics(). See doc/actor-only-ray-data/
# actor-pool-map-operator-design.md §5.5 for what each path means.
_SELECT_ACTORS_KEYS = (
    "select_actors_calls",
    "select_actors_capacity_probes",
    "select_actors_returned_none_empty",
    "select_actors_returned_none_saturated",
    "select_actors_probe_returned_none_empty",
    "select_actors_probe_returned_none_saturated",
    "select_actors_probe_returned_actor",
    "select_actors_locality_hit_path",
    "select_actors_global_fallback_no_locality_enabled",
    "select_actors_global_fallback_no_prefs",
    "select_actors_global_fallback_no_actor_on_pref_node",
    "find_actor_with_locality_nodes_probed_total",
    "find_actor_with_locality_short_circuited",
    "find_actor_with_locality_node_heap_absent",
    "find_actor_with_locality_node_heap_saturated",
)


def _add_select_actors_metrics(summary, metrics):
    """Extract per-operator select_actors path counters from extra_metrics.

    Only fires for actor-pool operators (the keys are absent on task-pool ops).
    Result keys are prefixed with the normalized operator name, e.g.
    ``flat_map_decode__map_preprocess_select_actors_calls``.
    """
    for s in _collect_all_summaries(summary):
        em = s.extra_metrics or {}
        if not any(k in em for k in _SELECT_ACTORS_KEYS):
            continue
        name = _normalize_op_name(s.base_name) if s.base_name else "unknown"
        for k in _SELECT_ACTORS_KEYS:
            if k in em:
                metrics[f"{name}_{k}"] = em[k]


def _get_total_rows(summary):
    """Get total data rows processed by the pipeline.

    Uses the Write operator's total_input_num_rows (which is the actual data
    row count), falling back to the last non-Write operator's output_num_rows.
    """
    # Check current summary's operators for Write input or last op output
    if summary.operators_stats:
        last_op = summary.operators_stats[-1]
        # Write operator's input = actual data rows
        if last_op.total_input_num_rows and last_op.total_input_num_rows > 0:
            return last_op.total_input_num_rows
        # Fallback: last operator's output (may be blocks, not rows)
        if last_op.output_num_rows and last_op.output_num_rows.sum > 0:
            return last_op.output_num_rows.sum
    # Walk parents to find a row count
    for parent in summary.parents or []:
        rows = _get_total_rows(parent)
        if rows and rows > 0:
            return rows
    return None


def _collect_all_summaries(summary):
    """Walk the parent chain to collect all DatasetStatsSummary nodes."""
    result = []
    queue = [summary]
    while queue:
        s = queue.pop(0)
        result.append(s)
        if s.parents:
            queue.extend(s.parents)
    return result


def _normalize_op_name(name):
    """Normalize operator name for use as a JSON key.

    "MapBatches(Infer)" -> "map_batches_infer"
    "ReadFiles" -> "read_files"
    "FlatMap(decode)" -> "flat_map_decode"
    "FlatMap(decode)->Map(preprocess)" -> "flat_map_decode__map_preprocess"
    "Limit=717000" -> "limit_717000"
    """
    # Replace fused operator separator and parentheses
    name = name.replace("->", "__").replace("(", "_").replace(")", "")
    name = name.replace("=", "_").strip("_")
    # CamelCase to snake_case
    result = []
    for i, c in enumerate(name):
        if c.isupper() and i > 0 and name[i - 1] not in ("_",):
            result.append("_")
        result.append(c.lower())
    return "".join(result)


def parse_object_store_state(outdir):
    """Aggregate the object_store_state.csv + actor_placement.csv into result.json keys.

    Distinguishes per-operator primary-object peak bytes on GPU vs. CPU nodes.
    "GPU" is detected by cross-referencing IP against nodes that emitted
    nvidia-smi traces (i.e., where a gpu_usage_<ip>.txt file exists).

    Output keys (each prefixed by normalized operator name):
        {op}_primary_bytes_gpu_peak_gb
        {op}_primary_bytes_cpu_peak_gb
        {op}_primary_pinned_gpu_peak_gb     # currently in active use
        {op}_primary_local_ref_gpu_peak_gb  # held by Python ref
        {op}_n_actors_gpu_max
        {op}_n_actors_cpu_max
    """
    import csv as _csv

    obj_path = os.path.join(outdir, "object_store_state.csv")
    actor_path = os.path.join(outdir, "actor_placement.csv")
    if not os.path.exists(obj_path):
        return {}

    # Identify GPU vs CPU IPs by gpu_usage_<ip>.txt presence.
    gpu_ips = set()
    for f in globmod.glob(os.path.join(outdir, "gpu_usage_*.txt")):
        ip = (
            os.path.basename(f)
            .replace("gpu_usage_", "")
            .replace(".txt", "")
            .replace("_", ".")
        )
        gpu_ips.add(ip)

    # Aggregate object-store time series.
    # For each tick × operator × role, sum bytes across nodes in that role.
    # Then take the peak of per-tick sums per operator × role.
    from collections import defaultdict

    # ts -> op -> role -> {bytes_total, bytes_pinned, bytes_local_ref}
    series = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(
                lambda: {"bytes_total": 0, "bytes_pinned": 0, "bytes_local_ref": 0}
            )
        )
    )
    with open(obj_path) as f:
        for row in _csv.DictReader(f):
            try:
                ts = float(row["timestamp"])
                ip = row["owner_ip"]
                op = row["operator"]
                role = "gpu" if ip in gpu_ips else "cpu"
                bucket = series[ts][op][role]
                bucket["bytes_total"] += int(row["bytes_total"] or 0)
                bucket["bytes_pinned"] += int(row["bytes_pinned"] or 0)
                bucket["bytes_local_ref"] += int(row["bytes_local_ref"] or 0)
            except (KeyError, ValueError):
                continue

    # Reduce to peaks per operator × role.
    peaks = defaultdict(
        lambda: defaultdict(
            lambda: {"bytes_total": 0, "bytes_pinned": 0, "bytes_local_ref": 0}
        )
    )
    for ts, by_op in series.items():
        for op, by_role in by_op.items():
            for role, bucket in by_role.items():
                p = peaks[op][role]
                for k in p:
                    p[k] = max(p[k], bucket[k])

    out = {}
    for op, by_role in peaks.items():
        # Keep "_other" — it captures bytes that couldn't be attributed to a
        # specific operator (e.g., framework objects, streaming-executor output
        # buffers, ReadFiles outputs the actor-pid join missed). On GPU nodes
        # this is often the dominant Plasma filler; dropping it loses the most
        # diagnostic signal.
        op_norm = _normalize_op_name(op) if op != "_other" else "other"
        for role in ("gpu", "cpu"):
            b = by_role.get(role)
            if not b:
                continue
            out[f"{op_norm}_primary_bytes_{role}_peak_gb"] = round(
                b["bytes_total"] / 1e9, 2
            )
            if role == "gpu":
                out[f"{op_norm}_primary_pinned_{role}_peak_gb"] = round(
                    b["bytes_pinned"] / 1e9, 2
                )
                out[f"{op_norm}_primary_local_ref_{role}_peak_gb"] = round(
                    b["bytes_local_ref"] / 1e9, 2
                )

    # Actor placement peaks per operator × role.
    if os.path.exists(actor_path):
        actor_peaks = defaultdict(lambda: defaultdict(int))
        ts_role_actors = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        with open(actor_path) as f:
            for row in _csv.DictReader(f):
                try:
                    ts = float(row["timestamp"])
                    ip = row["node_ip"]
                    op = row["operator"]
                    role = "gpu" if ip in gpu_ips else "cpu"
                    n = int(row["n_actors"] or 0)
                    ts_role_actors[ts][(op, role)]["n"] += n
                except (KeyError, ValueError):
                    continue
        for ts, m in ts_role_actors.items():
            for (op, role), v in m.items():
                actor_peaks[op][role] = max(actor_peaks[op][role], v["n"])
        for op, by_role in actor_peaks.items():
            op_norm = _normalize_op_name(op)
            for role in ("gpu", "cpu"):
                if role in by_role:
                    out[f"{op_norm}_n_actors_{role}_max"] = by_role[role]

    return out


def parse_plasma_stats(outdir):
    """Aggregate plasma_stats.csv into per-role peak metrics.

    plasma_stats.csv carries per-(tick, node) Plasma totals from each
    raylet's GetNodeStats RPC: bytes_used, bytes_avail, bytes_primary,
    cumulative spilled/restored. The diagnostic value is bytes_used vs
    bytes_primary — their difference is "secondary copies + framework
    overhead", which list_objects() can't see.

    Output keys (per role = gpu/cpu, max across the run):
        plasma_used_<role>_peak_gb
        plasma_primary_<role>_peak_gb
        plasma_secondary_or_other_<role>_peak_gb  (= used - primary)
        plasma_avail_<role>_peak_gb
        plasma_used_pct_<role>_peak               (= used / avail × 100)
        plasma_spilled_<role>_total_gb            (final cumulative)
        plasma_restored_<role>_total_gb           (final cumulative)
        plasma_spilled_total_gb                   (cluster-wide final)
    """
    import csv as _csv

    plasma_path = os.path.join(outdir, "plasma_stats.csv")
    if not os.path.exists(plasma_path):
        return {}

    gpu_ips = set()
    for f in globmod.glob(os.path.join(outdir, "gpu_usage_*.txt")):
        ip = (
            os.path.basename(f)
            .replace("gpu_usage_", "")
            .replace(".txt", "")
            .replace("_", ".")
        )
        gpu_ips.add(ip)

    # Aggregate per-tick × per-role sums; track peaks and final cumulative.
    from collections import defaultdict

    ts_role_used = defaultdict(lambda: defaultdict(int))
    ts_role_primary = defaultdict(lambda: defaultdict(int))
    ts_role_avail = defaultdict(lambda: defaultdict(int))
    final_role_spilled = defaultdict(int)  # cumulative final per role
    final_role_restored = defaultdict(int)
    final_node_spilled = {}  # ip -> final cumulative
    final_node_restored = {}

    with open(plasma_path) as f:
        for row in _csv.DictReader(f):
            try:
                ts = float(row["timestamp"])
                ip = row["node_ip"]
                role = "gpu" if ip in gpu_ips else "cpu"
                used = int(row["bytes_used"] or 0)
                primary = int(row["bytes_primary"] or 0)
                avail = int(row["bytes_avail"] or 0)
                spilled = int(row["spilled_bytes_total"] or 0)
                restored = int(row["restored_bytes_total"] or 0)
            except (KeyError, ValueError):
                continue
            ts_role_used[ts][role] += used
            ts_role_primary[ts][role] += primary
            ts_role_avail[ts][role] += avail
            final_node_spilled[ip] = spilled
            final_node_restored[ip] = restored

    for ip, b in final_node_spilled.items():
        role = "gpu" if ip in gpu_ips else "cpu"
        final_role_spilled[role] += b
    for ip, b in final_node_restored.items():
        role = "gpu" if ip in gpu_ips else "cpu"
        final_role_restored[role] += b

    out = {}
    for role in ("gpu", "cpu"):
        peak_used = max((ts_role_used[ts][role] for ts in ts_role_used), default=0)
        peak_primary = max(
            (ts_role_primary[ts][role] for ts in ts_role_primary), default=0
        )
        peak_avail = max((ts_role_avail[ts][role] for ts in ts_role_avail), default=0)
        # Per-tick "secondary+other" peak — take the max over ticks of
        # (used_role - primary_role), since the difference may peak at a
        # different tick than either component on its own.
        peak_sec_or_other = max(
            (ts_role_used[ts][role] - ts_role_primary[ts][role] for ts in ts_role_used),
            default=0,
        )
        out[f"plasma_used_{role}_peak_gb"] = round(peak_used / 1e9, 2)
        out[f"plasma_primary_{role}_peak_gb"] = round(peak_primary / 1e9, 2)
        out[f"plasma_secondary_or_other_{role}_peak_gb"] = round(
            peak_sec_or_other / 1e9, 2
        )
        out[f"plasma_avail_{role}_peak_gb"] = round(peak_avail / 1e9, 2)
        if peak_avail > 0:
            out[f"plasma_used_pct_{role}_peak"] = round(100 * peak_used / peak_avail, 1)
        out[f"plasma_spilled_{role}_total_gb"] = round(
            final_role_spilled[role] / 1e9, 2
        )
        out[f"plasma_restored_{role}_total_gb"] = round(
            final_role_restored[role] / 1e9, 2
        )
    out["plasma_spilled_total_gb"] = round(sum(final_node_spilled.values()) / 1e9, 2)
    out["plasma_restored_total_gb"] = round(sum(final_node_restored.values()) / 1e9, 2)
    return out


def parse_gpu_utilization(outdir):
    """Parse nvidia-smi dmon output files for GPU utilization metrics.

    Args:
        outdir: Directory containing gpu_usage_*.txt files.

    Returns:
        Dict with gpu_sm_mean_pct and gpu_duty_cycle_pct, or empty dict
        if no GPU data is available.
    """
    files = globmod.glob(os.path.join(outdir, "gpu_usage_*.txt"))
    if not files:
        return {}

    all_sm_values = []
    for filepath in files:
        sm_values = _parse_gpu_file(filepath)
        all_sm_values.extend(sm_values)

    if not all_sm_values:
        return {}

    total_samples = len(all_sm_values)
    active_samples = sum(1 for v in all_sm_values if v > 0)
    mean_sm = sum(all_sm_values) / total_samples

    return {
        "gpu_sm_mean_pct": round(mean_sm, 1),
        "gpu_duty_cycle_pct": round(100 * active_samples / total_samples, 1),
    }


def _parse_gpu_file(filepath):
    """Parse a single gpu_usage_*.txt file and return list of SM% values.

    Strips trailing zero-SM samples from the end of the file, since
    nvidia-smi keeps running after the benchmark finishes and those
    idle samples would skew utilization metrics downward.
    """
    sm_values = []
    try:
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                # Format: HH:MM:SS  gpu_idx  sm%  mem%  enc%  dec%  jpg%  ofa%
                if len(parts) >= 3:
                    try:
                        sm_values.append(int(parts[2]))
                    except ValueError:
                        continue
    except (OSError, IOError) as e:
        print(f"WARNING: Failed to read GPU usage file {filepath}: {e}")
    # Strip trailing idle samples (nvidia-smi continues after benchmark ends).
    while sm_values and sm_values[-1] == 0:
        sm_values.pop()
    return sm_values
