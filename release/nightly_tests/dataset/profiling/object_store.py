# ABOUTME: Periodically samples Ray object-store state via ray.util.state.
# ABOUTME: Emits per-(node, operator) primary-bytes time series for spill diagnosis.

import csv
import os
import re
import threading
import time
import traceback


_MAPWORKER_RE = re.compile(r"^MapWorker\((.+)\)$")


def start(outdir, interval_s=5, fast_window_s=60, fast_interval_s=1):
    """Start object-store sampling in a background thread.

    Outputs ``object_store_state.csv`` with one row per (tick, owner_node, operator)
    showing primary-object counts and bytes plus a reference-type breakdown.

    Args:
        outdir: Shared storage directory for output files.
        interval_s: Steady-state seconds between samples. Default 5s — fast enough
            to catch short-lived primaries between produce and GC. ``list_objects()``
            is ~100ms per call on this benchmark; 5s adds ~2% daemon overhead.
        fast_window_s: Seconds at the start of the run during which the daemon
            ticks every ``fast_interval_s`` instead of ``interval_s``. Resolves
            placement-ramp events (e.g. GPU-node CPU saturation by ReadFiles) to
            sub-5s precision. Set to 0 to disable.
        fast_interval_s: Tick interval during the fast window. Default 1s.
    """
    thread = threading.Thread(
        target=_loop,
        args=(outdir, interval_s, fast_window_s, fast_interval_s),
        daemon=True,
    )
    thread.start()
    return thread


def _loop(outdir, interval_s, fast_window_s=0, fast_interval_s=1):
    os.makedirs(outdir, exist_ok=True)
    csv_path = os.path.join(outdir, "object_store_state.csv")
    actors_path = os.path.join(outdir, "actor_placement.csv")
    plasma_path = os.path.join(outdir, "plasma_stats.csv")

    obj_fields = [
        "timestamp",
        "owner_ip",
        "operator",
        "n_objects",
        "bytes_total",
        "bytes_pinned",
        "bytes_local_ref",
        "bytes_used_by_pending_task",
        "bytes_other",
    ]
    actor_fields = [
        "timestamp",
        "node_ip",
        "operator",
        "n_actors",
    ]
    # Per-node Plasma stats from raylet GetNodeStats RPC. Distinguishes
    # primary bytes (objects whose authoritative owner is this node) from
    # total used bytes (= primary + secondary copies + framework overhead).
    # Also exposes spill/restore totals per node for time-correlated analysis.
    plasma_fields = [
        "timestamp",
        "node_ip",
        "bytes_used",
        "bytes_avail",
        "bytes_primary",
        "bytes_fallback",
        "n_local_objects",
        "spilled_bytes_total",
        "spilled_objects_total",
        "restored_bytes_total",
        "restored_objects_total",
    ]

    obj_f = open(csv_path, "w", newline="")
    obj_writer = csv.DictWriter(obj_f, fieldnames=obj_fields)
    obj_writer.writeheader()
    obj_f.flush()

    actor_f = open(actors_path, "w", newline="")
    actor_writer = csv.DictWriter(actor_f, fieldnames=actor_fields)
    actor_writer.writeheader()
    actor_f.flush()

    plasma_f = open(plasma_path, "w", newline="")
    plasma_writer = csv.DictWriter(plasma_f, fieldnames=plasma_fields)
    plasma_writer.writeheader()
    plasma_f.flush()

    if fast_window_s > 0:
        print(
            f"object_store_monitor: writing {csv_path}, {actors_path}, {plasma_path} "
            f"every {fast_interval_s}s for {fast_window_s}s starting from first actor, "
            f"then every {interval_s}s"
        )
    else:
        print(
            f"object_store_monitor: writing {csv_path}, {actors_path}, {plasma_path} "
            f"every {interval_s}s"
        )

    # Anchor the fast window to "first time we observe any actors" so the 1s
    # sampling lands on the actual ramp-up regardless of how long the job
    # idle-waits before spawning actors. None until first non-zero count.
    fast_window_started_at = None
    while True:
        try:
            now = time.time()
            actor_to_op, actor_placement = _snapshot_actors()
            for row in actor_placement:
                row["timestamp"] = now
                actor_writer.writerow(row)
            actor_f.flush()

            plasma_rows = _snapshot_plasma_per_node()
            for row in plasma_rows:
                row["timestamp"] = now
                plasma_writer.writerow(row)
            plasma_f.flush()

            obj_rows = _snapshot_objects(actor_to_op)
            for row in obj_rows:
                row["timestamp"] = now
                obj_writer.writerow(row)
            obj_f.flush()

            n_actors = sum(r["n_actors"] for r in actor_placement)
            if fast_window_started_at is None and n_actors > 0:
                fast_window_started_at = time.time()
                print(
                    f"object_store_monitor: first actor detected; fast window "
                    f"({fast_interval_s}s tick) active for next {fast_window_s}s"
                )

            plasma_used_total_gb = sum(r["bytes_used"] for r in plasma_rows) / 1e9
            spill_total_gb = sum(r["spilled_bytes_total"] for r in plasma_rows) / 1e9
            print(
                f"object_store_monitor: tick={now:.0f} "
                f"actors={n_actors} "
                f"objects={sum(r['n_objects'] for r in obj_rows)} "
                f"plasma_used={plasma_used_total_gb:.1f}GB "
                f"spilled={spill_total_gb:.1f}GB"
            )
        except Exception as e:
            print(f"object_store_monitor: WARN {e}")
            traceback.print_exc()

        in_fast_window = (
            fast_window_started_at is not None
            and (time.time() - fast_window_started_at) < fast_window_s
        )
        time.sleep(fast_interval_s if in_fast_window else interval_s)


def _snapshot_actors():
    """Return ((ip, pid) → operator_name) map and per-(node_ip, op) actor counts.

    ActorState carries node_id (a hex string) but not the IP. We join against
    list_nodes() to recover the IP — which is what list_objects() reports for
    object owners and what we cross-reference against in the metrics extractor.
    """
    from collections import defaultdict
    from ray.util.state import list_actors, list_nodes

    # raise_on_missing_output=False: allow partial results when the state
    # API truncates due to data size. Without this the daemon dies on the
    # first heavy snapshot.
    nodes = list_nodes(limit=10000, raise_on_missing_output=False)
    node_id_to_ip = {n["node_id"]: n.get("node_ip", "unknown") for n in nodes}

    actors = list_actors(
        filters=[("state", "=", "ALIVE")],
        limit=10000,
        raise_on_missing_output=False,
    )
    actor_to_op = {}
    counts = defaultdict(int)

    for a in actors:
        cls = a.get("class_name", "") or ""
        m = _MAPWORKER_RE.match(cls)
        if not m:
            continue
        operator = m.group(1)
        node_ip = node_id_to_ip.get(a.get("node_id"), "unknown")
        if a.get("pid") not in (None, 0) and node_ip != "unknown":
            actor_to_op[(node_ip, int(a["pid"]))] = operator
        counts[(node_ip, operator)] += 1

    placement = [
        {"node_ip": ip, "operator": op, "n_actors": n} for (ip, op), n in counts.items()
    ]
    return actor_to_op, placement


def _snapshot_objects(actor_to_op):
    """Aggregate live objects by (owner_ip, operator)."""
    from collections import defaultdict
    from ray.util.state import list_objects

    # API server caps the limit at 10000 unless RAY_MAX_LIMIT_FROM_API_SERVER
    # is set. For runs with > 10k objects, set that env var on the head node:
    #   RAY_MAX_LIMIT_FROM_API_SERVER=200000
    # to avoid truncated samples.
    #
    # raise_on_missing_output=False: the state API also truncates when the
    # response size (not just count) exceeds an internal RPC limit; with the
    # default the daemon dies the moment objects get heavy. Partial data is
    # preferable to no data here.
    objs = list_objects(limit=10_000, raise_on_missing_output=False)

    agg = defaultdict(
        lambda: {
            "n_objects": 0,
            "bytes_total": 0,
            "bytes_pinned": 0,
            "bytes_local_ref": 0,
            "bytes_used_by_pending_task": 0,
            "bytes_other": 0,
        }
    )

    for obj in objs:
        # obj.type is WORKER / DRIVER / SPILL_WORKER / RESTORE_WORKER. We focus on
        # WORKER since that's the productive workload.
        if obj.get("type") != "WORKER":
            continue
        owner_ip = obj.get("ip") or "unknown"
        pid = obj.get("pid")
        operator = (
            actor_to_op.get((owner_ip, int(pid))) if pid is not None else None
        ) or "_other"
        size = int(obj.get("object_size", 0) or 0)
        ref = obj.get("reference_type") or ""

        bucket = agg[(owner_ip, operator)]
        bucket["n_objects"] += 1
        bucket["bytes_total"] += size
        if ref == "PINNED_IN_MEMORY":
            bucket["bytes_pinned"] += size
        elif ref == "LOCAL_REFERENCE":
            bucket["bytes_local_ref"] += size
        elif ref == "USED_BY_PENDING_TASK":
            bucket["bytes_used_by_pending_task"] += size
        else:
            bucket["bytes_other"] += size

    rows = []
    for (ip, op), b in agg.items():
        rows.append({"owner_ip": ip, "operator": op, **b})
    return rows


def _snapshot_plasma_per_node():
    """Per-node Plasma stats from each raylet's GetNodeStats RPC.

    Returns a list of dicts, one per alive node, with the fields documented
    in plasma_fields. The bytes_used vs bytes_primary split is the key
    diagnostic — bytes_used - bytes_primary = bytes occupied by secondary
    copies + framework overhead, which list_objects() can't see.
    """
    import ray
    from ray._private.internal_api import node_stats

    rows = []
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        ip = node.get("NodeManagerAddress")
        port = node.get("NodeManagerPort")
        if not ip or not port:
            continue
        try:
            reply = node_stats(
                node_manager_address=ip,
                node_manager_port=port,
                include_memory_info=False,
            )
        except Exception:
            # Best-effort — skip nodes whose raylet RPC times out.
            continue
        s = reply.store_stats
        rows.append(
            {
                "node_ip": ip,
                "bytes_used": int(s.object_store_bytes_used),
                "bytes_avail": int(s.object_store_bytes_avail),
                "bytes_primary": int(s.object_store_bytes_primary_copy),
                "bytes_fallback": int(getattr(s, "object_store_bytes_fallback", 0)),
                "n_local_objects": int(s.num_local_objects),
                "spilled_bytes_total": int(s.spilled_bytes_total),
                "spilled_objects_total": int(s.spilled_objects_total),
                "restored_bytes_total": int(s.restored_bytes_total),
                "restored_objects_total": int(s.restored_objects_total),
            }
        )
    return rows
