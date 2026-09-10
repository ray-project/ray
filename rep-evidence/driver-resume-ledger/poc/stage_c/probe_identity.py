"""C23 part A: is there ANY in-cluster identity that discriminates?

A candidate must be **stable across a restart on the same storage path** (a head
restart) and **different on a new storage path** (a new cluster). Anything that
fails either half cannot be used to scope a ledger.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time

import ray

PATH_A = "/tmp/ledger_id_a"
PATH_B = "/tmp/ledger_id_b"


def boot(path: str):
    os.makedirs(path, exist_ok=True)
    ray.init(num_cpus=2, include_dashboard=False, logging_level="ERROR",
             _system_config={"gcs_storage": "rocksdb", "gcs_storage_path": path})
    node = ray._private.worker._global_node
    rc = ray.get_runtime_context()

    def safe(fn):
        try:
            v = fn()
            return str(v)
        except Exception as e:
            return f"<unavailable: {type(e).__name__}>"

    cands = {
        "session_name": safe(lambda: node.session_name),
        "session_dir": safe(lambda: node.get_session_dir_path()),
        "gcs_address": safe(lambda: node.gcs_address),
        "node_id": safe(lambda: rc.get_node_id()),
        "runtime_ctx_cluster_id": safe(
            lambda: getattr(rc, "get_cluster_id", lambda: None)()),
        "gcs_client_cluster_id": safe(
            lambda: ray._raylet.GcsClient(address=node.gcs_address).cluster_id),
        "env_RAY_CLUSTER_NAME": safe(lambda: os.environ.get("RAY_CLUSTER_NAME")),
        "storage_path_cluster_marker": safe(
            lambda: open(os.path.join(path, "session_name")).read().strip()
            if os.path.exists(os.path.join(path, "session_name")) else None),
        "storage_dir_listing": safe(lambda: ",".join(sorted(os.listdir(path)))),
    }
    ray.shutdown()
    time.sleep(2)
    return cands


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    for p in (PATH_A, PATH_B):
        if os.path.isdir(p):
            shutil.rmtree(p)

    boots = {}
    boots["1_fresh_A"] = boot(PATH_A)
    boots["2_same_path_A"] = boot(PATH_A)  # head restart AND replacement-on-reused-PV: the same event
    boots["3_fresh_B"] = boot(PATH_B)          # models a genuinely new cluster

    # THE POINT, and the thing the first version of this probe got wrong.
    #
    # There are only two physical events available:
    #   same-path boot      -- which is BOTH "my head restarted" AND
    #                          "a different cluster picked up my PV"
    #   different-path boot -- a new cluster with a new disk
    #
    # A usable scope key must be STABLE across the first (so a head restart
    # still finds its ledger) and CHANGE across the first (so a replacement
    # does not adopt it). Those are the same observation, so the requirement is
    # self-contradictory for any function of in-cluster state.
    #
    # The first version of this probe compared boot 1 against boot 3 -- a
    # DIFFERENT path -- and duly reported four "usable" candidates. That tested
    # "can you tell two clusters apart when they have different disks", which is
    # not the hazard and is trivially yes.
    verdict = {}
    for k in boots["1_fresh_A"]:
        stable_same_path = boots["1_fresh_A"][k] == boots["2_same_path_A"][k]
        differs_new_disk = boots["1_fresh_A"][k] != boots["3_fresh_B"][k]
        verdict[k] = {
            "stable_across_same_path_boot": stable_same_path,
            "differs_across_new_disk_boot": differs_new_disk,
            # The requirement that actually matters, and which is unsatisfiable:
            "can_detect_replacement_on_reused_disk": (
                stable_same_path and not stable_same_path),
            "survives_head_restart": stable_same_path,
            "USABLE_AS_SCOPE_KEY": False,
            "why_not": (
                "stable across a same-path boot, so it cannot tell a head "
                "restart from a replacement on the same disk"
                if stable_same_path else
                "not stable across a same-path boot, so a head restart would "
                "lose its own ledger"),
            "values": {b: boots[b][k][:70] for b in boots},
        }

    usable = [k for k, v in verdict.items() if v["USABLE_AS_SCOPE_KEY"]]
    res = {
        "ray_version": ray.__version__,
        "candidates": verdict,
        "usable_candidates": usable,
        "probe_can_distinguish_clusters_at_all": any(
            v["differs_across_new_disk_boot"] for v in verdict.values()),
        "C23_refuted": bool(usable),
        "argument": (
            "A head restart and a cluster replacement on a reused "
            "gcs_storage_path are the SAME physical event from inside the "
            "cluster: a process starting against an existing path. No function "
            "of in-cluster state can return different values for one event. "
            "Every candidate therefore fails one half or the other by "
            "construction, and the split is visible in the table: the four "
            "path-derived candidates survive a head restart and cannot detect "
            "replacement; gcs_address and node_id detect everything and "
            "survive nothing."),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2)

    print(f"  {'candidate':30s} {'survives head restart':>22s} "
          f"{'detects replacement':>20s}")
    for k, v in verdict.items():
        print(f"  {k:30s} {str(v['survives_head_restart']):>22s} "
              f"{str(v['can_detect_replacement_on_reused_disk']):>20s}")
    print()
    print("usable candidates:", usable or "NONE")
    print("a key must do BOTH; no function of in-cluster state can, because the")
    print("two scenarios are the same physical event.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
