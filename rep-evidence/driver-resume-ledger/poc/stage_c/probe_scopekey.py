"""C23 part B: does an INJECTED scope key actually fix C12?

Both directions are checked, because checking only one is how C12 passed review
in the first place:

  same injected uid  -> the ledger IS adopted and the job resumes.  A "fix" that
                        fails here has traded silent corruption for guaranteed
                        total loss, which is worse than the bug.
  different uid      -> the ledger is NOT adopted, every unit re-executes, and
                        the prior ledger is left intact.

The key stands in for the RayCluster's Kubernetes ``metadata.uid``, delivered
through the downward API.  The whole point is that it comes from **outside** the
cluster: `probe_identity.py` shows nothing inside can do this job.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time

import ray
from ray.experimental import internal_kv as ikv

import progress_ledger as pl

STORE = "/tmp/ledgerscope"
UID_1 = "raycluster-uid-1111"
UID_2 = "raycluster-uid-2222"


def boot(fresh: bool):
    if fresh and os.path.isdir(STORE):
        shutil.rmtree(STORE)
    os.makedirs(STORE, exist_ok=True)
    ray.init(num_cpus=3, include_dashboard=False, logging_level="ERROR",
             namespace="ledger", _system_config={"gcs_storage": "rocksdb",
                                             "gcs_storage_path": STORE})


def run_job(scope: str, job: str, n: int) -> dict:
    """Run one coordinator incarnation to completion under a given scope key."""
    led = pl.Ledger(scope, job)
    won = led.claim()
    resumed = len(led.state["done"]) if won else -1
    executed = 0
    if won:
        done = set(led.state["done"])
        seq = led.state["seq"]
        pending = []
        for u in [u for u in range(n) if u not in done]:
            executed += 1
            pending.append(u)
            if len(pending) >= 2:
                seq += 1
                led.commit(seq, pending)
                done |= set(pending)
                pending = []
        if pending:
            seq += 1
            led.commit(seq, pending)
            done |= set(pending)
        led.compact(seq, done)
    return {"scope": scope, "won_epoch": led.epoch, "resumed_from": resumed,
            "executed": executed, "keys_under_scope": led.key_count()}


def keys_for(scope: str, job: str):
    return sorted(k.decode() if isinstance(k, bytes) else k
                  for k in (ikv._internal_kv_list(f"{scope}/{job}".encode(),
                                                  namespace=pl.NS) or []))


def main() -> int:
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    res = {"ray_version": ray.__version__, "uid_1": UID_1, "uid_2": UID_2}

    boot(fresh=True)
    res["run1_original_cluster"] = run_job(UID_1, "jobX", 10)
    keys1 = keys_for(UID_1, "jobX")
    ray.shutdown(); time.sleep(3)

    # --- direction 1: SAME uid, same disk -> must resume ------------------
    boot(fresh=False)
    res["run2_same_uid_restart"] = run_job(UID_1, "jobX", 10)
    ray.shutdown(); time.sleep(3)

    # --- direction 2: DIFFERENT uid, same disk -> must NOT adopt ----------
    boot(fresh=False)
    res["run3_different_uid"] = run_job(UID_2, "jobX", 10)
    res["prior_ledger_keys_after"] = keys_for(UID_1, "jobX")
    res["prior_ledger_intact"] = bool(res["prior_ledger_keys_after"])
    ray.shutdown()

    r2, r3 = res["run2_same_uid_restart"], res["run3_different_uid"]
    res["verdict"] = {
        "same_uid_resumes": r2["resumed_from"] == 10 and r2["executed"] == 0,
        "different_uid_does_not_adopt": r3["resumed_from"] == 0 and r3["executed"] == 10,
        "different_uid_leaves_prior_ledger_intact": res["prior_ledger_intact"],
    }
    res["verdict"]["FIX_WORKS"] = all(res["verdict"].values())
    res["keys_after_run1"] = keys1

    with open(os.path.join(out, "results.json"), "w") as f:
        json.dump(res, f, indent=2)
    for k, v in res.items():
        if k.startswith("run"):
            print(f"  {k:26s} {v}")
    print(f"  prior ledger intact after a foreign cluster ran: "
          f"{res['prior_ledger_intact']} ({len(res['prior_ledger_keys_after'])} keys)")
    print()
    for k, v in res["verdict"].items():
        print(f"  {k:44s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
