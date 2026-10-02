"""Stage C: the pre-registered experiment, on unmodified Ray 2.58.

Five tests. Each writes its raw observations into results.json; the verdict is
computed from them rather than asserted.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ray
from ray.experimental import internal_kv as ikv

import progress_ledger as pl

STORE = "/tmp/ledger_stage_c"
SYSCFG = {"gcs_storage": "rocksdb", "gcs_storage_path": STORE}


def boot(fresh: bool):
    if fresh and os.path.isdir(STORE):
        shutil.rmtree(STORE)
    os.makedirs(STORE, exist_ok=True)
    ray.init(num_cpus=4, include_dashboard=False, logging_level="ERROR",
             namespace="ledger", _system_config=dict(SYSCFG))
    node = ray._private.worker._global_node
    return node.session_name, node.gcs_address


def t1_kv_semantics(log) -> dict:
    """The footguns, measured rather than remembered."""
    out = {}
    ikv._internal_kv_del(b"t1", namespace=pl.NS)
    out["put_when_absent"] = ikv._internal_kv_put(b"t1", b"a", False, namespace=pl.NS)
    out["put_when_present"] = ikv._internal_kv_put(b"t1", b"b", False, namespace=pl.NS)
    out["value_after_failed_cas"] = ikv._internal_kv_get(b"t1", namespace=pl.NS).decode()
    out["cas_held"] = out["value_after_failed_cas"] == "a"
    out["return_value_is_inverted"] = (
        out["put_when_absent"] is False and out["put_when_present"] is True)
    for i in range(3):
        ikv._internal_kv_put(f"t1pfx/{i}".encode(), b"x", True, namespace=pl.NS)
    out["prefix_list"] = sorted(
        k.decode() for k in (ikv._internal_kv_list(b"t1pfx/", namespace=pl.NS) or []))
    out["prefix_list_works"] = len(out["prefix_list"]) == 3
    log(f"  put(absent)={out['put_when_absent']}  put(present)={out['put_when_present']}"
        f"  -> inverted={out['return_value_is_inverted']}  CAS held={out['cas_held']}")
    log(f"  prefix list -> {out['prefix_list']}")
    return out


def t2_detached_survives_driver(log, session, gcs) -> dict:
    """C4a: the coordinator outlives the process that made it."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_driver1.py")
    with open(script, "w") as f:
        f.write(
            "import ray, sys, os, json\n"
            "sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n"
            "import progress_ledger as pl\n"
            f"ray.init(address='{gcs}', namespace='ledger', logging_level='ERROR')\n"
            "c = pl.JobCoordinator.options(name='coord-A', lifetime='detached',\n"
            "    get_if_exists=True).remote('" + session + "', 'jobA', 40, 4, 3, 0.05)\n"
            "print('COORD', json.dumps(ray.get(c.info.remote())))\n"
            "c.run.remote()\n"
            "print('DRIVER1_LAUNCHED')\n"
            "sys.stdout.flush()\n"
            "os._exit(0)\n")
    env = dict(os.environ)
    r = subprocess.run([sys.executable, script], capture_output=True, text=True,
                       timeout=180, env=env)
    launched = "DRIVER1_LAUNCHED" in r.stdout
    log(f"  driver 1 launched the coordinator and exited: {launched}")
    if not launched:
        log(f"  driver 1 stdout: {r.stdout[-300:]}")
        log(f"  driver 1 stderr: {r.stderr[-600:]}")
    # let the coordinator get real work committed, so that "resume" is
    # observable rather than vacuous
    progressed = 0
    for _ in range(40):
        time.sleep(1)
        try:
            h = ray.get_actor("coord-A", namespace="ledger")
            i = ray.get(h.info.remote(), timeout=30)
            progressed = i["executed_this_incarnation"]
            if progressed >= 12:
                break
        except Exception:
            continue
    log(f"  coordinator progressed to {progressed} units with NO driver attached")
    handle = ray.get_actor("coord-A", namespace="ledger")
    info = ray.get(handle.info.remote(), timeout=60)
    log(f"  fresh driver reattached by name -> epoch={info['epoch']} "
        f"pid={info['pid']} executed={info['executed_this_incarnation']}")
    return {
        "driver1_exited_cleanly": launched,
        "units_executed_with_no_driver_attached": progressed,
        "driver1_stderr_tail": r.stderr[-400:],
        "reattached_by_name": True,
        "info_after_reattach": info,
    }


def t3_end_to_end_resume(log) -> dict:
    """Kill the coordinator mid-job; the next incarnation must resume."""
    handle = ray.get_actor("coord-A", namespace="ledger")
    before = ray.get(handle.info.remote(), timeout=60)
    ray.kill(handle, no_restart=False)  # transient failure, not a takedown
    log(f"  killed the coordinator (was at {before['executed_this_incarnation']} units)")
    after = None
    for _ in range(60):
        time.sleep(2)
        try:
            h = ray.get_actor("coord-A", namespace="ledger")
            after = ray.get(h.info.remote(), timeout=30)
            if after["pid"] != before["pid"]:
                break
        except Exception:
            continue
    log(f"  restarted incarnation: pid={after['pid'] if after else None} "
        f"epoch={after['epoch'] if after else None} "
        f"resumed_from={after['resumed_from'] if after else None}")
    done = None
    if after:
        h = ray.get_actor("coord-A", namespace="ledger")
        done = ray.get(h.run.remote(), timeout=300)
        log(f"  ran to completion: finished={done['finished']} "
            f"executed_this_incarnation={done['executed_this_incarnation']} "
            f"ledger_keys={done['ledger_keys']}")
    return {
        "before_kill": before, "after_restart": after, "final": done,
        "new_process": bool(after and after["pid"] != before["pid"]),
        "epoch_advanced": bool(after and after["epoch"] > before["epoch"]),
        "resumed_rather_than_restarted": bool(after and after["resumed_from"] > 0),
    }


def t4_restart_cluster_same_path(log, old_session) -> dict:
    """C21 / C12: does a new cluster on the same storage path inherit identity?"""
    ikv._internal_kv_put(b"survivor", b"written-before-restart", True, namespace=pl.NS)
    keys_before = sorted(k.decode() for k in
                         (ikv._internal_kv_list(b"", namespace=pl.NS) or []))
    ray.shutdown()
    time.sleep(3)
    new_session, _ = boot(fresh=False)  # SAME storage path -- the reused-PV case
    survivor = ikv._internal_kv_get(b"survivor", namespace=pl.NS)
    keys_after = sorted(k.decode() for k in
                        (ikv._internal_kv_list(b"", namespace=pl.NS) or []))
    on_disk = os.path.join(STORE, "session_name")
    disk_name = open(on_disk).read().strip() if os.path.exists(on_disk) else None
    out = {
        "old_session": old_session,
        "new_session": new_session,
        "session_name_inherited": old_session == new_session,
        "session_name_file_contents": disk_name,
        "kv_survived_restart": survivor is not None,
        "survivor_value": survivor.decode() if survivor else None,
        "keys_before": keys_before[:12], "n_keys_before": len(keys_before),
        "n_keys_after": len(keys_after),
        "prior_ledger_visible_to_new_cluster": any(
            k.startswith(f"{old_session}/") for k in keys_after),
    }
    log(f"  old session: {old_session}")
    log(f"  new session: {new_session}")
    log(f"  INHERITED: {out['session_name_inherited']}   "
        f"kv survived restart: {out['kv_survived_restart']}")
    log(f"  prior ledger visible to the new cluster: "
        f"{out['prior_ledger_visible_to_new_cluster']}  "
        f"({out['n_keys_before']} -> {out['n_keys_after']} keys)")
    return out


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n")
        logf.flush()

    res = {"ray_version": ray.__version__, "system_config": SYSCFG,
           "python": sys.version.split()[0]}
    log(f"stage C on Ray {ray.__version__}, gcs_storage=rocksdb at {STORE}")
    log("")
    session, gcs = boot(fresh=True)
    log(f"session_name: {session}   gcs: {gcs}")
    try:
        log(""); log("T1 -- internal_kv semantics"); res["t1"] = t1_kv_semantics(log)
        log(""); log("T2 -- C4a: detached coordinator survives driver death")
        res["t2"] = t2_detached_survives_driver(log, session, gcs)
        log(""); log("T3 -- end-to-end resume after coordinator kill")
        res["t3"] = t3_end_to_end_resume(log)
        log(""); log("T4 -- C21/C12: cluster restart on the same storage path")
        res["t4"] = t4_restart_cluster_same_path(log, session)
    except Exception as e:
        import traceback
        res["error"] = traceback.format_exc()
        log("ERROR:", traceback.format_exc()[-2000:])
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass

    t4 = res.get("t4", {})
    res["verdict"] = {
        "C8_zero_core_patches": "error" not in res,
        "C4a_survives_driver_death": bool(
            res.get("t2", {}).get("driver1_exited_cleanly")
            and res.get("t2", {}).get("units_executed_with_no_driver_attached", 0) > 0
            and res.get("t2", {}).get("reattached_by_name")),
        "C4a_resume_after_coordinator_kill": bool(
            res.get("t3", {}).get("new_process")
            and res.get("t3", {}).get("epoch_advanced")
            and res.get("t3", {}).get("resumed_rather_than_restarted")),
        "C21_session_name_stable": t4.get("session_name_inherited"),
        "C12_refuted_by_inherited_session": bool(
            t4.get("session_name_inherited")
            and t4.get("prior_ledger_visible_to_new_cluster")),
        "rocksdb_really_durable": t4.get("kv_survived_restart"),
        "inverted_put_return_confirmed": res.get("t1", {}).get("return_value_is_inverted"),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 70)
    for k, v in res["verdict"].items():
        log(f"  {k:42s} {v}")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
