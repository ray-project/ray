"""C25: three-arm pinning experiment.

Iteration 12 showed a hard-pinned coordinator becoming *unschedulable* after its
node disappeared, while its name, epoch and ledger all survived.  One run cannot
tell "hard pinning is the cause" from "it was unrecoverable anyway and the
affinity error was just the first exception on the way out".

So run the same head-failover scenario three times, changing exactly one line:

    hard  NodeAffinitySchedulingStrategy(node_id=<worker>, soft=False)
    soft  NodeAffinitySchedulingStrategy(node_id=<worker>, soft=True)
    none  no scheduling strategy at all

`hard` is the NEGATIVE CONTROL and is required to fail again.  If it succeeds,
the run is VOID and nothing may be concluded from the other two arms either.

Pre-registered in experiments/C25.md.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PORT = 6581
RAY = os.path.join(os.path.dirname(sys.executable), "ray")
ADDR = f"127.0.0.1:{PORT}"
ARMS = ("hard", "soft", "none")

PLACEMENT = {
    "hard": ("from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy\n",
             "scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=target, soft=False),"),
    "soft": ("from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy\n",
             "scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=target, soft=True),"),
    "none": ("", ""),
}


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, timeout=300, **kw)


def gcs_pids():
    out = sh(["pgrep", "-f", "gcs_server"])
    return [int(p) for p in out.stdout.split() if p.strip().isdigit()]


def client(code: str, timeout: int = 240):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_client_c25.py")
    with open(path, "w") as f:
        f.write("import os, sys, json, time\n"
                "sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n"
                "import ray, progress_ledger as pl\n"
                f"ray.init(address='{ADDR}', namespace='ledger', logging_level='ERROR')\n"
                + code)
    r = subprocess.run([sys.executable, path], capture_output=True, text=True,
                       timeout=timeout)
    for line in r.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[7:]), r
    return None, r


def run_arm(arm: str, log) -> dict:
    """One complete head-failover cycle for one placement mode."""
    store = f"/tmp/ledger_pin_{arm}"
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": store})
    name = f"coord-{arm}"
    res: dict = {"arm": arm}

    sh([RAY, "stop", "--force"])
    time.sleep(2)
    if os.path.isdir(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)

    def start_head():
        return sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=2",
                   "--include-dashboard=False", f"--system-config={syscfg}",
                   "--disable-usage-stats"])

    h = start_head()
    if h.returncode != 0:
        res["error"] = "head start failed: " + h.stderr[-400:]
        return res
    time.sleep(4)
    sh([RAY, "start", f"--address={ADDR}", "--num-cpus=2", "--disable-usage-stats"])
    time.sleep(2)

    imports, strategy = PLACEMENT[arm]
    created, r = client(
        imports +
        "nodes=[x for x in ray.nodes() if x['Alive']]\n"
        "head_id = ray.get_runtime_context().get_node_id()\n"
        "worker=[x for x in nodes if x['NodeID']!=head_id]\n"
        "target = (worker or nodes)[0]['NodeID']\n"
        f"c = pl.JobCoordinator.options(name='{name}', lifetime='detached',\n"
        f"  get_if_exists=True, {strategy}\n"
        f"  ).remote('scope-{arm}','job{arm}',60,3,3,0.35)\n"
        "ray.get(c.info.remote())\n"
        "c.run.remote()\n"
        "time.sleep(12)\n"
        "i2 = ray.get(c.info.remote())\n"
        "try:\n"
        "    placed = ray.get(c.__ray_call__.remote(lambda s: __import__('ray')"
        ".get_runtime_context().get_node_id()))\n"
        "except Exception:\n"
        "    placed = '<unknown>'\n"
        "print('RESULT', json.dumps({'target_node': target, 'head_node': head_id,\n"
        "  'placed_node': placed, 'placed_off_head': placed != head_id,\n"
        "  'info': i2}))\n")
    if created is None:
        res["error"] = "coordinator creation failed: " + r.stderr[-600:]
        sh([RAY, "stop", "--force"])
        return res
    res["before"] = created
    log(f"  [{arm}] placed off-head={created['placed_off_head']} "
        f"progress={created['info']['executed_this_incarnation']} units")

    pids = gcs_pids()
    for p in pids:
        try:
            os.kill(p, signal.SIGKILL)
        except ProcessLookupError:
            pass
    log(f"  [{arm}] killed gcs_server {pids}")
    time.sleep(8)
    sh([RAY, "stop", "--force"])
    time.sleep(3)
    h2 = start_head()
    time.sleep(6)
    log(f"  [{arm}] head restarted rc={h2.returncode}")

    after, r = client(
        "out = {}\n"
        "out['session'] = ray._private.worker._global_node.session_name\n"
        f"led = pl.Ledger('scope-{arm}','job{arm}')\n"
        "st = led._read_epoch(1) or {}\n"
        "out['epoch1_state_units'] = len(st.get('done',[]))\n"
        "try:\n"
        "    out['named'] = list(ray.util.list_named_actors(all_namespaces=True))[:8]\n"
        "except Exception as e:\n"
        "    out['named'] = ['<err %s>' % type(e).__name__]\n"
        "try:\n"
        f"    h = ray.get_actor('{name}', namespace='ledger')\n"
        "    out['reattach'] = ray.get(h.info.remote(), timeout=60)\n"
        "except Exception as e:\n"
        "    out['reattach'] = {'error': '%s: %s' % (type(e).__name__, str(e)[:250])}\n"
        "print('RESULT', json.dumps(out))\n", timeout=300)
    sh([RAY, "stop", "--force"])
    if after is None:
        res["error_after"] = r.stderr[-800:]
        return res
    res["after"] = after
    ra = after["reattach"]
    ok = isinstance(ra, dict) and "error" not in ra
    res["reattached"] = ok
    res["resumed_from"] = ra.get("resumed_from") if ok else None
    res["reattach_error"] = None if ok else ra.get("error")
    log(f"  [{arm}] name_present={any(name in str(n) for n in after['named'])} "
        f"committed={after['epoch1_state_units']} reattached={ok} "
        f"resumed_from={res['resumed_from']}")
    if not ok:
        log(f"  [{arm}] error: {res['reattach_error']}")
    return res


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n"); logf.flush()

    res = {"arms": {}}
    try:
        for arm in ARMS:
            log("")
            log("=" * 68)
            log(f"ARM: {arm}")
            log("=" * 68)
            res["arms"][arm] = run_arm(arm, log)
    finally:
        sh([RAY, "stop", "--force"])

    a = res["arms"]
    hard_ok = a.get("hard", {}).get("reattached")
    soft_ok = a.get("soft", {}).get("reattached")
    none_ok = a.get("none", {}).get("reattached")
    # A reattach that succeeds but resumes from zero is WORSE than a failure:
    # it is silent redo.  Treat it as a non-success for the verdict.
    def good(arm):
        r = a.get(arm, {})
        return bool(r.get("reattached")) and (r.get("resumed_from") or 0) > 0

    res["verdict"] = {
        "control_hard_failed_as_required": hard_ok is False,
        "soft_reattached": soft_ok,
        "none_reattached": none_ok,
        "soft_reattached_with_progress": good("soft"),
        "none_reattached_with_progress": good("none"),
        "VOID_control_did_not_fire": hard_ok is True,
        "C25_proven": hard_ok is False and (good("soft") or good("none")),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 68)
    for k, v in res["verdict"].items():
        log(f"  {k:44s} {v}")
    log("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
