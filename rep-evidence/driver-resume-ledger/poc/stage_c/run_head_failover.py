"""C4 (head half) / C7 / C21 (head-restart half): kill the head, keep the work.

A real multi-process cluster: `ray start --head` plus a second raylet, with the
coordinator pinned to the **worker** node so that killing the head does not kill
it by construction. That placement is the design's own preference (open question
4 in design.md) and it is the only placement under which this question is even
meaningful.
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

STORE = "/tmp/ledger_headfail"
PORT = 6579
RAY = os.path.join(os.path.dirname(sys.executable), "ray")
SYSCFG = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": STORE})
ADDR = f"127.0.0.1:{PORT}"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, timeout=300, **kw)


def start_head():
    return sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=2",
               "--include-dashboard=False", f"--system-config={SYSCFG}",
               "--disable-usage-stats"])


def start_worker():
    return sh([RAY, "start", f"--address={ADDR}", "--num-cpus=2",
               "--disable-usage-stats"])


def gcs_pids():
    out = sh(["pgrep", "-f", "gcs_server"])
    return [int(p) for p in out.stdout.split() if p.strip().isdigit()]


def client(code: str, timeout: int = 240):
    """Run a snippet as a separate driver process against the cluster."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_client.py")
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


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n"); logf.flush()

    res = {}
    sh([RAY, "stop", "--force"])
    time.sleep(2)
    if os.path.isdir(STORE):
        shutil.rmtree(STORE)
    os.makedirs(STORE, exist_ok=True)

    try:
        log("starting head + worker")
        h = start_head()
        if h.returncode != 0:
            log("head failed:", h.stderr[-800:])
            res["error"] = "head start failed"
            raise SystemExit(0)
        time.sleep(4)
        w = start_worker()
        log(f"  head rc={h.returncode} worker rc={w.returncode}")

        sess1, _ = client(
            "n=[x for x in ray.nodes() if x['Alive']]\n"
            "print('RESULT', json.dumps({'session': ray._private.worker."
            "_global_node.session_name, 'nodes': len(n)}))\n")
        log(f"  session={sess1['session']} nodes={sess1['nodes']}")
        res["before"] = sess1

        log("")
        log("creating a NON-HEAD-PINNED detached coordinator on the worker node")
        created, r = client(
            "from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy\n"
            "head_ip = ray.util.get_node_ip_address()\n"
            "nodes=[x for x in ray.nodes() if x['Alive']]\n"
            "head_id = ray.get_runtime_context().get_node_id()\n"
            "worker=[x for x in nodes if x['NodeID']!=head_id]\n"
            "target = (worker or nodes)[0]['NodeID']\n"
            "c = pl.JobCoordinator.options(name='coord-H', lifetime='detached',\n"
            "  get_if_exists=True, scheduling_strategy=NodeAffinitySchedulingStrategy(\n"
            "    node_id=target, soft=False)).remote('scope-1','jobH',60,3,3,0.35)\n"
            "i = ray.get(c.info.remote())\n"
            "c.run.remote()\n"
            "time.sleep(12)\n"
            "i2 = ray.get(c.info.remote())\n"
            "print('RESULT', json.dumps({'target_node': target, 'head_node': head_id,\n"
            "  'off_head': target!=head_id, 'info': i2}))\n")
        if created is None:
            log("  client failed:", r.stderr[-800:])
            res["error"] = "coordinator creation failed"
            raise SystemExit(0)
        log(f"  off-head placement: {created['off_head']}  "
            f"progress: {created['info']['executed_this_incarnation']} units")
        res["coordinator_created"] = created

        log("")
        log("KILLING THE HEAD (gcs_server)")
        pids = gcs_pids()
        for p in pids:
            try:
                os.kill(p, signal.SIGKILL)
            except ProcessLookupError:
                pass
        log(f"  killed gcs_server pids {pids}")
        res["killed_gcs_pids"] = pids
        outage_start = time.time()
        time.sleep(10)
        res["gcs_alive_during_outage"] = bool(gcs_pids())

        log("")
        log("restarting the head on the SAME storage path and port")
        sh([RAY, "stop", "--force"])
        time.sleep(3)
        h2 = start_head()
        time.sleep(6)
        res["outage_seconds"] = round(time.time() - outage_start, 1)
        log(f"  head restart rc={h2.returncode}, outage {res['outage_seconds']}s")

        after, r = client(
            "s = ray._private.worker._global_node.session_name\n"
            "led = pl.Ledger('scope-1','jobH')\n"
            "st = led._read_epoch(1) or {}\n"
            "eps = sorted(int(k.rsplit('/',1)[1]) for k in led._list(led._ep()))\n"
            "names = []\n"
            "try:\n"
            "    names = [n for n in ray.util.list_named_actors(all_namespaces=True)]\n"
            "except Exception as e:\n"
            "    names = ['<err %s>' % type(e).__name__]\n"
            "alive = None\n"
            "try:\n"
            "    h = ray.get_actor('coord-H', namespace='ledger')\n"
            "    alive = ray.get(h.info.remote(), timeout=45)\n"
            "except Exception as e:\n"
            "    alive = {'error': '%s: %s' % (type(e).__name__, str(e)[:200])}\n"
            "print('RESULT', json.dumps({'session': s, 'epochs': eps,\n"
            "  'epoch1_state_units': len(st.get('done',[])), 'named': names[:8],\n"
            "  'reattach': alive}))\n", timeout=300)
        if after is None:
            log("  post-restart client failed:", r.stderr[-1000:])
            res["error_after"] = r.stderr[-1000:]
        else:
            log(f"  session after restart: {after['session']}")
            log(f"  session unchanged: {after['session'] == sess1['session']}")
            log(f"  ledger epochs present: {after['epochs']}  "
                f"epoch-1 committed units: {after['epoch1_state_units']}")
            log(f"  named actors visible: {after['named']}")
            log(f"  reattach: {json.dumps(after['reattach'])[:300]}")
            res["after_restart"] = after
    finally:
        sh([RAY, "stop", "--force"])

    a = res.get("after_restart", {})
    committed = a.get("epoch1_state_units", 0)
    reattached = isinstance(a.get("reattach"), dict) and "error" not in a["reattach"]
    res["verdict"] = {
        "C21_session_name_survives_real_head_restart": bool(
            a and a.get("session") == res.get("before", {}).get("session")),
        "ledger_survived_head_loss": committed > 0,
        "committed_units_recovered": committed,
        "actor_reattachable_after_head_restart": reattached,
        "coordinator_progressed_off_head_before_kill":
            res.get("coordinator_created", {}).get("info", {})
               .get("executed_this_incarnation", 0) > 0,
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 70)
    for k, v in res["verdict"].items():
        log(f"  {k:52s} {v}")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
