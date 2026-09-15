"""C28: does a crash during a GCS outage cause a restart storm?

Found by accident in runs/20260910-074504-C26async3, where the surviving epoch
key was numbered 18 after a single 35s outage -- roughly eighteen incarnations
where one was expected.  One data point cannot tell "18 is a constant" from
"18 is proportional to 35 seconds", so this sweeps the outage duration and looks
at the SHAPE.

Arms: die_healthy (0s, the negative control), outage_10, outage_30, outage_60.

Pre-registered in experiments/C28.md.
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

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 6591
ADDR = f"127.0.0.1:{PORT}"
RAY = os.path.join(os.path.dirname(sys.executable), "ray")

N_UNITS = 600
UNIT_SECONDS = 0.35
W, K = 3, 3
DIE_OFFSET = 6       # seconds into the outage at which the coordinator dies
SETTLE = 45          # seconds to watch after GCS returns

ARMS = {"die_healthy": 0, "outage_10": 10, "outage_30": 30, "outage_60": 60}


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, timeout=300, **kw)


def gcs_procs():
    out = sh(["pgrep", "-f", "gcs_server"])
    procs = []
    for tok in out.stdout.split():
        if not tok.strip().isdigit():
            continue
        pid = int(tok)
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                argv = [a.decode() for a in f.read().split(b"\0") if a]
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except (OSError, ProcessLookupError):
            continue
        if argv and "gcs_server" in argv[0]:
            procs.append((pid, argv, cwd))
    return procs


def drive(arm: str, body: str, tag: str, timeout: int = 300):
    src = ("import os, sys, json, time\n"
           f"sys.path.insert(0, {HERE!r})\n"
           "import ray, progress_ledger as pl\n"
           f"ray.init(address={ADDR!r}, namespace='ledger', logging_level='ERROR')\n"
           f"NAME = 'coord-' + {arm!r}\n"
           f"SCOPE, JOB = 'scope-' + {arm!r}, 'job28'\n"
           f"N, W, K, US = {N_UNITS}, {W}, {K}, {UNIT_SECONDS}\n"
           + body)
    path = os.path.join(HERE, f"_c28_{arm}_{tag}.py")
    with open(path, "w") as f:
        f.write(src)
    p = subprocess.run([sys.executable, path], capture_output=True, text=True,
                       timeout=timeout)
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[7:]), p
    return None, p


BOOT = """
c = pl.JobCoordinator.options(name=NAME, lifetime='detached',
    get_if_exists=True, max_concurrency=3).remote(SCOPE, JOB, N, W, K, US, 0)
i = ray.get(c.progress.remote())
c.run.remote()
print('RESULT', json.dumps(i))
"""

SAMPLE = """
h = ray.get_actor(NAME, namespace='ledger')
out = open(OUT, 'w')
t0 = time.time()
killed = False
while time.time() - t0 < DUR:
    rec = {'t': round(time.time() - t0, 2)}
    try:
        rec['p'] = ray.get(h.progress.remote(), timeout=6)
    except Exception as e:
        rec['error'] = '%s: %s' % (type(e).__name__, str(e)[:110])
    out.write(json.dumps(rec) + '\\n'); out.flush()
    if not killed and time.time() - t0 >= DIE_AT:
        killed = True
        try:
            h.die.remote()   # cached handle: reaches the worker without GCS
        except Exception as e:
            out.write(json.dumps({'t': round(time.time()-t0,2),
                                  'die_error': str(e)[:120]}) + '\\n')
        out.write(json.dumps({'t': round(time.time()-t0,2),
                              'died': True}) + '\\n')
        out.flush()
    time.sleep(2)
out.close()
print('RESULT', json.dumps({'done': True}))
"""

FINAL = """
out = {}
try:
    import ray._private.state as st
    rows = [a for a in st.actors().values() if a.get('Name') == NAME]
    out['actor_rows'] = [{'state': a.get('State'),
                          'restarts': a.get('NumRestarts')} for a in rows]
    out['num_restarts'] = max([int(a.get('NumRestarts') or 0) for a in rows]
                              or [0])
    out['live_rows'] = sum(1 for a in rows if a.get('State') == 'ALIVE')
except Exception as e:
    out['actor_rows'] = '<err %s: %s>' % (type(e).__name__, str(e)[:100])
try:
    h = ray.get_actor(NAME, namespace='ledger')
    out['final'] = ray.get(h.progress.remote(), timeout=90)
except Exception as e:
    out['final'] = {'error': '%s: %s' % (type(e).__name__, str(e)[:150])}
led = pl.Ledger(SCOPE, JOB)
eps = sorted(int(k.rsplit('/', 1)[1]) for k in led._list(led._ep()))
out['epoch_keys'] = eps
out['max_epoch'] = max(eps) if eps else None
out['ledger_key_count'] = len(led._list(led.root))
best = []
for e in eps:
    d = sorted((led._read_epoch(e) or {}).get('done', []))
    if len(d) > len(best):
        best = d
out['committed'] = len(best)
out['contiguous'] = best == list(range(len(best)))
out['has_dupes'] = len(best) != len(set(best))
print('RESULT', json.dumps(out))
"""


def run_arm(arm: str, outage: int, log) -> dict:
    res: dict = {"arm": arm, "outage_s": outage}
    store = f"/tmp/ledger_c28_{arm}"
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": store})
    sh([RAY, "stop", "--force"]); time.sleep(2)
    if os.path.isdir(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    h = sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=4",
            "--include-dashboard=False", f"--system-config={syscfg}",
            "--disable-usage-stats"])
    if h.returncode != 0:
        res["error"] = "head start failed: " + h.stderr[-400:]
        return res
    time.sleep(5)

    boot, rb = drive(arm, BOOT, "boot")
    if boot is None:
        res["error"] = "boot: " + rb.stderr[-500:]
        sh([RAY, "stop", "--force"])
        return res

    samples_path = os.path.join(HERE, f"_c28_samples_{arm}.jsonl")
    total = 15 + outage + SETTLE + 10
    body = (SAMPLE.replace("OUT", repr(samples_path))
                  .replace("DUR", str(total))
                  .replace("DIE_AT", str(15 + DIE_OFFSET)))
    src = os.path.join(HERE, f"_c28_{arm}_sampler.py")
    with open(src, "w") as f:
        f.write("import os, sys, json, time\n"
                f"sys.path.insert(0, {HERE!r})\n"
                "import ray, progress_ledger as pl\n"
                f"ray.init(address={ADDR!r}, namespace='ledger', logging_level='ERROR')\n"
                f"NAME = 'coord-' + {arm!r}\n" + body)
    sampler = subprocess.Popen([sys.executable, src], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
    time.sleep(15)

    if outage > 0:
        procs = gcs_procs()
        if not procs:
            res["error"] = "no gcs_server found"
            sh([RAY, "stop", "--force"])
            return res
        pid, argv, cwd = procs[0]
        os.kill(pid, signal.SIGKILL)
        log(f"  [{arm}] SIGKILL gcs_server {pid}; die at +{DIE_OFFSET}s, "
            f"outage {outage}s")
        time.sleep(outage)
        subprocess.Popen(argv, cwd=cwd, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
        log(f"  [{arm}] gcs_server restored")
    else:
        log(f"  [{arm}] control: die with GCS healthy, no outage")
        time.sleep(DIE_OFFSET + 4)

    time.sleep(SETTLE)
    try:
        sampler.wait(timeout=90)
    except subprocess.TimeoutExpired:
        sampler.kill()

    samples = []
    if os.path.exists(samples_path):
        with open(samples_path) as f:
            for line in f:
                try:
                    samples.append(json.loads(line))
                except ValueError:
                    pass
    res["samples"] = samples

    # Incarnation boundaries are pid changes; redo per incarnation is the work
    # it executed and never got durable.
    ok = [s for s in samples if "p" in s]
    incs, cur = [], None
    for s in ok:
        p = s["p"]
        if cur is None or p.get("pid") != cur["pid"]:
            cur = {"pid": p.get("pid"), "epoch": p.get("epoch"),
                   "resumed_from": p.get("resumed_from", 0),
                   "first_t": s["t"], "last_t": s["t"],
                   "executed": p.get("executed_this_incarnation", 0),
                   "durable": p.get("durable_units", 0)}
            incs.append(cur)
        else:
            cur["last_t"] = s["t"]
            cur["executed"] = p.get("executed_this_incarnation", 0)
            cur["durable"] = p.get("durable_units", 0)
            cur["epoch"] = p.get("epoch")
    for i in incs:
        i["redo"] = max(0, i["executed"] - max(0, i["durable"] - i["resumed_from"]))
    res["incarnations"] = incs
    res["observed_incarnations"] = len(incs)
    res["sampler_errors"] = sum(1 for s in samples if "error" in s)

    fin, rf = drive(arm, FINAL, "final")
    res["final"] = fin if fin is not None else {"error": rf.stderr[-400:]}
    if fin:
        log(f"  [{arm}] max_epoch={fin['max_epoch']} "
            f"num_restarts={fin.get('num_restarts')} "
            f"ledger_keys={fin['ledger_key_count']} "
            f"epoch_keys={len(fin['epoch_keys'])} "
            f"committed={fin['committed']} contiguous={fin['contiguous']}")
        log(f"  [{arm}] observed incarnations in samples: "
            f"{res['observed_incarnations']}, sampler errors: "
            f"{res['sampler_errors']}")
    sh([RAY, "stop", "--force"])
    return res


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n"); logf.flush()

    res = {"arms": {}, "config": {"N": N_UNITS, "W": W, "K": K,
                                  "unit_seconds": UNIT_SECONDS,
                                  "die_offset": DIE_OFFSET, "settle": SETTLE}}
    try:
        for arm, outage in ARMS.items():
            log(""); log("=" * 68); log(f"ARM: {arm}  (outage {outage}s)")
            log("=" * 68)
            res["arms"][arm] = run_arm(arm, outage, log)
    finally:
        sh([RAY, "stop", "--force"])

    def f(arm, key, default=None):
        return res["arms"].get(arm, {}).get("final", {}).get(key, default)

    ctrl_epoch = f("die_healthy", "max_epoch")
    ctrl_restarts = f("die_healthy", "num_restarts")
    epochs = {a: f(a, "max_epoch") for a in ARMS}
    keys = {a: f(a, "ledger_key_count") for a in ARMS}
    restarts = {a: f(a, "num_restarts") for a in ARMS}
    # Shape test: does the epoch counter track outage duration?
    pairs = [(ARMS[a], epochs[a]) for a in ARMS if epochs[a] is not None]
    scales = (len(pairs) >= 3
              and epochs.get("outage_60") is not None
              and epochs.get("outage_10") is not None
              and epochs["outage_60"] >= 2 * max(1, epochs["outage_10"]))
    res["verdict"] = {
        "NC_control_single_restart": ctrl_epoch == 2 and (ctrl_restarts or 0) <= 1,
        "control_max_epoch": ctrl_epoch,
        "control_num_restarts": ctrl_restarts,
        "VOID_control_churned": (ctrl_epoch or 0) > 2,
        "epochs_by_arm": epochs,
        "restarts_by_arm": restarts,
        "clause2_epochs_scale_with_outage": scales,
        "ledger_keys_by_arm": keys,
        "clause1_keys_bounded": (
            all(v is not None and v <= 8 for v in keys.values())),
        "clause3_redo_by_arm": {
            a: sum(i["redo"] for i in res["arms"].get(a, {})
                   .get("incarnations", []))
            for a in ARMS},
        "contiguous_everywhere": all(f(a, "contiguous") is True for a in ARMS),
        "dupes_anywhere": any(f(a, "has_dupes") is True for a in ARMS),
        "C28_proven": (ctrl_epoch == 2 and not scales
                       and all(v is not None and v <= 8 for v in keys.values())),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f2:
        json.dump(res, f2, indent=2, default=str)
    log(""); log("=" * 68)
    for k2, v2 in res["verdict"].items():
        log(f"  {k2:38s} {v2}")
    log("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
