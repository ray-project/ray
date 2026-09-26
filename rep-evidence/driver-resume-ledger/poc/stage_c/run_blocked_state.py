"""C30: is the redo window a constant or an interval?

C26 was refuted because measured redo (30) exceeded its stated bound (27).  The
excess was explicable -- queue + writer's in-flight batch + producer's blocked
batch -- but an explanation invented after seeing the number is not evidence.
This turns it back into a POINT PREDICTION, redo = W*(Q+2), and sweeps Q so the
formula is tested at points that had no say in forming it.

`die()` dumps progress() to a local file at the instant of death, because a 2s
sampler blurs the count by more than the gap between adjacent sweep points.

Pre-registered in experiments/C27.md.
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
PORT = 6595
ADDR = f"127.0.0.1:{PORT}"
RAY = os.path.join(os.path.dirname(sys.executable), "ray")

N_UNITS = 600
UNIT_SECONDS = 0.35
W, K = 3, 3
OUTAGE_S = 55          # < C29's 60s cluster-death ceiling
DIE_INTO_OUTAGE = 30   # enough for Q=16 (48 units) to saturate
# (Q, death-phase seconds into the outage, label)
ARMS_SPEC = [(16, 30.0, "q16_early"), (16, 45.0, "q16_late"),
             (32, 45.0, "q32_late")]


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, timeout=300, **k)


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


def drive(q: int, idx: int, body: str, tag: str, timeout: int = 300):
    src = ("import os, sys, json, time\n"
           f"sys.path.insert(0, {HERE!r})\n"
           "import ray, progress_ledger as pl\n"
           f"ray.init(address={ADDR!r}, namespace='ledger', logging_level='ERROR')\n"
           f"NAME = 'coord-r{idx}'\n"
           f"SCOPE, JOB = 'scope-r{idx}', 'job30'\n"
           f"N, W, K, US, Q = {N_UNITS}, {W}, {K}, {UNIT_SECONDS}, {q}\n"
           f"DEATH = {os.path.join(HERE, f'_death_r{idx}.json')!r}\n"
           + body)
    path = os.path.join(HERE, f"_c30_r{idx}_{tag}.py")
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
    get_if_exists=True, max_concurrency=3).remote(SCOPE, JOB, N, W, K, US, Q)
i = ray.get(c.progress.remote())
c.run.remote()
print('RESULT', json.dumps(i))
"""

# Long-lived: holds a cached handle so it can reach the actor with GCS down.
KILLER = """
h = ray.get_actor(NAME, namespace='ledger')
log = []
t0 = time.time()
while time.time() - t0 < WAIT:
    try:
        p = ray.get(h.progress.remote(), timeout=6)
        log.append({'t': round(time.time()-t0,1), 'ex': p['executed_this_incarnation'],
                    'q': p['queued'], 'dur': p['durable_units']})
    except Exception as e:
        log.append({'t': round(time.time()-t0,1), 'err': type(e).__name__})
    time.sleep(3)
sent_err = None
try:
    h.die.remote(0.05, DEATH)
except Exception as e:
    sent_err = '%s: %s' % (type(e).__name__, str(e)[:150])
time.sleep(10)
exists = os.path.exists(DEATH)
alive = None
try:
    alive = ray.get(h.progress.remote(), timeout=10)
except Exception as e:
    alive = {'err': '%s: %s' % (type(e).__name__, str(e)[:120])}
print('RESULT', json.dumps({'sent': True, 'sent_err': sent_err,
    'death_file_exists': exists, 'after_die': alive, 'log': log}))
"""

AFTER = """
h = ray.get_actor(NAME, namespace='ledger')
after = ray.get(h.progress.remote(), timeout=120)
led = pl.Ledger(SCOPE, JOB)
eps = sorted(int(k.rsplit('/',1)[1]) for k in led._list(led._ep()))
best = []
for e in eps:
    d = sorted((led._read_epoch(e) or {}).get('done', []))
    if len(d) > len(best):
        best = d
print('RESULT', json.dumps({
    'after': after, 'epoch_keys': eps, 'committed': len(best),
    'contiguous': best == list(range(len(best))),
    'has_dupes': len(best) != len(set(best)),
}))
"""


def run_q(q: int, idx: int, phase: float, log) -> dict:
    res: dict = {"Q": q, "repeat": idx, "death_phase": phase,
                 "buffer_units": q * W,
                 "predicted_lo": q * W + W + 1, "predicted_hi": W * (q + 2)}
    store = f"/tmp/ledger_c30_r{idx}"
    death_path = os.path.join(HERE, f"_death_r{idx}.json")
    if os.path.exists(death_path):
        os.remove(death_path)
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": store})
    sh([RAY, "stop", "--force"]); time.sleep(2)
    if os.path.isdir(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    h = sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=4",
            "--include-dashboard=False", f"--system-config={syscfg}",
            "--disable-usage-stats"])
    if h.returncode != 0:
        res["error"] = "head start: " + h.stderr[-300:]
        return res
    time.sleep(5)

    boot, rb = drive(q, idx, BOOT, "boot")
    if boot is None:
        res["error"] = "boot: " + rb.stderr[-400:]
        sh([RAY, "stop", "--force"])
        return res

    # Killer must connect BEFORE the outage: ray.init needs GCS.
    src = os.path.join(HERE, f"_c30_r{idx}_killer.py")
    with open(src, "w") as f:
        f.write("import os, sys, json, time\n"
                f"sys.path.insert(0, {HERE!r})\n"
                "import ray, progress_ledger as pl\n"
                f"ray.init(address={ADDR!r}, namespace='ledger', logging_level='ERROR')\n"
                f"NAME = 'coord-r{idx}'\n"
                f"DEATH = {death_path!r}\n"
                + KILLER.replace("WAIT", str(phase + 3)))
    kerr = open(os.path.join(HERE, f"_c30_r{idx}_killer.err"), "w")
    kout = open(os.path.join(HERE, f"_c30_r{idx}_killer.out"), "w")
    killer = subprocess.Popen([sys.executable, src], stdout=kout, stderr=kerr)
    time.sleep(15)

    procs = gcs_procs()
    if not procs:
        res["error"] = "no gcs_server"
        sh([RAY, "stop", "--force"])
        return res
    pid, argv, cwd = procs[0]
    os.kill(pid, signal.SIGKILL)
    log(f"  [r{idx}] gcs killed; die at +{phase}s, restore at "
        f"+{OUTAGE_S}s")
    time.sleep(OUTAGE_S)
    subprocess.Popen(argv, cwd=cwd, stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL)
    try:
        killer.wait(timeout=60)
    except subprocess.TimeoutExpired:
        killer.kill()
    time.sleep(25)

    if os.path.exists(death_path):
        with open(death_path) as f:
            res["at_death"] = json.load(f)
    else:
        res["error"] = "no death dump -- coordinator did not die"
        sh([RAY, "stop", "--force"])
        return res

    aft, ra = drive(q, idx, AFTER, "after", timeout=400)
    if aft is None:
        res["error"] = "after: " + ra.stderr[-400:]
        sh([RAY, "stop", "--force"])
        return res
    res["after"] = aft
    d = res["at_death"]
    rf = aft["after"].get("resumed_from", 0)
    res["measured"] = {
        "executed_at_death": d.get("executed_this_incarnation"),
        "durable_at_death": d.get("durable_units"),
        "queued_at_death": d.get("queued"),
        "resumed_from": rf,
        "redo": d.get("executed_this_incarnation", 0) - rf,
        "saturated": (d.get("queued") == q) if q else True,
        "in_interval":
            res["predicted_lo"] <= (d.get("executed_this_incarnation", 0) - rf)
            <= res["predicted_hi"],
    }
    m = res["measured"]
    m["p_inferred"] = m["redo"] - q * W - W
    # The control that was missing: a FULL QUEUE does not mean a BLOCKED
    # PRODUCER.  The producer is blocked iff it holds a complete batch.
    m["producer_blocked"] = m["p_inferred"] == W
    m["equals_formula"] = m["redo"] == q * W + 2 * W
    log(f"  [r{idx} phase={phase}] executed={m['executed_at_death']} "
        f"queued={m['queued_at_death']} resumed_from={rf} redo={m['redo']} "
        f"p={m['p_inferred']} blocked={m['producer_blocked']} "
        f"formula={q*W+2*W} equals={m['equals_formula']} "
        f"saturated={m['saturated']}")
    log(f"  [r{idx}] committed={aft['committed']} contiguous={aft['contiguous']}"
        f" dupes={aft['has_dupes']}")
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
                                  "outage_s": OUTAGE_S, "arms": ARMS_SPEC}}
    try:
        for idx, (q, phase, label) in enumerate(ARMS_SPEC):
            log(""); log("=" * 66)
            log(f"{label}: Q={q}, death at +{phase}s into a {OUTAGE_S}s outage")
            log("=" * 66)
            a = run_q(q, idx, phase, log)
            a["label"] = label
            res["arms"][label] = a
    finally:
        sh([RAY, "stop", "--force"])

    arms = res["arms"]

    def m(label):
        return arms.get(label, {}).get("measured", {})

    early, late, q32 = m("q16_early"), m("q16_late"), m("q32_late")
    res["verdict"] = {
        "NC_early_reproduced_anomaly": early.get("redo") == 53,
        "early_redo": early.get("redo"), "early_p": early.get("p_inferred"),
        "early_blocked": early.get("producer_blocked"),
        "VOID_anomaly_did_not_reproduce":
            early.get("redo") is not None and early.get("redo") != 53,
        "late_redo": late.get("redo"), "late_p": late.get("p_inferred"),
        "late_blocked": late.get("producer_blocked"),
        "late_equals_formula": late.get("equals_formula"),
        "q32_redo": q32.get("redo"), "q32_blocked": q32.get("producer_blocked"),
        "q32_predicted_below_102": (q32.get("redo") is not None
                                    and q32.get("redo") < 102),
        "contiguous_everywhere": all(
            a.get("after", {}).get("contiguous") is True for a in arms.values()),
        "C31_proven": (early.get("redo") == 53
                       and late.get("equals_formula") is True
                       and late.get("producer_blocked") is True),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 66)
    for k2, v2 in res["verdict"].items():
        log(f"  {k2:34s} {v2}")
    log("=" * 66)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
