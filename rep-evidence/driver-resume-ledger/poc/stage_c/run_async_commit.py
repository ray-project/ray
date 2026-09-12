"""C26: does an async ledger writer buy liveness, and at what price?

C7 was refuted because commit() is a synchronous internal_kv put on the unit
loop's critical path, so a GCS outage froze the coordinator outright.  C26 is the
other branch of that fork: put the commit behind a bounded FIFO drained by a
single writer thread.

Three clauses, measured separately (the rule this loop adopted after C1->C16):
  1. work continues during an outage, by about Q units and then blocks
  2. the redo window is bounded by Q + W, not by W
  3. commits stay contiguous and duplicate-free

Pre-registered in experiments/C26.md.
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
PORT = 6589
ADDR = f"127.0.0.1:{PORT}"
RAY = os.path.join(os.path.dirname(sys.executable), "ray")

N_UNITS = 400
UNIT_SECONDS = 0.35
W = 3
K = 3
OUTAGE_S = 35

# Q is in BATCHES, so buffer capacity in UNITS is Q*W.  Run 1 used Q=64 = 192
# units of buffer against a 35 s outage that only produces ~98 units, so the
# queue never approached its bound and clause 2 was starved.  Q=8 gives 24 units
# of buffer, which must saturate within ~9 s of the outage.
Q_SMALL = 8

ARMS = ("async_nokill", "async_outage", "async_crash_outage", "sync_crash_outage")


def q_for(arm: str) -> int:
    return 0 if arm.startswith("sync") else Q_SMALL


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
           f"ARM, NAME = {arm!r}, 'coord-' + {arm!r}\n"
           f"SCOPE, JOB = 'scope-' + {arm!r}, 'job26'\n"
           f"N, W, K, US = {N_UNITS}, {W}, {K}, {UNIT_SECONDS}\n"
           f"Q = {q_for(arm)}\n"
           + body)
    path = os.path.join(HERE, f"_c26_{arm}_{tag}.py")
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

# Long-lived sampler: must pre-exist a GCS outage, since ray.init needs GCS.
SAMPLE = """
h = ray.get_actor(NAME, namespace='ledger')
out = open(OUT, 'w')
t0 = time.time()
killed = False
while time.time() - t0 < DUR:
    rec = {'t': round(time.time() - t0, 2)}
    try:
        rec['p'] = ray.get(h.progress.remote(), timeout=8)
    except Exception as e:
        rec['error'] = '%s: %s' % (type(e).__name__, str(e)[:100])
    out.write(json.dumps(rec) + '\\n'); out.flush()
    if DIE_AT and not killed and time.time() - t0 >= DIE_AT:
        # Cached handle reaches the worker directly, so this works even with
        # GCS down -- which is the only moment a backlog exists.
        killed = True
        try:
            h.die.remote()
        except Exception:
            pass
        rec2 = {'t': round(time.time() - t0, 2), 'died': True}
        out.write(json.dumps(rec2) + '\\n'); out.flush()
    time.sleep(2)
out.close()
print('RESULT', json.dumps({'done': True}))
"""

# Kill the coordinator mid-flight, then start a fresh incarnation and see how
# much it has to redo.
REDO = """
# The coordinator died mid-outage via die(); max_restarts=-1 means Ray brings
# it back on its own, which IS the resume path under test.
h = ray.get_actor(NAME, namespace='ledger')
after = ray.get(h.progress.remote(), timeout=120)
print('RESULT', json.dumps({'after': after}))
"""

FINAL = """
out = {}
try:
    h = ray.get_actor(NAME, namespace='ledger')
    out['final'] = ray.get(h.progress.remote(), timeout=60)
except Exception as e:
    out['final'] = {'error': '%s: %s' % (type(e).__name__, str(e)[:150])}
led = pl.Ledger(SCOPE, JOB)
eps = sorted(int(k.rsplit('/',1)[1]) for k in led._list(led._ep()))
best = []
for e in eps:
    st = led._read_epoch(e) or {}
    d = sorted(st.get('done', []))
    if len(d) > len(best):
        best = d
out['epochs'] = eps
out['committed'] = len(best)
out['has_dupes'] = len(best) != len(set(best))
out['contiguous'] = best == list(range(len(best)))
print('RESULT', json.dumps(out))
"""


def start_cluster(store: str):
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": store})
    sh([RAY, "stop", "--force"]); time.sleep(2)
    if os.path.isdir(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    h = sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=4",
            "--include-dashboard=False", f"--system-config={syscfg}",
            "--disable-usage-stats"])
    time.sleep(5)
    return h


def run_arm(arm: str, log) -> dict:
    res: dict = {"arm": arm, "Q": q_for(arm),
                 "buffer_units": q_for(arm) * W}
    h = start_cluster(f"/tmp/ledger_c26_{arm}")
    if h.returncode != 0:
        res["error"] = "head start failed: " + h.stderr[-400:]
        return res
    boot, rb = drive(arm, BOOT, "boot")
    if boot is None:
        res["error"] = "boot: " + rb.stderr[-500:]
        sh([RAY, "stop", "--force"])
        return res
    log(f"  [{arm}] booted, Q={res['Q']}")

    die_at = 35 if "crash" in arm else 0
    if True:
        samples_path = os.path.join(HERE, f"_c26_samples_{arm}.jsonl")
        body = (SAMPLE.replace("OUT", repr(samples_path))
                      .replace("DUR", "150")
                      .replace("DIE_AT", str(die_at)))
        src = os.path.join(HERE, f"_c26_{arm}_sampler.py")
        with open(src, "w") as f:
            f.write("import os, sys, json, time\n"
                    f"sys.path.insert(0, {HERE!r})\n"
                    "import ray, progress_ledger as pl\n"
                    f"ray.init(address={ADDR!r}, namespace='ledger', logging_level='ERROR')\n"
                    f"NAME = 'coord-' + {arm!r}\n" + body)
        sampler = subprocess.Popen([sys.executable, src],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
        time.sleep(15)
        if arm.endswith("outage"):
            procs = gcs_procs()
            if not procs:
                res["error"] = "no gcs_server found"
                sh([RAY, "stop", "--force"])
                return res
            pid, argv, cwd = procs[0]
            os.kill(pid, signal.SIGKILL)
            log(f"  [{arm}] SIGKILL gcs_server {pid}")
            time.sleep(OUTAGE_S)
            subprocess.Popen(argv, cwd=cwd, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
            log(f"  [{arm}] gcs_server re-exec'd after {OUTAGE_S}s")
            time.sleep(10)
        else:
            time.sleep(OUTAGE_S + 10)
        try:
            sampler.wait(timeout=140)
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
        ok = [s for s in samples if "p" in s]
        during = [s for s in ok if 15 <= s["t"] < 15 + OUTAGE_S]
        after = [s for s in ok if s["t"] >= 15 + OUTAGE_S + 5]
        res["window"] = {
            "exec_during": (during[0]["p"]["executed_this_incarnation"],
                            during[-1]["p"]["executed_this_incarnation"])
            if during else None,
            "durable_during": (during[0]["p"]["durable_units"],
                               during[-1]["p"]["durable_units"]) if during else None,
            "queued_during": [s["p"]["queued"] for s in during],
            "exec_after": (after[0]["p"]["executed_this_incarnation"],
                           after[-1]["p"]["executed_this_incarnation"])
            if after else None,
            "writer_errors": sorted({s["p"]["writer_error"] for s in ok
                                     if s["p"].get("writer_error")}),
        }
        w = res["window"]
        w["work_during_outage"] = (w["exec_during"][1] - w["exec_during"][0]
                                   if w["exec_during"] else None)
        w["max_queued"] = max(w["queued_during"]) if w["queued_during"] else None
        w["queue_saturated"] = (w["max_queued"] is not None
                                and w["max_queued"] >= q_for(arm) - 1)
        log(f"  [{arm}] work during window: {w['work_during_outage']} units, "
            f"max queued {w['max_queued']} (Q={q_for(arm)} batches = "
            f"{q_for(arm)*W} units), saturated={w['queue_saturated']}")

    if die_at:
        ok2 = [x for x in res.get("samples", []) if "p" in x]
        pre_death = [x for x in ok2 if x["t"] <= die_at]
        last = pre_death[-1]["p"] if pre_death else {}
        rd, rr = drive(arm, REDO, "redo")
        ex = last.get("executed_this_incarnation", 0)
        dur = last.get("durable_units", 0)
        rf = (rd or {}).get("after", {}).get("resumed_from", 0)
        bound = (res["Q"] * W + W) if res["Q"] else W
        res["redo"] = {
            "executed_at_death": ex, "durable_at_death": dur,
            "backlog_at_death": ex - dur,
            "queued_at_death": last.get("queued"),
            "resumed_from": rf, "redo_units": ex - rf,
            "bound": bound, "within_bound": (ex - rf) <= bound,
            "died": any(x.get("died") for x in res.get("samples", [])),
            "restart_epoch": (rd or {}).get("after", {}).get("epoch"),
        }
        log(f"  [{arm}] died={res['redo']['died']} executed={ex} durable={dur} "
            f"backlog={ex-dur} queued={last.get('queued')} resumed_from={rf} "
            f"redo={ex-rf} bound={bound} within={res['redo']['within_bound']}")

    fin, rf2 = drive(arm, FINAL, "final")
    res["final"] = fin if fin is not None else {"error": rf2.stderr[-400:]}
    if fin:
        log(f"  [{arm}] committed={fin['committed']} dupes={fin['has_dupes']} "
            f"contiguous={fin['contiguous']} epochs={fin['epochs']}")
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

    res = {"arms": {}, "config": {"N": N_UNITS, "W": W, "K": K, "Q": Q_SMALL,
                                  "unit_seconds": UNIT_SECONDS,
                                  "outage_s": OUTAGE_S}}
    try:
        for arm in ARMS:
            log(""); log("=" * 68); log(f"ARM: {arm}"); log("=" * 68)
            res["arms"][arm] = run_arm(arm, log)
    finally:
        sh([RAY, "stop", "--force"])

    a = res["arms"]
    nk = a.get("async_nokill", {})
    ou = a.get("async_outage", {})
    ac = a.get("async_crash_outage", {})
    sc = a.get("sync_crash_outage", {})
    nk_fin = nk.get("final", {})
    contiguous_all = all(
        a.get(x, {}).get("final", {}).get("contiguous") is True
        and a.get(x, {}).get("final", {}).get("has_dupes") is False
        for x in ARMS)
    res["verdict"] = {
        "NC_nokill_completed_all": nk_fin.get("committed") == N_UNITS,
        "NC_nokill_clean": nk_fin.get("contiguous") is True
                           and nk_fin.get("has_dupes") is False,
        "VOID_nokill_corrupted": not (nk_fin.get("contiguous") is True
                                      and nk_fin.get("has_dupes") is False),
        "clause1_work_during_outage": ou.get("window", {}).get("work_during_outage"),
        "clause1_beats_sync_baseline_zero":
            (ou.get("window", {}).get("work_during_outage") or 0) > 0,
        "NC_queue_saturated": ou.get("window", {}).get("queue_saturated"),
        "clause2_async_queued_at_death": ac.get("redo", {}).get("queued_at_death"),
        "clause2_async_backlog_at_death": ac.get("redo", {}).get("backlog_at_death"),
        "NC_async_had_a_backlog_to_lose":
            (ac.get("redo", {}).get("backlog_at_death") or 0) >= Q_SMALL,
        "NC_sync_backlog_at_death": sc.get("redo", {}).get("backlog_at_death"),
        "NC_coordinator_actually_died":
            bool(ac.get("redo", {}).get("died"))
            and bool(sc.get("redo", {}).get("died")),
        "clause2_async_redo": ac.get("redo", {}).get("redo_units"),
        "clause2_async_bound": ac.get("redo", {}).get("bound"),
        "clause2_async_within_bound": ac.get("redo", {}).get("within_bound"),
        "NC_sync_redo": sc.get("redo", {}).get("redo_units"),
        "NC_sync_within_W": sc.get("redo", {}).get("within_bound"),
        "clause3_contiguous_everywhere": contiguous_all,
        "writer_errors": ou.get("window", {}).get("writer_errors"),
        "C26_proven":
            (ou.get("window", {}).get("work_during_outage") or 0) > 0
            and bool(ou.get("window", {}).get("queue_saturated"))
            and (ac.get("redo", {}).get("backlog_at_death") or 0) >= Q_SMALL
            and (ac.get("redo", {}).get("redo_units") or 0)
                > (sc.get("redo", {}).get("redo_units") or 0)
            and ac.get("redo", {}).get("within_bound") is True
            and sc.get("redo", {}).get("within_bound") is True
            and contiguous_all,
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 68)
    for k2, v2 in res["verdict"].items():
        log(f"  {k2:38s} {v2}")
    log("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
