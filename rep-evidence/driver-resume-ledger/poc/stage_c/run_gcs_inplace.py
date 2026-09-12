"""C7 + C4 (duplication clause): a GCS outage the coordinator SURVIVES.

Every earlier cluster run used `ray stop --force`, which takes the worker raylet
and the coordinator down with the head (gap F8).  That makes C7 unobservable --
there is no work left to continue -- and it makes C4's duplication clause
unreachable, because a coordinator that is already dead cannot be duplicated.

Here the outage is produced by capturing `/proc/<pid>/cmdline` for gcs_server,
SIGKILLing it, and later re-executing that exact argv.  Nothing else is stopped.

A sampler driver is started BEFORE the kill and polls the coordinator every 2s,
because `ray.init` needs GCS and so no new driver can attach during the outage.

Pre-registered in experiments/C7.md.  Two arms: `kill` and `nokill` (NC-outage).
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

PORT = 6583
RAY = os.path.join(os.path.dirname(sys.executable), "ray")
ADDR = f"127.0.0.1:{PORT}"
HERE = os.path.dirname(os.path.abspath(__file__))

N_UNITS = 400
UNIT_SECONDS = 0.35
OUTAGE_S = 35


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, timeout=300, **kw)


def gcs_procs():
    """(pid, argv) for every live gcs_server."""
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


SAMPLER = r'''
import os, sys, json, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ray, progress_ledger as pl
ray.init(address=ADDR_PLACEHOLDER, namespace='ledger', logging_level='ERROR')
h = ray.get_actor('coord-c7', namespace='ledger')
out = open(OUT_PLACEHOLDER, 'w')
t0 = time.time()
while time.time() - t0 < DUR_PLACEHOLDER:
    rec = {'t': round(time.time() - t0, 2)}
    # Two independent signals.  progress() touches no KV, so it reports work.
    # info() lists KV keys, so it reports durability -- and its FAILURE is
    # itself the evidence that the KV path is unavailable.
    try:
        rec['info'] = ray.get(h.progress.remote(), timeout=8)
    except Exception as e:
        rec['error'] = '%s: %s' % (type(e).__name__, str(e)[:120])
    try:
        ray.get(h.info.remote(), timeout=8)
        rec['kv_ok'] = True
    except Exception as e:
        rec['kv_ok'] = False
        rec['kv_error'] = '%s: %s' % (type(e).__name__, str(e)[:120])
    out.write(json.dumps(rec) + '\n'); out.flush()
    time.sleep(2)
out.close()
'''


def run_arm(arm: str, log) -> dict:
    store = f"/tmp/ledger_c7_{arm}"
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": store})
    res: dict = {"arm": arm}

    sh([RAY, "stop", "--force"])
    time.sleep(2)
    if os.path.isdir(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)

    h = sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=2",
            "--include-dashboard=False", f"--system-config={syscfg}",
            "--disable-usage-stats"])
    if h.returncode != 0:
        res["error"] = "head start failed: " + h.stderr[-400:]
        return res
    time.sleep(4)
    sh([RAY, "start", f"--address={ADDR}", "--num-cpus=2", "--disable-usage-stats"])
    time.sleep(2)

    # Create the coordinator and start it, from a driver that then exits.
    boot = os.path.join(HERE, "_boot_c7.py")
    with open(boot, "w") as f:
        f.write("import os, sys, json, time\n"
                "sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n"
                "import ray, progress_ledger as pl\n"
                f"ray.init(address='{ADDR}', namespace='ledger', logging_level='ERROR')\n"
                # max_concurrency so info() answers while run() is blocked --
                # without it a wedged actor and a busy one look identical.
                "c = pl.JobCoordinator.options(name='coord-c7', lifetime='detached',\n"
                "  get_if_exists=True, max_concurrency=3).remote(\n"
                f"  'scope-c7','jobc7',{N_UNITS},3,3,{UNIT_SECONDS})\n"
                "i = ray.get(c.info.remote())\n"
                "c.run.remote()\n"
                "print('RESULT', json.dumps(i))\n")
    b = subprocess.run([sys.executable, boot], capture_output=True, text=True,
                       timeout=180)
    if "RESULT" not in b.stdout:
        res["error"] = "boot failed: " + b.stderr[-600:]
        sh([RAY, "stop", "--force"])
        return res
    log(f"  [{arm}] coordinator started")

    # Sampler must pre-exist the outage: ray.init needs GCS.
    samples_path = os.path.join(HERE, f"_samples_{arm}.jsonl")
    sp = os.path.join(HERE, f"_sampler_{arm}.py")
    with open(sp, "w") as f:
        f.write(SAMPLER
                .replace("ADDR_PLACEHOLDER", repr(ADDR))
                .replace("OUT_PLACEHOLDER", repr(samples_path))
                .replace("DUR_PLACEHOLDER", "150"))
    sampler = subprocess.Popen([sys.executable, sp], stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    time.sleep(15)

    if arm == "kill":
        procs = gcs_procs()
        if not procs:
            res["error"] = "no gcs_server found"
            sh([RAY, "stop", "--force"])
            return res
        pid, argv, cwd = procs[0]
        res["gcs_argv"] = argv
        os.kill(pid, signal.SIGKILL)
        res["kill_at"] = time.time()
        log(f"  [{arm}] SIGKILL gcs_server pid={pid} -- nothing else stopped")
        time.sleep(OUTAGE_S)
        res["gcs_alive_during_outage"] = bool(gcs_procs())
        # Re-exec the SAME argv: a GCS process failover, in place.
        newp = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        res["gcs_restarted_pid"] = newp.pid
        log(f"  [{arm}] re-exec'd gcs_server as pid {newp.pid} after {OUTAGE_S}s")
        time.sleep(10)
        res["gcs_alive_after_restore"] = bool(gcs_procs())
    else:
        log(f"  [{arm}] NC-outage: no kill")
        time.sleep(OUTAGE_S + 10)

    # Let the sampler finish its window.
    try:
        sampler.wait(timeout=140)
    except subprocess.TimeoutExpired:
        sampler.kill()
    res["sampler_stderr"] = (sampler.stderr.read().decode()[-500:]
                             if sampler.stderr else "")

    samples = []
    if os.path.exists(samples_path):
        with open(samples_path) as f:
            for line in f:
                try:
                    samples.append(json.loads(line))
                except ValueError:
                    pass
    res["samples"] = samples

    # Duplication check + final state, from a fresh driver (GCS is back).
    post = os.path.join(HERE, "_post_c7.py")
    with open(post, "w") as f:
        f.write("import os, sys, json\n"
                "sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n"
                "import ray, progress_ledger as pl\n"
                f"ray.init(address='{ADDR}', namespace='ledger', logging_level='ERROR')\n"
                "out={}\n"
                "try:\n"
                "    import ray._private.state as st\n"
                "    acts=[a for a in st.actors().values()\n"
                "          if a.get('Name')=='coord-c7']\n"
                "    out['actor_rows']=[{'state':a.get('State'),\n"
                "      'pid':a.get('Address',{}).get('WorkerID','')[:8],\n"
                "      'restarts':a.get('NumRestarts')} for a in acts]\n"
                "    out['live_rows']=sum(1 for a in acts if a.get('State')=='ALIVE')\n"
                "except Exception as e:\n"
                "    out['actor_rows']='<err %s: %s>'%(type(e).__name__,str(e)[:100])\n"
                "try:\n"
                "    h=ray.get_actor('coord-c7', namespace='ledger')\n"
                "    out['final']=ray.get(h.info.remote(), timeout=60)\n"
                "except Exception as e:\n"
                "    out['final']={'error':'%s: %s'%(type(e).__name__,str(e)[:200])}\n"
                "led=pl.Ledger('scope-c7','jobc7')\n"
                "st1=led._read_epoch(1) or {}\n"
                "d=sorted(st1.get('done',[]))\n"
                "out['committed']=len(d)\n"
                "out['committed_has_dupes']=len(d)!=len(set(d))\n"
                "out['committed_contiguous']= d==list(range(len(d)))\n"
                "print('RESULT', json.dumps(out))\n")
    p = subprocess.run([sys.executable, post], capture_output=True, text=True,
                       timeout=200)
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "):
            res["post"] = json.loads(line[7:])
    if "post" not in res:
        res["post_error"] = p.stderr[-600:]
    sh([RAY, "stop", "--force"])
    return res


def analyse(res: dict, log) -> dict:
    """Split the sample stream at the kill and at the restore."""
    samples = res.get("samples", [])
    ok = [s for s in samples if "info" in s]
    errs = [s for s in samples if "error" in s]

    def exec_at(s):
        return s["info"].get("executed_this_incarnation", 0)

    def writes_at(s):
        return s["info"].get("committed_this_incarnation", 0)

    # sampler t=0 is ~ the boot; the kill happened ~15s in, restore ~15+OUTAGE.
    pre = [s for s in ok if s["t"] < 15]
    during = [s for s in ok if 15 <= s["t"] < 15 + OUTAGE_S]
    after = [s for s in ok if s["t"] >= 15 + OUTAGE_S + 5]
    d = {
        "samples_total": len(samples), "samples_ok": len(ok),
        "sampler_errors": len(errs),
        "sampler_error_examples": [e["error"] for e in errs[:3]],
        "exec_pre": (exec_at(pre[0]), exec_at(pre[-1])) if pre else None,
        "exec_during": (exec_at(during[0]), exec_at(during[-1])) if during else None,
        "exec_after": (exec_at(after[0]), exec_at(after[-1])) if after else None,
        "writes_during": (writes_at(during[0]), writes_at(during[-1])) if during else None,
        "writes_after": (writes_at(after[0]), writes_at(after[-1])) if after else None,
        "epochs_seen": sorted({s["info"].get("epoch") for s in ok}),
        "kv_ok_during": [s.get("kv_ok") for s in during],
        "kv_ok_after": [s.get("kv_ok") for s in after],
        "kv_error_examples": [s["kv_error"] for s in samples
                              if s.get("kv_error")][:3],
    }
    d["kv_unavailable_during_outage"] = (
        bool(during) and not any(s.get("kv_ok") for s in during))
    d["kv_available_after_restore"] = (
        bool(after) and any(s.get("kv_ok") for s in after))
    d["work_progressed_during_outage"] = bool(
        d["exec_during"] and d["exec_during"][1] > d["exec_during"][0])
    d["work_progressed_after_restore"] = bool(
        d["exec_after"] and d["exec_after"][1] > d["exec_after"][0])
    d["durability_progressed_during_outage"] = bool(
        d["writes_during"] and d["writes_during"][1] > d["writes_during"][0])
    d["durability_resumed_after_restore"] = bool(
        d["writes_after"] and d["writes_after"][1] > d["writes_after"][0])
    return d


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n"); logf.flush()

    res = {"arms": {}, "analysis": {}}
    try:
        for arm in ("nokill", "kill"):
            log(""); log("=" * 68); log(f"ARM: {arm}"); log("=" * 68)
            r = run_arm(arm, log)
            res["arms"][arm] = r
            a = analyse(r, log)
            res["analysis"][arm] = a
            for k in ("exec_pre", "exec_during", "exec_after", "writes_during",
                      "writes_after", "sampler_errors", "epochs_seen",
                      "kv_unavailable_during_outage", "kv_available_after_restore",
                      "kv_error_examples"):
                log(f"  [{arm}] {k:32s} {a[k]}")
            post = r.get("post", {})
            log(f"  [{arm}] live actor rows: {post.get('live_rows')}  "
                f"rows: {json.dumps(post.get('actor_rows'))[:180]}")
            log(f"  [{arm}] committed={post.get('committed')} "
                f"dupes={post.get('committed_has_dupes')} "
                f"contiguous={post.get('committed_contiguous')}")
            log(f"  [{arm}] final={json.dumps(post.get('final'))[:220]}")
    finally:
        sh([RAY, "stop", "--force"])

    k = res["analysis"].get("kill", {})
    nk = res["analysis"].get("nokill", {})
    kp = res["arms"].get("kill", {}).get("post", {})
    res["verdict"] = {
        "NC_outage_nokill_progressed":
            bool(nk.get("work_progressed_during_outage")
                 and nk.get("durability_progressed_during_outage")),
        "VOID_nokill_also_stalled":
            not (nk.get("work_progressed_during_outage")
                 and nk.get("durability_progressed_during_outage")),
        "work_continued_during_outage": k.get("work_progressed_during_outage"),
        "durability_paused_during_outage":
            not k.get("durability_progressed_during_outage"),
        "kv_confirmed_unavailable_during_outage":
            k.get("kv_unavailable_during_outage"),
        "kv_confirmed_available_after_restore":
            k.get("kv_available_after_restore"),
        "NC_sampler_saw_kv_go_red": bool(k.get("kv_error_examples")),
        "recovered_after_restore": k.get("work_progressed_after_restore"),
        "durability_resumed_after_restore": k.get("durability_resumed_after_restore"),
        "ray55996_wedged_after_restore":
            not k.get("work_progressed_after_restore")
            and not k.get("durability_resumed_after_restore"),
        "coordinator_survived": "error" not in kp.get("final", {"error": 1}),
        "no_duplicate_incarnation": kp.get("live_rows") in (0, 1),
        "ledger_no_dupes": kp.get("committed_has_dupes") is False,
        "ledger_contiguous": kp.get("committed_contiguous"),
        "no_restart_epoch_stayed_1": k.get("epochs_seen") == [1],
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 68)
    for kk, vv in res["verdict"].items():
        log(f"  {kk:44s} {vv}")
    log("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
