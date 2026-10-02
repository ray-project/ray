"""C3 (second half): does the ledger disturb other GCS users?

"No measurable regression" is unfalsifiable on its own -- a rig that cannot see a
regression reports none and looks like a pass.  Two controls make it testable:

  overload      the same ledger driven flat out.  MUST regress, or the rig is
                blind and the whole run is void.
  idle_a/idle_b two identical no-load arms whose difference IS the noise floor.
                An effect smaller than that floor is not an effect.

Victim = a driver doing internal_kv put/get in its own namespace, timing every
operation; this is the path Serve's KV store, the Jobs API and the dashboard all
use.  A second signal, actor round-trip latency, separates a KV-handler
regression from a whole-GCS regression.

Pre-registered in experiments/C3.md.
"""
from __future__ import annotations

import json
import os
import shutil
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 6597
ADDR = f"127.0.0.1:{PORT}"
RAY = os.path.join(os.path.dirname(sys.executable), "ray")
STORE = "/tmp/ledger_c3"

VICTIM_SECONDS = 60
DESIGN_RATE = 26.0        # ~10% of the 262/s same-key ceiling

# Baselines are INTERLEAVED between load arms so each treatment is compared
# against its temporal neighbour and warm-up/scheduling drift cancels.
# (label, rate-per-loader or None, n_loaders, value_bytes)
ARMS_SPEC = [
    ("base_0",    None,        0, 0),
    ("r26",       26.0,        1, 256),
    ("base_1",    None,        0, 0),
    ("r50",       50.0,        1, 256),
    ("base_2",    None,        0, 0),
    ("r100",      100.0,       1, 256),
    ("base_3",    None,        0, 0),
    ("r200",      200.0,       1, 256),
    ("base_4",    None,        0, 0),
    ("bigvalue",  0.0,         4, 4 * 1024 * 1024),
    ("base_5",    None,        0, 0),
]
ARMS = [a[0] for a in ARMS_SPEC]


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, timeout=300, **k)


VICTIM = r'''
import os, sys, json, time
sys.path.insert(0, HERE)
import ray
from ray.experimental import internal_kv as ikv

ray.init(address=ADDR, namespace='victim', logging_level='ERROR')

NS = b'victim_ns'
kv_lat, rt_lat = [], []


@ray.remote(num_cpus=0)
class Ping:
    def ping(self):
        return 1


p = Ping.remote()
ray.get(p.ping.remote())

t0 = time.time()
i = 0
while time.time() - t0 < DURATION:
    k = b'victim/key%d' % (i % 32)
    s = time.perf_counter()
    ikv._internal_kv_put(k, b'x' * 64, True, namespace=NS)
    ikv._internal_kv_get(k, namespace=NS)
    kv_lat.append((time.perf_counter() - s) * 1000.0)
    if i % 5 == 0:
        s = time.perf_counter()
        ray.get(p.ping.remote())
        rt_lat.append((time.perf_counter() - s) * 1000.0)
    i += 1

print('RESULT', json.dumps({'kv_ms': kv_lat, 'rt_ms': rt_lat}))
'''

LOAD = r'''
import os, sys, json, time
sys.path.insert(0, HERE)
import ray
from ray.experimental import internal_kv as ikv

ray.init(address=ADDR, namespace='load', logging_level='ERROR')
import os as _os
NS = b'progress_ledger'
VALUE = b'y' * VALUE_BYTES
t0 = time.time()
n = 0
period = (1.0 / RATE) if RATE > 0 else 0.0
nxt = time.time()
while time.time() - t0 < DURATION:
    ikv._internal_kv_put(b'ledger/seg/%06d' % (n % 4096), VALUE, True,
                         namespace=NS)
    n += 1
    if period:
        nxt += period
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)
print('RESULT', json.dumps({'writes': n,
                            'rate': n / max(1e-9, time.time() - t0)}))
'''


def write_script(name: str, body: str, subs: dict) -> str:
    src = body
    for k, v in subs.items():
        src = src.replace(k, v)
    path = os.path.join(HERE, name)
    with open(path, "w") as f:
        f.write(src)
    return path


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return round(xs[i], 3)


def run_arm(spec, log) -> dict:
    arm, rate, n_loaders, vbytes = spec
    res: dict = {"arm": arm, "rate": rate, "n_loaders": n_loaders,
                 "value_bytes": vbytes}

    vpath = write_script(f"_c3_victim_{arm}.py", VICTIM, {
        "HERE": repr(HERE), "ADDR": repr(ADDR), "DURATION": str(VICTIM_SECONDS)})
    victim = subprocess.Popen([sys.executable, vpath], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    time.sleep(3)

    loaders = []
    for li in range(n_loaders):
        lpath = write_script(f"_c3_load_{arm}_{li}.py", LOAD, {
            "HERE": repr(HERE), "ADDR": repr(ADDR),
            "DURATION": str(VICTIM_SECONDS - 5), "RATE": str(rate),
            "VALUE_BYTES": str(vbytes)})
        loaders.append(subprocess.Popen([sys.executable, lpath],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True))

    out, err = victim.communicate(timeout=VICTIM_SECONDS + 180)
    for line in out.splitlines():
        if line.startswith("RESULT "):
            d = json.loads(line[7:])
            res["kv_n"] = len(d["kv_ms"])
            res["kv"] = {q: pct(d["kv_ms"], v) for q, v in
                         (("p50", .5), ("p95", .95), ("p99", .99),
                          ("p999", .999))}
            res["rt"] = {q: pct(d["rt_ms"], v) for q, v in
                         (("p50", .5), ("p95", .95), ("p99", .99))}
    if "kv" not in res:
        res["error"] = err[-400:]
    total = 0.0
    for ld in loaders:
        try:
            lo, _ = ld.communicate(timeout=90)
            for line in lo.splitlines():
                if line.startswith("RESULT "):
                    total += json.loads(line[7:]).get("rate", 0.0)
        except subprocess.TimeoutExpired:
            ld.kill()
    res["measured_load_rate"] = round(total, 1)
    log(f"  [{arm}] n={res.get('kv_n')} kv={res.get('kv')} "
        f"load={res['measured_load_rate']}/s")
    return res


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n"); logf.flush()

    sh([RAY, "stop", "--force"]); time.sleep(2)
    if os.path.isdir(STORE):
        shutil.rmtree(STORE)
    os.makedirs(STORE, exist_ok=True)
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": STORE})
    h = sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=4",
            "--include-dashboard=False", f"--system-config={syscfg}",
            "--disable-usage-stats"])
    if h.returncode != 0:
        log("head start failed:", h.stderr[-400:])
        return 1
    time.sleep(5)
    sh([RAY, "start", f"--address={ADDR}", "--num-cpus=2", "--disable-usage-stats"])
    time.sleep(3)

    res = {"arms": {}, "order": [], "config": {
        "victim_seconds": VICTIM_SECONDS, "design_rate": DESIGN_RATE,
        "arms_spec": [list(x) for x in ARMS_SPEC]}}
    try:
        for spec in ARMS_SPEC:
            log(""); log("=" * 66)
            log(f"ARM: {spec[0]}  rate={spec[1]} loaders={spec[2]} "
                f"value={spec[3]}B")
            log("=" * 66)
            res["arms"][spec[0]] = run_arm(spec, log)
            res["order"].append(spec[0])
    finally:
        sh([RAY, "stop", "--force"])

    a = res["arms"]

    def p99(arm):
        return a.get(arm, {}).get("kv", {}).get("p99")

    bases = [x for x in res["order"] if x.startswith("base_")]
    bvals = [p99(b) for b in bases if p99(b)]
    # Noise floor from the SPREAD of the interleaved baselines, not one pair.
    bmean = statistics.fmean(bvals) if bvals else None
    bsd = statistics.pstdev(bvals) if len(bvals) >= 2 else None
    floor = (2 * bsd / bmean) if (bsd and bmean) else None
    spread = ((max(bvals) - min(bvals)) / bmean) if (bvals and bmean) else None
    thresh = max(0.25, floor) if floor is not None else 0.25

    def neighbour_base(arm):
        idx = res["order"].index(arm)
        for off in range(1, len(res["order"])):
            for j2 in (idx - off, idx + off):
                if 0 <= j2 < len(res["order"]):
                    nm = res["order"][j2]
                    if nm.startswith("base_") and p99(nm):
                        return nm
        return None

    deltas = {}
    for arm in res["order"]:
        if arm.startswith("base_") or not p99(arm):
            continue
        nb = neighbour_base(arm)
        if nb:
            p95a = a.get(arm, {}).get("kv", {}).get("p95")
            p95b = a.get(nb, {}).get("kv", {}).get("p95")
            deltas[arm] = {
                "vs": nb, "p99": p99(arm), "base_p99": p99(nb),
                "delta_frac": round((p99(arm) - p99(nb)) / p99(nb), 4),
                "p95": p95a, "base_p95": p95b,
                "p95_delta_frac": (round((p95a - p95b) / p95b, 4)
                                   if (p95a and p95b) else None),
                "load": a[arm]["measured_load_rate"],
            }
    big = deltas.get("bigvalue", {})
    des = deltas.get("r26", {})
    regressed = {k: (v["delta_frac"] > thresh) for k, v in deltas.items()}
    res["verdict"] = {
        "baseline_p99s": {b: p99(b) for b in bases},
        "noise_floor_2sigma_frac": round(floor, 4) if floor is not None else None,
        "baseline_spread_frac": round(spread, 4) if spread is not None else None,
        "baseline_p99_mean": round(bmean, 3) if bmean else None,
        "noise_floor_below_25pct": (floor is not None and floor < 0.25),
        "threshold_frac": round(thresh, 4),
        "deltas": deltas,
        "regressed": regressed,
        "POSCTRL_bigvalue_regressed": bool(regressed.get("bigvalue")),
        "VOID_rig_blind": not bool(regressed.get("bigvalue")),
        "design_r26_delta": des.get("delta_frac"),
        "design_within_threshold": (des.get("delta_frac") is not None
                                    and des["delta_frac"] <= thresh),
        "ledger_rates_that_regressed":
            [k for k, v in regressed.items() if v and k != "bigvalue"],
        "max_ledger_rate_tested": max(
            [v["load"] for k, v in deltas.items() if k != "bigvalue"] or [0]),
        "C3b_proven": (bool(regressed.get("bigvalue"))
                       and floor is not None and floor < 0.25
                       and des.get("delta_frac") is not None
                       and des["delta_frac"] <= thresh),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 66)
    for k, v in res["verdict"].items():
        log(f"  {k:32s} {v}")
    log("=" * 66)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
