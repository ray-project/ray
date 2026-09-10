"""C5: is coordinator state actually reconstructible from ledger + cluster?

Every run so far used a coordinator whose entire state is a set of completed unit
ids -- which lives wholly in the ledger, so "two-source reconciliation" was never
exercised.  The interesting state lives in the CLUSTER: long-lived child actors.

Creating a child and recording it are two operations, so a crash can land between
them.  The dangerous order is create -> crash -> record: a child exists that the
ledger never heard of.  If the next incarnation cannot attribute it, the actor is
an orphan -- alive, owned by nobody, invisible -- and a duplicate gets created
beside it.

Variable under test: how children are NAMED.  Not randomness -- named actors are
all listable either way -- but ATTRIBUTION.

Pre-registered in experiments/C5.md.  Three arms; the third is the control.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

HERE = os.path.dirname(os.path.abspath(__file__))
STORE = "/tmp/ledger_c5"
K = 6          # children to create
R = 2          # ...of which only this many get recorded before the crash
ARMS = ("opaque", "prefixed_explicit", "prefixed_getifexists")

PRELUDE = """
import os, sys, json, time, uuid
sys.path.insert(0, {here!r})
import ray, progress_ledger as pl

ARM = {arm!r}
SCOPE, JOB = 'scope-c5-' + ARM, 'jobc5'
PREFIX = 'ledger/%s/%s/child-' % (SCOPE, JOB)

ray.init(address={addr!r}, namespace='ledger', logging_level='ERROR')


@ray.remote(num_cpus=0)
class Child:
    def __init__(self, idx):
        self.idx = idx
    def ident(self):
        return {{'idx': self.idx, 'pid': os.getpid()}}


def child_name(i):
    # The whole experiment is this function.
    return (PREFIX + str(i)) if ARM.startswith('prefixed') else uuid.uuid4().hex


def ledger():
    return pl.Ledger(SCOPE, JOB)
"""

PHASE1 = """
led = ledger()
led.claim()
created = []
for i in range(K_PLACEHOLDER):
    name = child_name(i)
    c = Child.options(name=name, lifetime='detached',
                      get_if_exists=True).remote(i)
    ray.get(c.ident.remote())          # force creation before we go on
    created.append({'idx': i, 'name': name,
                    'pid': ray.get(c.ident.remote())['pid']})
    if i < R_PLACEHOLDER:
        # Record intent AFTER creation, so the crash lands in the dangerous
        # create -> crash -> record window for every child beyond R.
        led.commit(i + 1, [i])
        led._put(led.root + '/children/' + str(i), name)
print('RESULT', json.dumps({'created': created}))
sys.stdout.flush()
os._exit(1)      # crash, mid-window, K-R times over
"""

PHASE2 = """
led = ledger()
recorded = {}
for k in led._list(led.root + '/children/'):
    recorded[k.rsplit('/', 1)[1]] = led._get(k)

# Only the 'explicit' arm does a listing pass.  'getifexists' deliberately
# does none: it relies solely on deterministic names plus get_if_exists.
discovered = []
if ARM == 'prefixed_explicit':
    try:
        for n in ray.util.list_named_actors(all_namespaces=True):
            nm = n['name'] if isinstance(n, dict) else n
            if nm.startswith(PREFIX):
                discovered.append(nm)
    except Exception as e:
        discovered = ['<err %s>' % type(e).__name__]

# Reality = what we could attribute.  Union of both sources.
known = set(recorded.values()) | set(d for d in discovered
                                     if not d.startswith('<'))
adopted, created_now, unreachable = [], [], []
for i in range(K_PLACEHOLDER):
    name = (PREFIX + str(i)) if ARM.startswith('prefixed') else recorded.get(str(i))
    if name and name in known:
        try:
            h = ray.get_actor(name, namespace='ledger')
            adopted.append({'name': name,
                            'ident': ray.get(h.ident.remote(), timeout=30)})
            continue
        except Exception as e:
            unreachable.append({'name': name, 'err': type(e).__name__})
    # Not attributable from either source -> we believe it does not exist and
    # create it.  get_if_exists is FALSE here on purpose: it must not be
    # allowed to silently paper over a failed reconciliation, which is exactly
    # how the first version of this rig produced a green control.
    nm = (PREFIX + str(i)) if ARM.startswith('prefixed') else uuid.uuid4().hex
    if ARM == 'prefixed_getifexists':
        # ...except in the arm whose whole hypothesis IS get_if_exists.
        c = Child.options(name=nm, lifetime='detached',
                          get_if_exists=True).remote(i)
        pid = ray.get(c.ident.remote())['pid']
        created_now.append({'name': nm, 'pid': pid, 'via': 'get_if_exists'})
    else:
        try:
            c = Child.options(name=nm, lifetime='detached').remote(i)
            pid = ray.get(c.ident.remote())['pid']
            created_now.append({'name': nm, 'pid': pid, 'via': 'create'})
        except Exception as e:
            created_now.append({'name': nm, 'pid': None,
                                'via': 'FAILED:' + type(e).__name__})

live = []
try:
    for n in ray.util.list_named_actors(all_namespaces=True):
        nm = n['name'] if isinstance(n, dict) else n
        live.append(nm)
except Exception as e:
    live = ['<err %s>' % type(e).__name__]

print('RESULT', json.dumps({
    'recorded': recorded, 'discovered': discovered,
    'adopted': adopted, 'created_now': created_now,
    'unreachable': unreachable,
    'live_named_total': len(live), 'live_named': live[:32],
}))
"""


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, timeout=300, **kw)


def run_phase(arm: str, body: str, addr: str, reconcile: bool, tag: str):
    src = (PRELUDE.format(here=HERE, arm=arm, addr=addr)
           + body.replace("K_PLACEHOLDER", str(K))
                 .replace("R_PLACEHOLDER", str(R))
                 .replace("RECONCILE_PLACEHOLDER", str(reconcile)))
    path = os.path.join(HERE, f"_c5_{arm}_{tag}.py")
    with open(path, "w") as f:
        f.write(src)
    p = subprocess.run([sys.executable, path], capture_output=True, text=True,
                       timeout=300)
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[7:]), p
    return None, p


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    logf = open(os.path.join(out_dir, "stdout.log"), "w")

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        logf.write(line + "\n"); logf.flush()

    ray_bin = os.path.join(os.path.dirname(sys.executable), "ray")
    port = 6587
    addr = f"127.0.0.1:{port}"
    syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": STORE})

    sh([ray_bin, "stop", "--force"]); time.sleep(2)
    if os.path.isdir(STORE):
        shutil.rmtree(STORE)
    os.makedirs(STORE, exist_ok=True)
    h = sh([ray_bin, "start", "--head", f"--port={port}", "--num-cpus=4",
            "--include-dashboard=False", f"--system-config={syscfg}",
            "--disable-usage-stats"])
    if h.returncode != 0:
        log("head start failed:", h.stderr[-500:])
        return 1
    time.sleep(5)

    res: dict = {"arms": {}, "K": K, "R": R}
    try:
        for arm in ARMS:
            log(""); log("=" * 68); log(f"ARM: {arm}"); log("=" * 68)
            reconcile = not arm.endswith("noreconcile")
            a: dict = {"arm": arm, "reconcile": reconcile}

            p1, r1 = run_phase(arm, PHASE1, addr, reconcile, "p1")
            if p1 is None:
                a["error"] = "phase1: " + r1.stderr[-500:]
                res["arms"][arm] = a
                log("  phase1 failed:", r1.stderr[-400:])
                continue
            a["phase1"] = p1
            log(f"  phase1 created {len(p1['created'])} children, "
                f"only {R} recorded before the crash")

            p2, r2 = run_phase(arm, PHASE2, addr, reconcile, "p2")
            if p2 is None:
                a["error"] = "phase2: " + r2.stderr[-500:]
                res["arms"][arm] = a
                log("  phase2 failed:", r2.stderr[-400:])
                continue
            a["phase2"] = p2

            before = {c["name"]: c["pid"] for c in p1["created"]}
            adopted_names = [x["name"] for x in p2["adopted"]]
            same_pid = all(x["ident"]["pid"] == before.get(x["name"])
                           for x in p2["adopted"])
            reacquired = {c["name"] for c in p2["created_now"]
                          if c.get("pid") == before.get(c["name"])}
            held = set(adopted_names) | reacquired
            orphans = [n for n in before
                       if n not in held and n in p2["live_named"]]
            a["reacquired_same_pid"] = sorted(reacquired)
            a["metrics"] = {
                "adopted": len(p2["adopted"]),
                "adopted_same_pid": same_pid,
                "created_now": len(p2["created_now"]),
                "unreachable": len(p2["unreachable"]),
                "orphans": len(orphans),
                "orphan_names": orphans[:8],
                "live_named_total": p2["live_named_total"],
            }
            res["arms"][arm] = a
            for k2, v2 in a["metrics"].items():
                log(f"  {k2:22s} {v2}")
    finally:
        sh([ray_bin, "stop", "--force"])

    m = {k: v.get("metrics", {}) for k, v in res["arms"].items()}
    op = m.get("opaque", {})
    pe = m.get("prefixed_explicit", {})
    pg = m.get("prefixed_getifexists", {})
    # The OPAQUE arm is the negative control: names carry no attribution, so
    # the K-R unrecorded children must be genuinely orphaned and genuinely
    # duplicated.  If opaque comes out clean, attribution is not load-bearing
    # and the rig is not measuring what it claims.
    res["verdict"] = {
        "NC_opaque_leaked_as_required": op.get("orphans", 0) == (K - R),
        "VOID_control_clean": op.get("orphans", -1) == 0,
        "opaque_orphans": op.get("orphans"),
        "opaque_real_duplicates": op.get("created_now"),
        "explicit_orphans": pe.get("orphans"),
        "explicit_adopted_all": pe.get("adopted") == K,
        "explicit_adoption_real_same_pid": pe.get("adopted_same_pid"),
        "explicit_created_nothing": pe.get("created_now") == 0,
        "getifexists_orphans": pg.get("orphans"),
        "getifexists_reacquired": len(res["arms"]
                                      .get("prefixed_getifexists", {})
                                      .get("reacquired_same_pid", [])),
        "C5_proven_conditional_on_scoped_names":
            op.get("orphans", 0) == (K - R)
            and pe.get("orphans") == 0 and pe.get("adopted") == K
            and bool(pe.get("adopted_same_pid"))
            and pe.get("created_now") == 0,
        "getifexists_is_sufficient_on_its_own":
            pg.get("orphans") == 0
            and len(res["arms"].get("prefixed_getifexists", {})
                    .get("reacquired_same_pid", [])) == (K - R),
    }
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)
    log(""); log("=" * 68)
    for k2, v2 in res["verdict"].items():
        log(f"  {k2:46s} {v2}")
    log("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
