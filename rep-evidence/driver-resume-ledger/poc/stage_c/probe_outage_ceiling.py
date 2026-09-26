"""Follow-up to C28's outage_60 arm: the cluster itself died.

A 60s GCS outage exceeds gcs_rpc_server_reconnect_timeout_s (default 60s), so
the raylet gives up and the node exits -- there is no coordinator left to
restart.  The question that remains is whether the LEDGER survived on the
rocksdb path, i.e. whether the ledger design degrades to cold restart rather than data loss.
"""
import json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
RAY = os.path.join(os.path.dirname(sys.executable), "ray")
STORE = "/tmp/ledger_c28_outage_60"
PORT = 6593
ADDR = f"127.0.0.1:{PORT}"

def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, timeout=300, **k)

out = {"store_exists": os.path.isdir(STORE)}
out["store_entries"] = sorted(os.listdir(STORE))[:10] if out["store_exists"] else []
alive = sh(["pgrep", "-f", "raylet"]).stdout.split()
out["raylet_alive_before_restart"] = len(alive)

sh([RAY, "stop", "--force"]); time.sleep(2)
syscfg = json.dumps({"gcs_storage": "rocksdb", "gcs_storage_path": STORE})
h = sh([RAY, "start", "--head", f"--port={PORT}", "--num-cpus=2",
        "--include-dashboard=False", f"--system-config={syscfg}",
        "--disable-usage-stats"])
out["cold_restart_rc"] = h.returncode
time.sleep(6)

src = os.path.join(HERE, "_probe_ceiling.py")
with open(src, "w") as f:
    f.write(f"""
import os, sys, json
sys.path.insert(0, {HERE!r})
import ray, progress_ledger as pl
ray.init(address={ADDR!r}, namespace='ledger', logging_level='ERROR')
o = {{}}
o['session_name'] = ray._private.worker._global_node.session_name
led = pl.Ledger('scope-outage_60', 'job28')
eps = sorted(int(k.rsplit('/',1)[1]) for k in led._list(led._ep()))
o['epoch_keys'] = eps
best = []
for e in eps:
    d = sorted((led._read_epoch(e) or {{}}).get('done', []))
    if len(d) > len(best):
        best = d
o['committed'] = len(best)
o['contiguous'] = best == list(range(len(best)))
o['ledger_key_count'] = len(led._list(led.root))
try:
    o['named'] = list(ray.util.list_named_actors(all_namespaces=True))[:8]
except Exception as e:
    o['named'] = '<err %s>' % type(e).__name__
c = pl.JobCoordinator.options(name='coord-outage_60', lifetime='detached',
    get_if_exists=True, max_concurrency=3).remote(
    'scope-outage_60', 'job28', 600, 3, 3, 0.35, 0)
o['fresh_incarnation'] = ray.get(c.progress.remote(), timeout=90)
print('RESULT', json.dumps(o))
""")
p = subprocess.run([sys.executable, src], capture_output=True, text=True, timeout=300)
for line in p.stdout.splitlines():
    if line.startswith("RESULT "):
        out["probe"] = json.loads(line[7:])
if "probe" not in out:
    out["probe_error"] = p.stderr[-600:]
sh([RAY, "stop", "--force"])
print(json.dumps(out, indent=2))
