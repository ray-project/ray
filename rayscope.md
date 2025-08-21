## RayScope runbook (local smoke test)

### Prerequisites

- Activate the Ray-compatible conda env (provided):

```bash
conda activate rayenv
```

- Point Python to the in-repo Ray sources (we’ll run the CLI from this repo):

```bash
export PYTHONPATH=/Users/sagar/workspace/codope/ray/python:$PYTHONPATH
```

### Start a local cluster (and capture the API server address)

- Option A: via CLI (dashboard can auto-pick a free port with `--dashboard-port=0`)

```bash
python -m ray.scripts.scripts start --head --include-dashboard=True --num-cpus=4 --dashboard-port=0
# The output prints an API address like:
#   RAY_API_SERVER_ADDRESS='http://127.0.0.1:63241'
# Export it so all commands use the same cluster:
export API=http://127.0.0.1:63241   # <-- replace with the value printed above
```

- Option B: in-process (quick test)

```bash
python - <<'PY'
import ray
ray.init()
print("Ray is up:", ray.is_initialized())
PY
```

- Verify the CLI entrypoint loads

```bash
python -m ray.scripts.scripts --help
```

### Run a small workload (tasks, actors, objects)

Use a second terminal with the same environment:

```bash
conda activate rayenv
export PYTHONPATH=/Users/sagar/workspace/codope/ray/python:$PYTHONPATH

python - <<'PY'
import time
import os
import numpy as np
import ray


# Prefer the same address as the started cluster. If not set, try auto; if that fails, start local.
addr = os.environ.get("RAY_ADDRESS", os.environ.get("API", "auto"))
try:
    ray.init(address=addr)
except Exception:
    ray.init()

@ray.remote
def sleep_and_return(i, secs=0.5, size_mb=5):
    time.sleep(secs)
    # Create an array to store in the object store (~ size_mb)
    arr = np.ones((size_mb * 250_000,), dtype=np.float32)
    return i, arr

@ray.remote
class Counter:
    def __init__(self):
        self.n = 0
    def incr(self, k=1):
        self.n += k
        time.sleep(0.2)
        return self.n

# Submit tasks
# Smaller size to keep output readable; bump size_mb for larger objects
refs = [sleep_and_return.remote(i, secs=0.2, size_mb=2) for i in range(20)]

# Create an actor, call it a few times
c = Counter.remote()
actor_refs = [c.incr.remote() for _ in range(10)]

print("First 5 task results:", ray.get(refs[:5]))
print("Actor result sample:", ray.get(actor_refs[-1]))
print("Workload submitted; leaving others in-flight for observability...")
time.sleep(5)
PY

# Optional: keep tasks in-flight so list/top show activity. Run in another terminal.
```bash
python - <<'PY'
import time, os, ray
addr = os.environ.get("RAY_ADDRESS", os.environ.get("API", "auto"))
try:
    ray.init(address=addr)
except Exception:
    ray.init()
@ray.remote
def f(t=10):
    time.sleep(t)
    return t
while True:
    _ = [f.remote(10) for _ in range(100)]
    time.sleep(1)
PY
```
```

### Smoke test the new CLI commands

- Object listing (alias for the State API “list objects”)

```bash
# Prefer using the explicit API to ensure same cluster.
python -m ray.scripts.scripts object ls --address="$API" --limit 10 --format table

# JSON output
python -m ray.scripts.scripts object ls --address="$API" --limit 10 --format json

# Server-side filter examples
python -m ray.scripts.scripts object ls --address="$API" --filter "ip=127.0.0.1" --limit 50 --format table
```

- Lightweight terminal “top” snapshot (experimental)

```bash
python -m ray.scripts.scripts top --address="$API" --refresh 1.0 --show both
# Press Ctrl+C to exit
```

- Baseline state snapshots (useful for comparison)

```bash
python -m ray.scripts.scripts list tasks  --address="$API" --limit 20 --format table
python -m ray.scripts.scripts list actors --address="$API" --limit 20 --format table
python -m ray.scripts.scripts list objects --address="$API" --limit 20 --format table
```

### Cleanup

```bash
python -m ray.scripts.scripts stop
# If you used in-process init, exiting the process is enough (ray.shutdown() optional)
```

### Troubleshooting

- CLI cannot find the cluster:

```bash
# Ensure a cluster is running and pass the same address
python -m ray.scripts.scripts status --address="$API"
```

- Dashboard port 8265 is in use:

```bash
python -m ray.scripts.scripts start --head --dashboard-port=0
```

- Import fail or CLI not found:

```bash
conda activate rayenv
export PYTHONPATH=/Users/sagar/workspace/codope/ray/python:$PYTHONPATH
```

- Record object callsites for richer object info:

Set `RAY_record_ref_creation_sites=1` in the environment for both the cluster and client processes before starting Ray.


