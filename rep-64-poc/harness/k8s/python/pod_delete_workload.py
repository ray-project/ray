"""Pod-delete recovery workload (COLLABORATORS.md item #5).

Runs in two phases: PHASE=setup creates state and prints a snapshot;
PHASE=verify reconnects and checks the state matches the snapshot.

Env vars:
  ACTOR_COUNT (default 10)
  PHASE       (setup | verify)
  SNAPSHOT    (JSON, required when PHASE=verify)

Output: METRICS_JSON line on the last stdout line; for setup, prints a
SNAPSHOT line with the JSON snapshot the harness round-trips back as
$SNAPSHOT for the verify phase.
"""

from __future__ import annotations

import json
import os
import sys

import ray


NAMESPACE = "rep64-test"


def setup() -> int:
    n = int(os.environ.get("ACTOR_COUNT", "10"))
    ray.init(namespace=NAMESPACE)

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def incr(self, k: int) -> int:
            self.value += k
            return self.value

        def value_(self) -> int:
            return self.value

    # Kill any leftover actors from a previous run so setup is idempotent.
    for i in range(n):
        try:
            old = ray.get_actor(f"rep64-pd-actor-{i}", namespace=NAMESPACE)
            ray.kill(old)
        except Exception:  # noqa: BLE001
            pass

    actors = [
        Counter.options(name=f"rep64-pd-actor-{i}", lifetime="detached").remote()
        for i in range(n)
    ]
    increments = list(range(1, n + 1))
    ray.get([a.incr.remote(k) for a, k in zip(actors, increments)])
    snapshot = {f"rep64-pd-actor-{i}": k for i, k in enumerate(increments)}

    # No flush sleep needed: GcsActorManager fsyncs the ALIVE write before
    # publishing actor state, the driver's ConnectActor is gated on that
    # publish, and method-call dispatch is gated on ConnectActor — so by the
    # time ray.get(incr) returns, the ALIVE row IS durable in the WAL. See
    # rocksdb_store_client.cc::SyncWriteOptions (wo.sync=true) and
    # gcs_actor_manager.cc::OnActorCreationSuccess.

    print("SNAPSHOT_JSON " + json.dumps(snapshot), flush=True)
    print("METRICS_JSON " + json.dumps({"actors_created": n}), flush=True)
    return 0


def verify() -> int:
    snapshot = json.loads(os.environ["SNAPSHOT"])
    ray.init(namespace=NAMESPACE)
    recovered = {}
    for name in snapshot:
        try:
            a = ray.get_actor(name, namespace=NAMESPACE)
            recovered[name] = ray.get(a.value_.remote())
        except Exception:  # noqa: BLE001
            pass
    pct = 100.0 * sum(
        1 for k, v in snapshot.items() if recovered.get(k) == v
    ) / max(len(snapshot), 1)

    # New tasks: confirm cluster is serving.
    @ray.remote
    def f(x):
        return x + 1
    new_ok = ray.get(f.remote(1)) == 2

    metrics = {
        "actors_created": len(snapshot),
        "actors_recovered": len(recovered),
        "state_preserved_pct": pct,
        "new_tasks_executed": int(new_ok),
        "new_tasks_ok": new_ok,
    }
    print("METRICS_JSON " + json.dumps(metrics), flush=True)
    return 0 if pct >= 95 and new_ok else 1


if __name__ == "__main__":
    phase = os.environ.get("PHASE", "")
    if phase == "setup":
        sys.exit(setup())
    if phase == "verify":
        sys.exit(verify())
    print(f"unknown PHASE: {phase!r}", file=sys.stderr)
    sys.exit(2)
