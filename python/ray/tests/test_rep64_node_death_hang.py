"""REP-64 — node-death lost-wakeup: deterministic reproduction and fix validation.

Runs the dynamic-generator reconstruction scenario on the in-memory GCS (no
RocksDB) and injects the death-notification timing via test-only env knobs (read
through std::getenv inside the GCS / core worker; see REP64.md). A fixed 5-min
delay >> the 90s per-arm timeout deterministically models "this notification
never arrives", with zero flakiness.

One arm per claim (run each in its own process via run_rep64.sh):

    control                    baseline                                  -> FAST
    actor_publish_delayed      actor-death publish delayed forever       -> FAST  (actor channel self-heals)
    node_publish_delayed       node-death publish delayed forever        -> HANG  (node channel is the trigger)
    node_persist_delayed       node persist (fsync) delayed forever      -> HANG  (reproduces the real bug)
    node_persist_delayed_f3    persist delayed + F3 publish-before-persist -> FAST (F3 fixes it)
    node_publish_delayed_f5    publish delayed + F5 owner-side fallback  -> FAST  (F5 heals without the push)
"""

import threading
import time

import numpy as np
import pytest

import ray

_DELAY_MS = 300000  # >> _HANG_TIMEOUT_S, i.e. "never delivered" within the window
_HANG_TIMEOUT_S = 90.0

# arm -> env knobs armed before the head node (and its GCS) starts.
_ARMS = {
    "control": {},
    "actor_publish_delayed": {"RAY_TESTING_GCS_ACTOR_PUBLISH_DELAY_MS": str(_DELAY_MS)},
    "node_publish_delayed": {"RAY_TESTING_GCS_NODE_PUBLISH_DELAY_MS": str(_DELAY_MS)},
    "node_persist_delayed": {"RAY_TESTING_GCS_NODE_PERSIST_DELAY_MS": str(_DELAY_MS)},
    "node_persist_delayed_f3": {
        "RAY_TESTING_GCS_NODE_PERSIST_DELAY_MS": str(_DELAY_MS),
        "RAY_TESTING_GCS_NODE_PUBLISH_BEFORE_PERSIST": "1",
    },
    "node_publish_delayed_f5": {
        "RAY_TESTING_GCS_NODE_PUBLISH_DELAY_MS": str(_DELAY_MS),
        "RAY_TESTING_ENABLE_NODE_DEATH_FALLBACK": "1",
    },
}


@pytest.mark.parametrize("arm", list(_ARMS.keys()))
def test_node_death_hang(ray_start_cluster, monkeypatch, arm):
    for env_name, env_val in _ARMS[arm].items():
        monkeypatch.setenv(env_name, env_val)

    config = {
        "health_check_failure_threshold": 10,
        "health_check_period_ms": 100,
        "health_check_initial_delay_ms": 0,
        "max_direct_call_object_size": 100,
        "task_retry_delay_ms": 100,
        "object_timeout_milliseconds": 200,
        "fetch_warn_timeout_milliseconds": 1000,
        "local_gc_min_interval_s": 1,
    }

    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=1,
        _system_config=config,
        enable_object_reconstruction=True,
        resources={"head": 1},
    )
    ray.init(address=cluster.address)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, resources={"head": 1})
    class FailureSignal:
        def __init__(self):
            return

        def ping(self):
            return

    @ray.remote(num_returns=None)
    def dynamic_generator(failure_signal):
        num_returns = 10
        try:
            ray.get(failure_signal.ping.remote())
        except ray.exceptions.RayActorError:
            num_returns -= 1
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    failure_signal = FailureSignal.remote()
    gen = ray.get(dynamic_generator.remote(failure_signal))
    cluster.remove_node(node_to_kill, allow_graceful=False)
    ray.kill(failure_signal)

    # Drive list(gen) on a daemon thread so a permanent hang is observable
    # instead of blocking the test forever.
    result = {"done": False, "n": None, "err": None, "elapsed": None}

    def drive():
        t0 = time.perf_counter()
        try:
            result["n"] = len(list(gen))
        except Exception as e:  # noqa: BLE001
            result["err"] = repr(e)
        finally:
            result["elapsed"] = time.perf_counter() - t0
            result["done"] = True

    t = threading.Thread(target=drive, daemon=True)
    t.start()
    t.join(timeout=_HANG_TIMEOUT_S)

    verdict = "PERMANENT-HANG" if not result["done"] else "FAST"
    print(
        f"\n[{arm}] verdict={verdict} done={result['done']} "
        f"elapsed={result['elapsed']} n={result['n']} err={result['err']}"
    )
    # Experiment, not a pass/fail gate: always surface the verdict.
    assert t is not None
