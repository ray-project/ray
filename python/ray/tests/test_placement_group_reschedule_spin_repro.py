"""Repro for the GCS placement-group RESCHEDULING busy-spin.

Bug (present in ray-2.53.0 through current master):
    When a node that hosts a bundle of a CREATED placement group dies, the GCS
    moves the PG to RESCHEDULING and re-queues it via
    ``AddToPendingQueue(pg, /*rank=*/0)`` with **no exponential backoff**
    (``gcs_placement_group_manager.cc``). If the PG is *feasible but currently
    unplaceable* (a node of the right shape exists but has no free resources --
    e.g. a packed cluster that just lost a node), the scheduler re-attempts the
    placement as fast as the GCS main io_context can cycle -- thousands of
    passes per second -- starving the GCS event loop.

    A normally PENDING unschedulable PG does NOT hit this: its requeue carries
    an ExponentialBackoff (100ms -> 1000ms), so it is bounded to <= ~10/s.

This test reconstructs the exact precondition with ``ray.cluster_utils`` and
measures the scheduler attempt rate straight from the GCS "Gcs Debug state"
counter (dumped every second here via ``event_stats_print_interval_ms``).

Expected outcome:
    * On buggy Ray: after the node dies the counter climbs by thousands/s and
      this test FAILS at the final assertion (that is the repro).
    * After adding backoff to the RESCHEDULING requeue path: the rate stays
      bounded (~1/s) and this test PASSES.
"""

import os
import re
import time

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.util.placement_group import placement_group, placement_group_table

# Dump "Gcs Debug state" (incl. the PG scheduling-attempt counter) every second,
# and detect a dead node within ~2s so the RESCHEDULING path engages promptly.
_SYSTEM_CONFIG = {
    "event_stats_print_interval_ms": 1000,
    "health_check_initial_delay_ms": 0,
    "health_check_period_ms": 1000,
    "health_check_failure_threshold": 2,
}

# Correct (backoff-bounded) behavior is ~1 attempt/s; the busy spin is thousands/s.
_MAX_ATTEMPTS_PER_SEC = 100.0

_COUNT_RE = re.compile(r"Scheduling pending placement group count: (\d+)")


def _gcs_logs_dir(cluster):
    for getter in (
        lambda: cluster.head_node.get_session_dir_path(),
        lambda: ray._private.worker._global_node.get_session_dir_path(),
    ):
        try:
            session_dir = getter()
        except Exception:
            continue
        if session_dir:
            return os.path.join(session_dir, "logs")
    raise RuntimeError("could not locate the GCS session/log directory")


def _latest_attempt_count(logs_dir):
    """Highest 'scheduling pending PG count' seen across the GCS log files."""
    latest = None
    for name in ("gcs_server.out", "gcs_server.err"):
        path = os.path.join(logs_dir, name)
        if not os.path.exists(path):
            continue
        with open(path, "r") as f:
            matches = _COUNT_RE.findall(f.read())
        if matches:
            value = int(matches[-1])
            latest = value if latest is None else max(latest, value)
    return latest


def _wait_for_first_count(logs_dir, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _latest_attempt_count(logs_dir) is not None:
            return
        time.sleep(0.5)
    raise AssertionError("GCS never emitted a debug-state PG counter")


def _measure_attempt_rate(logs_dir, window_s=3.0):
    """Cumulative scheduling passes per second over ``window_s``."""
    start = _latest_attempt_count(logs_dir)
    t0 = time.monotonic()
    time.sleep(window_s)
    end = _latest_attempt_count(logs_dir)
    dt = time.monotonic() - t0
    if start is None or end is None:
        return 0.0
    return (end - start) / dt


def _wait_for_pg_state(pg, state, timeout=20.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = placement_group_table(pg)["state"]
        if last == state:
            return
        time.sleep(0.5)
    raise AssertionError(f"PG never reached {state!r}; last state was {last!r}")


@pytest.fixture
def cluster():
    c = Cluster(
        initialize_head=True,
        head_node_args={"num_cpus": 0, "_system_config": _SYSTEM_CONFIG},
    )
    ray.init(address=c.address)
    try:
        yield c
    finally:
        ray.shutdown()
        c.shutdown()


def test_rescheduling_pg_does_not_busy_spin(cluster):
    # Two worker nodes can each host a {"slot": 1} bundle. Only the second also
    # carries a unique "pin2" resource, which we use to pin an occupier actor
    # there and consume that node's slot -- so the target PG can only land on w1,
    # and after w1 dies the PG is feasible (w2's shape fits) but unplaceable
    # (w2's slot is taken). That is the exact trigger for the busy spin.
    w1 = cluster.add_node(num_cpus=1, resources={"slot": 1})
    cluster.add_node(num_cpus=1, resources={"slot": 1, "pin2": 1})
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=0, resources={"slot": 1, "pin2": 1})
    class Occupier:
        def ping(self):
            return True

    occupier = Occupier.remote()
    ray.get(occupier.ping.remote(), timeout=30)

    pg = placement_group([{"slot": 1}], strategy="PACK")
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"

    logs_dir = _gcs_logs_dir(cluster)
    _wait_for_first_count(logs_dir)

    baseline_rate = _measure_attempt_rate(logs_dir)

    # Simulate the GCE host-maintenance kill of the node hosting the bundle.
    cluster.remove_node(w1, allow_graceful=False)
    _wait_for_pg_state(pg, "RESCHEDULING")

    spin_rate = _measure_attempt_rate(logs_dir)
    print(
        f"\n[repro] scheduling attempt rate: baseline={baseline_rate:.1f}/s "
        f"after-node-death={spin_rate:.0f}/s (threshold {_MAX_ATTEMPTS_PER_SEC:.0f}/s)"
    )

    assert spin_rate < _MAX_ATTEMPTS_PER_SEC, (
        f"GCS placement-group scheduler busy-spun at {spin_rate:.0f} passes/s "
        f"(baseline {baseline_rate:.1f}/s) after the node hosting a bundle died "
        f"and the PG became feasible-but-unplaceable. The RESCHEDULING requeue "
        f"path re-inserts at rank=0 with no backoff "
        f"(gcs_placement_group_manager.cc). Expected < {_MAX_ATTEMPTS_PER_SEC:.0f}/s."
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
