"""Integration tests for memory-pressure-driven in-place pod resize (IPPR).

Verify that a Raylet-observed pressure signal flows end-to-end through
the autoscaler v2 pipeline:

        ResourcesData heartbeat        (raylet → GCS)
            │
            ▼
        ClusterResourceState           (autoscaler ingest)
            │
            ▼
        IPPRStatus.memory_pressure  (IPPRStatus.apply_pressure_signal,
            │                           absorbed from instance.ray_node at the
            │                           scheduler injection point — same place and
            │                           same source NodeState as raylet_id, no
            │                           provider side-channel)
            ▼
        Phase B' one-shot jump to max  (ResourceDemandScheduler.schedule)
            │
            ▼
        SchedulingReply.to_ippr        (queued resize for the pressured pod)

A full hardware-driven setup would use stress-ng plus a real Raylet to drive
the C++ MemoryPressureMonitor. That requires either consuming the host's
RAM up to ``/sys/fs/cgroup/memory.max`` (unsafe on this dev box) or
extending ``FileMemoryPressureReader`` with an env-var override and
rebuilding the C++ artifacts. Since the C++ side already has full gtest
coverage (``memory_pressure_reader_test`` +
``ratio_hysteresis_pressure_monitor_test``),
this test fakes the Raylet by setting the synthetic ``memory_pressure_ratio``
field on the matching ``NodeState`` — the same field a real raylet heartbeat
populates — and asserts every Python-side stage consumes it correctly,
through the actual scheduler injection point (no hand-set
``memory_pressure`` shortcut).
"""

import logging
import os
import sys
import unittest

import pytest

import ray
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.scheduler import NodeTypeConfig, ResourceDemandScheduler
from ray.autoscaler.v2.schema import (
    IPPRGroupSpec,
    IPPRSpecs,
    IPPRStatus,
)
from ray.autoscaler.v2.tests.test_scheduler import sched_request
from ray.autoscaler.v2.tests.util import MockEventLogger, make_autoscaler_instance
from ray.core.generated.autoscaler_pb2 import NodeState
from ray.core.generated.instance_manager_pb2 import Instance


GiB = 1024 * 1024 * 1024


def _scheduler():
    return ResourceDemandScheduler(
        AutoscalerEventLogger(MockEventLogger(logging.getLogger(__name__)))
    )


def _pressure_instance(
    raylet_hex: str,
    cloud_id: str,
    *,
    ratio: float = None,
):
    """Build an autoscaler instance whose ray_node carries (or omits) a raylet
    memory-pressure signal — the same field a real heartbeat populates.

    When ``ratio`` is None, no signal is hung on the node (a quiet raylet).
    Field presence is the signal; the ratio value itself is observability-only.
    The signal is NOT pre-folded into the IPPRStatus; it must be absorbed by the
    scheduler injection point.
    """
    ray_node = NodeState(
        ray_node_type_name="type_1",
        available_resources={"CPU": 1, "memory": 4 * GiB},
        total_resources={"CPU": 1, "memory": 4 * GiB},
        node_id=bytes.fromhex(raylet_hex),
    )
    if ratio is not None:
        ray_node.memory_pressure_ratio = ratio
    return make_autoscaler_instance(
        ray_node=ray_node,
        im_instance=Instance(
            instance_type="type_1",
            status=Instance.RAY_RUNNING,
            instance_id=cloud_id,
            node_id=raylet_hex,
        ),
        cloud_instance_id=cloud_id,
    )


def _spec():
    return IPPRGroupSpec(
        min_cpu=1,
        max_cpu=4,
        min_memory=1 * GiB,
        max_memory=12 * GiB,
        resize_timeout=60,
    )


def _status(cloud_id: str, spec):
    # memory_pressure intentionally unset — the injection point must absorb
    # it from the matching ray_node, not the test.
    return IPPRStatus(
        cloud_instance_id=cloud_id,
        spec=spec,
        current_cpu=1,
        current_memory=4 * GiB,
        desired_cpu=1,
        desired_memory=4 * GiB,
    )


def _node_type_configs():
    return {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1, "memory": 4 * GiB},
            min_worker_nodes=0,
            max_worker_nodes=10,
        )
    }


class TestRayletEmitsSignal(unittest.TestCase):
    """End-to-end Python-side wiring test (fake-monitor variant).

    Signal rides on ``NodeState.memory_pressure_ratio`` (real heartbeat field)
    and must be absorbed at the scheduler injection point by
    ``IPPRStatus.apply_pressure_signal`` — no pre-folded ``memory_pressure``.
    """

    def test_raylet_emits_signal(self):
        """Pressure signal on NodeState → injection point absorbs it →
        scheduler Phase B' jumps memory straight to max."""

        raylet_hex = "a" * 56
        spec = _spec()
        status = _status("ray-worker-1", spec)

        instance = _pressure_instance(raylet_hex, "ray-worker-1", ratio=0.9)
        request = sched_request(
            node_type_configs=_node_type_configs(),
            resource_requests=[],
            instances=[instance],
            ippr_specs=IPPRSpecs(groups={"type_1": spec}),
            ippr_statuses={"ray-worker-1": status},
        )
        reply = _scheduler().schedule(request)

        # Injection point absorbed the signal off the node (no hand-set field).
        self.assertTrue(
            status.memory_pressure,
            "memory_pressure must be absorbed at the injection point",
        )

        self.assertEqual(len(reply.to_ippr), 1)
        queued = reply.to_ippr[0]
        self.assertEqual(queued.cloud_instance_id, "ray-worker-1")
        # One-shot jump straight to max_memory (12Gi).
        self.assertEqual(queued.desired_memory, 12 * GiB)
        # CPU unchanged — pressure is memory-only.
        self.assertEqual(queued.desired_cpu, 1)

    def test_only_pressured_pod_scales_quiet_pod_untouched(self):
        """Two pods share one IPPRSpec group; only the pressured pod whose
        NodeState carries a signal is scaled up by Phase B'; the quiet pod gets no
        IPPR request. Rules out "faking it by hand-setting fields"."""
        spec = _spec()

        pressured = _pressure_instance("a" * 56, "pod-1", ratio=0.95)
        quiet = _pressure_instance("b" * 56, "pod-2")  # no signal on node

        request = sched_request(
            node_type_configs=_node_type_configs(),
            resource_requests=[],
            instances=[pressured, quiet],
            ippr_specs=IPPRSpecs(groups={"type_1": spec}),
            ippr_statuses={
                "pod-1": _status("pod-1", spec),
                "pod-2": _status("pod-2", spec),
            },
        )
        reply = _scheduler().schedule(request)

        self.assertEqual(len(reply.to_ippr), 1)
        self.assertEqual(reply.to_ippr[0].cloud_instance_id, "pod-1")
        self.assertEqual(reply.to_ippr[0].desired_memory, 12 * GiB)
        self.assertEqual(reply.to_launch, [])

    def test_any_signal_triggers_without_python_side_threshold(self):
        """The threshold decision is now entirely delegated to the C++ raylet:
        as long as the autoscaler receives a signal, the Python side no longer
        re-compares the ratio and unconditionally absorbs it as
        memory_pressure. This case uses a deliberately low ratio (a real raylet
        would never emit such a signal; it only verifies Python no longer filters)
        and still triggers a scale-up."""
        raylet_hex = "c" * 56
        spec = _spec()
        status = _status("ray-worker-1", spec)

        instance = _pressure_instance(raylet_hex, "ray-worker-1", ratio=0.7)
        request = sched_request(
            node_type_configs=_node_type_configs(),
            resource_requests=[],
            instances=[instance],
            ippr_specs=IPPRSpecs(groups={"type_1": spec}),
            ippr_statuses={"ray-worker-1": status},
        )
        reply = _scheduler().schedule(request)

        # The presence of a signal triggers; not filtered by Python for a low
        # ratio; scales up to max.
        self.assertTrue(status.memory_pressure)
        self.assertEqual(len(reply.to_ippr), 1)
        self.assertEqual(reply.to_ippr[0].desired_memory, 12 * GiB)


# =============================================================================
# Real-Raylet variant: drive the actual C++ MemoryPressureMonitor by
# injecting a fake cgroup tree, then assert the signal surfaces all the way at
# the GCS ClusterResourceState. Covers the C++→GCS→Python hop that the
# fake-proto tests above cannot reach.
#
# Mechanism: a single-process ``ray.init`` boots one real raylet. We point the
# raylet's FileMemoryPressureReader at a tmp dir holding static
# ``memory.current`` / ``memory.max`` files via the ``RAY_IPPR_TEST_CGROUP_ROOT``
# environment variable, which the pressure-monitor factory reads directly with
# ``std::getenv`` (NOT a RAY_CONFIG key, NOT ``_system_config``). The reader sees
# a deterministic ratio every poll — no host RAM is consumed. We then read the
# signal back via the autoscaler v2 SDK (``get_cluster_resource_state``), the
# same call the real autoscaler reconcile loop uses.
#
# IMPORTANT — why a plain env var, not ``_system_config``: this suite runs inside
# the ``ray-ippr:dev`` image, which keeps the upstream Cython ``_raylet.so`` (for
# ABI safety) while swapping only the rebuilt raylet/gcs_server C++ binaries. The
# stale ``.so``'s ``RayConfig::initialize`` validates every key in the
# ``_system_config`` JSON (config_list path) and FATAL-aborts on a key it does
# not know. A plain ``std::getenv`` env var sidesteps RayConfig entirely: the
# stale core-worker .so never looks at ``RAY_IPPR_TEST_CGROUP_ROOT``, while the
# rebuilt raylet binary reads it in the factory. So this env var is the only
# injection channel that works without rebuilding the .so.
# =============================================================================


def _write_fake_cgroup_v2(root, current_bytes: int, limit_bytes: int) -> str:
    """Write a fake cgroup v2 tree in a tmp directory and return its path string.

    The presence of cgroup.controllers triggers the reader's v2 probe;
    memory.current/max are static files, so the raylet reads the same value on
    every poll -> a stable ratio.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "cgroup.controllers").write_text("memory io\n")
    (root / "memory.current").write_text(f"{current_bytes}\n")
    (root / "memory.max").write_text(f"{limit_bytes}\n")
    return str(root)


def _set_pressure_env(monkeypatch, cgroup_root: str, ratio: float = 0.85) -> None:
    """Inject monitor config via RAY_* environment variables (not via
    _system_config; see the note above).

    The poll interval is shortened to 200ms to speed up the test. monkeypatch
    ensures automatic restoration between cases.
    """
    monkeypatch.setenv("RAY_memory_pressure_monitor_enabled", "true")
    monkeypatch.setenv("RAY_IPPR_TEST_CGROUP_ROOT", cgroup_root)
    monkeypatch.setenv("RAY_memory_pressure_poll_interval_ms", "200")
    monkeypatch.setenv("RAY_memory_pressure_ratio", str(ratio))
    monkeypatch.setenv("RAY_memory_pressure_consecutive_hits", "2")


def _gcs_signals():
    """Read all nodes carrying a memory_pressure_ratio in the current GCS
    ClusterResourceState."""
    import ray as _ray
    from ray.autoscaler.v2.sdk import get_cluster_resource_state

    gcs_client = _ray._private.worker.global_worker.gcs_client
    state = get_cluster_resource_state(gcs_client)
    return [ns for ns in state.node_states if ns.HasField("memory_pressure_ratio")]


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="FileMemoryPressureReader reads cgroup-style files; Linux-only.",
)
class TestRealRayletEmitsSignal:
    """Real raylet + fake cgroup injection -- verifies the C++->GCS pass-through
    chain (real-raylet variant).

    Does not rely on conftest's ``shutdown_only`` fixture (conftest is not loaded
    when running a single file inside the container); instead uses an autouse
    ``_shutdown`` to force ray.shutdown() after each case, ensuring raylet /
    environment-variable isolation between cases.
    """

    @pytest.fixture(autouse=True)
    def _shutdown(self):
        # Ensure no leftover instance before Arrange; clean up after the case.
        if ray.is_initialized():
            ray.shutdown()
        yield
        if ray.is_initialized():
            ray.shutdown()

    def test_real_raylet_emits_signal_when_cgroup_ratio_exceeds_threshold(
        self, monkeypatch, tmp_path
    ):
        """fake cgroup ratio 0.93 >= threshold 0.85 -> memory_pressure_ratio
        visible at GCS."""
        # Arrange: 10GiB limit, 9.3GiB current -> ratio 0.93.
        limit = 10 * GiB
        current = int(9.3 * GiB)
        cgroup_root = _write_fake_cgroup_v2(tmp_path / "cg", current, limit)
        _set_pressure_env(monkeypatch, cgroup_root)
        ray.init(num_cpus=1)

        # Act: wait for the signal to become visible via raylet poll -> GCS
        # pass-through (GCS pulls once per 1s).
        from ray._common.test_utils import wait_for_condition

        def signal_present():
            return len(_gcs_signals()) >= 1

        wait_for_condition(signal_present, timeout=30, retry_interval_ms=1000)

        # Assert: ratio lands in the expected high range.
        signals = _gcs_signals()
        assert len(signals) >= 1
        ratio = signals[0].memory_pressure_ratio
        assert 0.9 <= ratio <= 1.0, f"ratio={ratio} not in [0.9,1.0]"

    def test_real_raylet_withholds_signal_when_cgroup_ratio_below_threshold(
        self, monkeypatch, tmp_path
    ):
        """fake cgroup ratio 0.10 < threshold 0.85 -> GCS never has a signal.

        Named "withholds": asserts the signal is *consistently absent* over a long
        enough observation window, not just for a single frame.
        """
        # Arrange: 10GiB limit, 1GiB current -> ratio 0.10.
        limit = 10 * GiB
        current = 1 * GiB
        cgroup_root = _write_fake_cgroup_v2(tmp_path / "cg", current, limit)
        _set_pressure_env(monkeypatch, cgroup_root)
        ray.init(num_cpus=1)

        # Act + Assert: poll continuously for ~10s (far beyond the 200ms poll + 1s
        # GCS pull), and there is never a signal.
        import time

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            assert len(_gcs_signals()) == 0, "a low ratio must not produce any signal"
            time.sleep(1.0)

    def test_real_raylet_propagates_ratio_unfiltered_through_autoscaler(
        self, monkeypatch, tmp_path
    ):
        """Raw ratio pass-through: the GCS-side ratio ~= current/limit, unmodified
        by the autoscaler.

        Named "unfiltered": constructs a non-round ratio (0.91...) and asserts the
        value GCS sees matches current/limit within a 1e-3 tolerance, proving there
        is no quantization / threshold rewriting along the way.
        """
        # Arrange: deliberately pick a non-round ratio. current/limit = 0.913.
        limit = 10 * GiB
        current = int(0.913 * limit)
        expected_ratio = current / limit
        cgroup_root = _write_fake_cgroup_v2(tmp_path / "cg", current, limit)
        _set_pressure_env(monkeypatch, cgroup_root)
        ray.init(num_cpus=1)

        # Act
        from ray._common.test_utils import wait_for_condition

        wait_for_condition(
            lambda: len(_gcs_signals()) >= 1, timeout=30, retry_interval_ms=1000
        )

        # Assert: raw pass-through, tolerance 1e-3.
        ratio = _gcs_signals()[0].memory_pressure_ratio
        assert (
            abs(ratio - expected_ratio) < 1e-3
        ), f"GCS ratio={ratio} deviates from current/limit={expected_ratio}"

    def test_real_signal_drives_phase_b_prime_resize_end_to_end(
        self, monkeypatch, tmp_path
    ):
        """Full chain: real C++ raylet produces a signal -> GCS NodeState ->
        scheduler injection point IPPRStatus.apply_pressure_signal absorbs it ->
        Phase B' scales up to max.

        This "bridge" connects the two existing variants: the real variant only
        verifies up to GCS visibility, the fake variant starts from a hand-built
        NodeState; this case feeds a *real GCS NodeState* directly into the real
        scheduler, proving the C++->GCS->injection point->scale-up chain has no
        break.
        """
        # Arrange: fake cgroup ratio 0.93 >= threshold, the real raylet must emit a
        # signal.
        limit = 10 * GiB
        current = int(9.3 * GiB)
        cgroup_root = _write_fake_cgroup_v2(tmp_path / "cg", current, limit)
        _set_pressure_env(monkeypatch, cgroup_root)
        ray.init(num_cpus=1)

        from ray._common.test_utils import wait_for_condition

        wait_for_condition(
            lambda: len(_gcs_signals()) >= 1, timeout=30, retry_interval_ms=1000
        )

        # Take the real GCS NodeState (with the real memory_pressure_ratio) as
        # instance.ray_node.
        gcs_node = _gcs_signals()[0]
        raylet_hex = gcs_node.node_id.hex()

        spec = IPPRGroupSpec(
            min_cpu=1,
            max_cpu=4,
            min_memory=1 * GiB,
            max_memory=12 * GiB,
            resize_timeout=60,
        )
        # memory_pressure is not preset -- it must be absorbed by the injection
        # point from the real NodeState.
        status = IPPRStatus(
            cloud_instance_id="ray-worker-1",
            spec=spec,
            current_cpu=1,
            current_memory=4 * GiB,
            desired_cpu=1,
            desired_memory=4 * GiB,
        )
        instance = make_autoscaler_instance(
            ray_node=gcs_node,
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="ray-worker-1",
                node_id=raylet_hex,
            ),
            cloud_instance_id="ray-worker-1",
        )
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 1, "memory": 4 * GiB},
                min_worker_nodes=0,
                max_worker_nodes=10,
            )
        }
        scheduler = ResourceDemandScheduler(
            AutoscalerEventLogger(MockEventLogger(logging.getLogger(__name__)))
        )
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[],
            instances=[instance],
            ippr_specs=IPPRSpecs(groups={"type_1": spec}),
            ippr_statuses={"ray-worker-1": status},
        )

        # Act
        reply = scheduler.schedule(request)

        # Assert: the real signal is absorbed by the injection point -> Phase B'
        # jumps in one shot to max (12Gi).
        assert status.memory_pressure is True
        assert len(reply.to_ippr) == 1
        assert reply.to_ippr[0].cloud_instance_id == "ray-worker-1"
        assert reply.to_ippr[0].desired_memory == 12 * GiB
        assert reply.to_ippr[0].desired_cpu == 1  # Memory pressure; CPU unchanged.


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
