"""Unit tests for preemption handling. No Ray cluster required.

Covers the four non-TorchFT paths (A/B/C/D) at the state-machine + failure-
policy level by feeding synthetic WorkerStatus values into the helpers
directly. End-to-end UDF behavior is exercised separately by
``preemption_integration_demo.py``.
"""

import time
from unittest.mock import MagicMock

import pytest

from ray.train.v2._internal.execution.controller.state import (
    PreemptingState,
    TrainControllerStateType,
)
from ray.train.v2._internal.execution.failure_handling.default import (
    DefaultFailurePolicy,
)
from ray.train.v2._internal.execution.failure_handling.failure_policy import (
    FailureDecision,
)
from ray.train.v2._internal.execution.preemption import (
    PreemptionInfo,
    make_preemption_info_for_worker,
)
from ray.train.v2._internal.execution.worker_group.poll import (
    WorkerGroupPollStatus,
    WorkerStatus,
)
from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.exceptions import (
    PreemptionError,
    TrainingFailedError,
    WorkerGroupError,
)


# ─── PreemptionInfo basics ──────────────────────────────────────────────────


def test_preemption_info_seconds_remaining_decreases():
    info = PreemptionInfo(
        deadline=time.time() + 10,
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-a"],
    )
    s1 = info.seconds_remaining
    time.sleep(0.01)
    s2 = info.seconds_remaining
    assert s1 > s2


def test_preemption_info_seconds_remaining_inf_when_no_deadline():
    info = PreemptionInfo(deadline=float("inf"), reason="drain")
    assert info.seconds_remaining == float("inf")


def test_preemption_info_clamps_to_zero():
    info = PreemptionInfo(deadline=time.time() - 100, reason="drain")
    assert info.seconds_remaining == 0.0


def test_make_preemption_info_for_worker_sets_flag_correctly():
    base = PreemptionInfo(
        deadline=time.time() + 25,
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-a", "node-b"],
    )
    assert make_preemption_info_for_worker(base, 2).this_worker_preempted
    assert make_preemption_info_for_worker(base, 3).this_worker_preempted
    assert not make_preemption_info_for_worker(base, 0).this_worker_preempted
    assert not make_preemption_info_for_worker(base, 1).this_worker_preempted


# ─── PreemptingState ────────────────────────────────────────────────────────


def test_preempting_state_carries_info():
    info = PreemptionInfo(
        deadline=time.time() + 10, reason="drain", preempted_ranks=[1]
    )
    ps = PreemptingState(preemption_info=info)
    assert ps.preemption_info is info
    assert ps._state_type == TrainControllerStateType.PREEMPTING
    assert not ps.is_terminal()
    assert not ps.needs_new_run_attempt()


# ─── DefaultFailurePolicy: PreemptionError branch ───────────────────────────


def test_preemption_budget_is_independent_of_max_failures():
    """Path A: planned preemption must not consume the unplanned-failure budget."""
    cfg = FailureConfig(max_failures=0, max_preemption_failures=3)
    policy = DefaultFailurePolicy(cfg)

    err = PreemptionError(
        "test", preempted_ranks=[2], preempted_node_ids=["n"], deadline_exceeded=False
    )
    # 3 retries allowed, 4th raises
    assert policy.make_decision(err) == FailureDecision.RETRY
    assert policy.make_decision(err) == FailureDecision.RETRY
    assert policy.make_decision(err) == FailureDecision.RETRY
    assert policy.make_decision(err) == FailureDecision.RAISE


def test_preemption_zero_budget_raises_immediately():
    """Path B: max_preemption_failures=0 means no retry on planned preemption."""
    cfg = FailureConfig(max_failures=10, max_preemption_failures=0)
    policy = DefaultFailurePolicy(cfg)
    err = PreemptionError("test", preempted_ranks=[2], preempted_node_ids=["n"])
    assert policy.make_decision(err) == FailureDecision.RAISE


def test_preemption_unlimited_budget():
    cfg = FailureConfig(max_failures=0, max_preemption_failures=-1)
    policy = DefaultFailurePolicy(cfg)
    err = PreemptionError("test", preempted_ranks=[2], preempted_node_ids=["n"])
    for _ in range(50):
        assert policy.make_decision(err) == FailureDecision.RETRY


def test_worker_group_error_budget_independent_from_preemption():
    """An unplanned WG error consumes max_failures, NOT max_preemption_failures."""
    cfg = FailureConfig(max_failures=2, max_preemption_failures=5)
    policy = DefaultFailurePolicy(cfg)

    wg_err = WorkerGroupError("test", worker_failures={})
    assert policy.make_decision(wg_err) == FailureDecision.RETRY
    assert policy.make_decision(wg_err) == FailureDecision.RETRY
    assert policy.make_decision(wg_err) == FailureDecision.RAISE

    # preemption budget still untouched
    preempt_err = PreemptionError(
        "test", preempted_ranks=[2], preempted_node_ids=["n"]
    )
    for _ in range(5):
        assert policy.make_decision(preempt_err) == FailureDecision.RETRY


# ─── _should_leave_preempting / _build_preemption_error ─────────────────────


def _fake_controller(manages_replica_groups: bool, worker_statuses):
    """Build a stub controller exposing just the methods we test."""
    from ray.train.v2._internal.execution.controller.controller import (
        TrainController,
    )

    # Bind methods to a stub via __get__ so we don't instantiate the actor.
    stub = MagicMock()
    stub._manages_replica_groups = manages_replica_groups
    stub._worker_group = MagicMock()
    stub._worker_group.get_latest_poll_status.return_value = (
        WorkerGroupPollStatus(worker_statuses=worker_statuses)
    )
    stub._should_leave_preempting = (
        TrainController._should_leave_preempting.__get__(stub)
    )
    stub._build_preemption_error = (
        TrainController._build_preemption_error.__get__(stub)
    )
    return stub


def test_should_leave_preempting_path_A_all_clean_exits():
    """Path A: API used, all preempted ranks returned cleanly, survivors also done."""
    info = PreemptionInfo(
        deadline=time.time() + 25,
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-b"],
    )
    statuses = {
        0: WorkerStatus(running=False, return_value="ok"),  # survivor exited
        1: WorkerStatus(running=False, return_value="ok"),
        2: WorkerStatus(running=False, return_value="ok"),  # preempted, clean
        3: WorkerStatus(running=False, return_value="ok"),
    }
    ctrl = _fake_controller(manages_replica_groups=False, worker_statuses=statuses)
    assert ctrl._should_leave_preempting(info) is True


def test_should_leave_preempting_waits_for_survivor():
    """Path A in progress: preempted ranks exited but survivors still writing JIT ckpt."""
    info = PreemptionInfo(
        deadline=time.time() + 25,
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-b"],
    )
    statuses = {
        0: WorkerStatus(running=True),  # survivor still going
        1: WorkerStatus(running=True),
        2: WorkerStatus(running=False, return_value="ok"),
        3: WorkerStatus(running=False, return_value="ok"),
    }
    ctrl = _fake_controller(manages_replica_groups=False, worker_statuses=statuses)
    assert ctrl._should_leave_preempting(info) is False


def test_should_leave_preempting_deadline_expired():
    """Path C/D safety net: deadline expired triggers transition regardless."""
    info = PreemptionInfo(
        deadline=time.time() - 1,  # already past
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-b"],
    )
    statuses = {
        0: WorkerStatus(running=True),
        1: WorkerStatus(running=True),
        2: WorkerStatus(running=True),  # still running, but deadline passed
        3: WorkerStatus(running=True),
    }
    ctrl = _fake_controller(manages_replica_groups=False, worker_statuses=statuses)
    assert ctrl._should_leave_preempting(info) is True


def test_should_leave_preempting_torchft_does_not_wait_for_survivors():
    """A-torchft: leave as soon as preempted RG ranks have exited."""
    info = PreemptionInfo(
        deadline=time.time() + 25,
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-b"],
    )
    statuses = {
        0: WorkerStatus(running=True),  # survivor RG keeps stepping
        1: WorkerStatus(running=True),
        2: WorkerStatus(running=False, return_value="ok"),  # affected RG exited
        3: WorkerStatus(running=False, return_value="ok"),
    }
    ctrl = _fake_controller(manages_replica_groups=True, worker_statuses=statuses)
    assert ctrl._should_leave_preempting(info) is True


def test_build_preemption_error_synthesizes_on_clean_exits():
    """Rule 1: clean-exit preempted ranks must get a PreemptionError synthesized
    into their WorkerStatus.error so failing_replica_group_indices picks them up."""
    info = PreemptionInfo(
        deadline=time.time() + 25,
        reason="drain",
        preempted_ranks=[2, 3],
        preempted_node_ids=["node-b"],
    )
    statuses = {
        0: WorkerStatus(running=False, return_value="ok"),
        1: WorkerStatus(running=False, return_value="ok"),
        2: WorkerStatus(running=False, return_value="ok"),  # clean exit
        3: WorkerStatus(running=False, return_value="ok"),
    }
    ctrl = _fake_controller(manages_replica_groups=False, worker_statuses=statuses)
    err = ctrl._build_preemption_error(info)

    assert isinstance(err, PreemptionError)
    assert isinstance(err, TrainingFailedError)
    assert err.preempted_ranks == [2, 3]
    assert not err.deadline_exceeded
    # Synthesized errors are now on the preempted ranks' WorkerStatus
    assert isinstance(statuses[2].error, PreemptionError)
    assert isinstance(statuses[3].error, PreemptionError)
    # Survivor ranks unchanged
    assert statuses[0].error is None
    assert statuses[1].error is None


def test_build_preemption_error_preserves_existing_errors():
    """Path C: preempted ranks died via RayActorError. Don't clobber the
    original error; just include it in worker_failures."""
    underlying = RuntimeError("RayActorError: pod evicted")
    info = PreemptionInfo(
        deadline=time.time() - 1,  # deadline passed
        reason="drain",
        preempted_ranks=[2],
        preempted_node_ids=["node-b"],
    )
    statuses = {
        0: WorkerStatus(running=False, error=underlying),  # NCCL hang then die
        1: WorkerStatus(running=False, error=underlying),
        2: WorkerStatus(running=False, error=underlying),  # actually preempted
    }
    ctrl = _fake_controller(manages_replica_groups=False, worker_statuses=statuses)
    err = ctrl._build_preemption_error(info)
    assert err.deadline_exceeded is True
    assert statuses[2].error is underlying  # not clobbered


# ─── PreemptionCallback drain mapping ───────────────────────────────────────


def _fake_worker(rank, node_id, slice_resource=None):
    w = MagicMock()
    w.distributed_context.world_rank = rank
    w.metadata.node_id = node_id
    w.resources = {slice_resource: 1.0} if slice_resource else {}
    return w


def test_failure_domain_node_mapping():
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )

    workers = [
        _fake_worker(0, "node-a"),
        _fake_worker(1, "node-a"),
        _fake_worker(2, "node-b"),
        _fake_worker(3, "node-b"),
    ]
    wg = MagicMock()
    wg.get_workers.return_value = workers
    cb = PreemptionCallback(
        controller_actor=None, drain_source=lambda: {}, failure_domain="node"
    )
    mapping = cb._build_failure_domain_map(wg)
    # node-a: ranks 0, 1; node-b: ranks 2, 3
    assert mapping["node-a"] == [0, 1]
    assert mapping["node-b"] == [2, 3]


def test_failure_domain_tpu_slice_mapping():
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )

    workers = [
        _fake_worker(0, "node-a", slice_resource="TPU-v6e-8-head-slice-0"),
        _fake_worker(1, "node-b", slice_resource="TPU-v6e-8-head-slice-0"),
        _fake_worker(2, "node-c", slice_resource="TPU-v6e-8-head-slice-1"),
        _fake_worker(3, "node-d", slice_resource="TPU-v6e-8-head-slice-1"),
    ]
    wg = MagicMock()
    wg.get_workers.return_value = workers
    cb = PreemptionCallback(
        controller_actor=None, drain_source=lambda: {}, failure_domain="tpu_slice"
    )
    mapping = cb._build_failure_domain_map(wg)
    # Each host maps to ALL ranks in its slice.
    assert mapping["node-a"] == [0, 1]
    assert mapping["node-b"] == [0, 1]
    assert mapping["node-c"] == [2, 3]
    assert mapping["node-d"] == [2, 3]


def test_on_drain_change_torchft_signals_only_affected():
    """Rule 3: TorchFT mode -> only preempted ranks get mark_preempt."""
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )

    workers = [
        _fake_worker(i, f"node-{i}") for i in range(4)
    ]
    for w in workers:
        w.actor.mark_preempt.remote = MagicMock()

    wg = MagicMock()
    wg.get_workers.return_value = workers

    fake_controller = MagicMock()

    cb = PreemptionCallback(
        controller_actor=fake_controller,
        manages_replica_groups=True,
        drain_source=lambda: {},
        failure_domain="node",
    )
    # Manually wire wg + domain map (simulating after_worker_group_start).
    cb._wg = wg
    cb._node_to_domain_ranks = cb._build_failure_domain_map(wg)

    deadline_ms = int((time.time() + 25) * 1000)
    cb._on_drain_change({"node-2": deadline_ms})  # rank 2 drained

    # Controller was notified
    fake_controller.notify_preempting.remote.assert_called_once()

    # Only rank 2 was signaled
    assert workers[0].actor.mark_preempt.remote.call_count == 0
    assert workers[1].actor.mark_preempt.remote.call_count == 0
    assert workers[2].actor.mark_preempt.remote.call_count == 1
    assert workers[3].actor.mark_preempt.remote.call_count == 0


def test_resolve_deadline_uses_earliest_when_reported():
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )
    # Earliest (min) wins.
    d = PreemptionCallback._resolve_deadline([2000, 1000, 3000])
    assert d == 1.0  # 1000ms = 1.0s since epoch (just verify it's converted)


def test_resolve_deadline_default_60s_when_no_deadlines(monkeypatch):
    """When Ray Core reports drain but no deadline, we fall back to a
    bounded default (NOT inf) so a UDF that acks-but-forgets-to-exit
    can't hang the controller forever."""
    monkeypatch.delenv("RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S", raising=False)
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )
    before = time.time()
    d = PreemptionCallback._resolve_deadline([])
    after = time.time()
    # Default is 60s from now.
    assert before + 59 <= d <= after + 61, f"deadline={d}"


def test_resolve_deadline_user_override(monkeypatch):
    monkeypatch.setenv("RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S", "5")
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )
    before = time.time()
    d = PreemptionCallback._resolve_deadline([])
    after = time.time()
    assert before + 4 <= d <= after + 6, f"deadline={d}"


def test_resolve_deadline_inf_opt_in(monkeypatch):
    """Explicit opt-in to unbounded wait."""
    monkeypatch.setenv("RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S", "inf")
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )
    assert PreemptionCallback._resolve_deadline([]) == float("inf")

    monkeypatch.setenv("RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S", "none")
    assert PreemptionCallback._resolve_deadline([]) == float("inf")


def test_resolve_deadline_invalid_value_falls_back(monkeypatch):
    monkeypatch.setenv("RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S", "garbage")
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )
    before = time.time()
    d = PreemptionCallback._resolve_deadline([])
    after = time.time()
    # Falls back to 60s default after warning.
    assert before + 59 <= d <= after + 61, f"deadline={d}"


def test_parse_fake_node_map():
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )

    assert PreemptionCallback._parse_fake_node_map(None) == {}
    assert PreemptionCallback._parse_fake_node_map("") == {}
    assert PreemptionCallback._parse_fake_node_map("0:nodeA,1:nodeB") == {
        0: "nodeA",
        1: "nodeB",
    }
    # Whitespace tolerated
    assert PreemptionCallback._parse_fake_node_map(" 0:nodeA , 1:nodeB ") == {
        0: "nodeA",
        1: "nodeB",
    }
    # Malformed entry is skipped, valid entries kept
    assert PreemptionCallback._parse_fake_node_map("0:nodeA,bogus,2:nodeC") == {
        0: "nodeA",
        2: "nodeC",
    }


def test_fake_node_map_enables_survivor_path(monkeypatch):
    """With RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP, a single-node local cluster
    can simulate multi-node so that some ranks are survivors."""
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )

    monkeypatch.setenv(
        "RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP", "0:nodeA,1:nodeB,2:nodeA,3:nodeB"
    )

    # All four workers physically on the same node; fake map splits them.
    workers = [_fake_worker(i, "same-real-node") for i in range(4)]
    for w in workers:
        w.actor.mark_preempt.remote = MagicMock()
    wg = MagicMock()
    wg.get_workers.return_value = workers
    fake_controller = MagicMock()

    cb = PreemptionCallback(
        controller_actor=fake_controller,
        manages_replica_groups=False,
        drain_source=lambda: {},
        failure_domain="node",
    )
    cb._wg = wg
    cb._node_to_domain_ranks = cb._build_failure_domain_map(wg)

    # The mapping should use the FAKE node IDs.
    assert sorted(cb._node_to_domain_ranks["nodeA"]) == [0, 2]
    assert sorted(cb._node_to_domain_ranks["nodeB"]) == [1, 3]

    # Drain only nodeB → ranks 1 and 3 preempted; ranks 0 and 2 are survivors.
    cb._on_drain_change({"nodeB": int((time.time() + 25) * 1000)})

    calls = [w.actor.mark_preempt.remote.call_args[0][0] for w in workers]
    assert calls[0].this_worker_preempted is False  # survivor
    assert calls[1].this_worker_preempted is True   # preempted
    assert calls[2].this_worker_preempted is False  # survivor
    assert calls[3].this_worker_preempted is True   # preempted
    assert calls[0].preempted_ranks == [1, 3]


def test_on_drain_change_non_torchft_signals_everyone():
    """Non-TorchFT mode -> ALL workers get mark_preempt (with per-worker
    this_worker_preempted)."""
    from ray.train.v2._internal.execution.preemption_callback import (
        PreemptionCallback,
    )

    workers = [_fake_worker(i, f"node-{i}") for i in range(4)]
    for w in workers:
        w.actor.mark_preempt.remote = MagicMock()

    wg = MagicMock()
    wg.get_workers.return_value = workers
    fake_controller = MagicMock()

    cb = PreemptionCallback(
        controller_actor=fake_controller,
        manages_replica_groups=False,
        drain_source=lambda: {},
        failure_domain="node",
    )
    cb._wg = wg
    cb._node_to_domain_ranks = cb._build_failure_domain_map(wg)

    cb._on_drain_change({"node-2": int((time.time() + 25) * 1000)})

    # All four workers signaled.
    for w in workers:
        assert w.actor.mark_preempt.remote.call_count == 1

    # Only rank 2's info has this_worker_preempted=True.
    calls = [w.actor.mark_preempt.remote.call_args[0][0] for w in workers]
    assert not calls[0].this_worker_preempted
    assert not calls[1].this_worker_preempted
    assert calls[2].this_worker_preempted is True
    assert not calls[3].this_worker_preempted


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
