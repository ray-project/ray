import logging
import time
import types
from collections import deque
from unittest.mock import MagicMock, call

import pytest

from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES,
    ActorPoolDrainPolicy,
    ResourceBudgetBackpressurePolicy,
    get_backpressure_policies,
)
from ray.data._internal.execution.backpressure_policy.actor_pool_drain_policy import (  # noqa: E501
    SAMPLES_PER_WINDOW,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data.context import (
    DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO,
    DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO,
    DataContext,
)


def _make_policy(
    window_s=10.0,
    stall_ratio=DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO,
    resume_ratio=DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO,
    cap_decay=1.0,
    cap_grow=0.1,
):
    ctx = MagicMock()
    ctx.actor_pool_drain_window_s = window_s
    ctx.actor_pool_drain_stall_ratio = stall_ratio
    ctx.actor_pool_drain_resume_ratio = resume_ratio
    rm = MagicMock()
    rm.is_op_eligible = types.MethodType(ResourceManager.is_op_eligible, rm)
    rm._is_blocking_materializing_op = types.MethodType(
        ResourceManager._is_blocking_materializing_op, rm
    )
    rm.get_downstream_eligible_ops = types.MethodType(
        ResourceManager.get_downstream_eligible_ops, rm
    )
    # `ActorPoolDrainPolicy` delegates its "downstream ran dry" safety valve to the
    # resource manager (shared with the autoscaler's upscale gate).
    rm.downstream_starved = types.MethodType(ResourceManager.downstream_starved, rm)
    # No ineligible pass-through operator between the op and the sink.
    rm._get_downstream_ineligible_ops = MagicMock(return_value=[])
    # Terminal-operator support: only the DAG root read by an external consumer
    # is eligible. Default to the non-terminal case; tests override both.
    rm._output_operator = None
    rm.has_external_consumer = False
    topology = MagicMock()
    # Downstream has queued input, i.e. it has not run dry, and nobody waits.
    topology.__getitem__ = lambda _self, _op: MagicMock(
        total_enqueued_input_blocks=lambda: 10, num_waiting_consumers=0
    )
    # `downstream_starved` reads the topology through the resource manager.
    rm._topology = topology
    policy = ActorPoolDrainPolicy(ctx, topology, rm)
    policy._cap_decay = cap_decay
    policy._cap_grow = cap_grow
    return policy


def _make_op(
    *,
    in_output_backpressure=True,
    guard_unblocked=False,
    current_size=2,
    min_size=1,
    max_size=1000,
    tasks_in_flight=2,
    has_output_deps=True,
    has_actor_pools=True,
):
    pool = MagicMock(
        current_size=MagicMock(return_value=current_size),
        min_size=MagicMock(return_value=min_size),
        max_size=MagicMock(return_value=max_size),
        num_tasks_in_flight=MagicMock(return_value=tasks_in_flight),
    )
    # Model the adaptive ceiling of `AutoscalingActorPool`: `None` means
    # "no cap", and `effective_max_size` is the cap clamped into [min, max].
    pool._dynamic_max_size = None
    pool.dynamic_max_size = MagicMock(side_effect=lambda: pool._dynamic_max_size)

    def _effective_max_size():
        cap = pool._dynamic_max_size
        if cap is None:
            return pool.max_size()
        return max(pool.min_size(), min(pool.max_size(), cap))

    def _set_dynamic_max_size(cap):
        if cap is None:
            pool._dynamic_max_size = None
        else:
            pool._dynamic_max_size = max(
                pool.min_size(), min(pool.max_size(), int(cap))
            )

    pool.effective_max_size = MagicMock(side_effect=_effective_max_size)
    pool.set_dynamic_max_size = MagicMock(side_effect=_set_dynamic_max_size)
    downstream = MagicMock()
    downstream.num_active_tasks = MagicMock(return_value=2)
    downstream.has_execution_finished = MagicMock(return_value=False)
    downstream.throttling_disabled = MagicMock(return_value=False)
    # A plain `MagicMock()` would auto-create this attribute as a truthy mock,
    # which reads as "draining" and hides the downstream from `downstream_starved`.
    downstream.actor_pool_drain_in_progress = False
    downstream.output_dependencies = []

    op = MagicMock()
    op.get_autoscaling_actor_pools = MagicMock(
        return_value=[pool] if has_actor_pools else []
    )
    op.in_task_output_backpressure = in_output_backpressure
    op.task_output_backpressure_guard_unblocked = guard_unblocked
    op.output_dependencies = [downstream] if has_output_deps else []
    op.has_execution_finished = MagicMock(return_value=False)
    op.throttling_disabled = MagicMock(return_value=False)
    op.num_active_tasks = MagicMock(return_value=3)
    op.notify_actor_pool_drain = MagicMock()
    return op


def _fill_window(policy, op, stalled_ratio, *, samples=100):
    """Prefill a fully covered window in which ``stalled_ratio`` of it is stalled.

    Stalled samples come first: every gap is weighted by the state in effect at
    its start, so the share of the window is exactly ``stalled_ratio``.
    """
    now = time.monotonic()
    span = policy._window_s
    n_stalled = round(stalled_ratio * samples)
    policy._watching_since[op] = now - span * 2
    policy._samples[op] = deque(
        (now - span + (i + 1) * span / (samples + 1), i < n_stalled)
        for i in range(samples)
    )


def _set_downstream_stalled(policy, op, stalled=True):
    """Make the downstream look stalled: no running tasks and nothing queued."""
    policy._topology.__getitem__ = lambda _self, _op: MagicMock(
        total_enqueued_input_blocks=lambda: 0 if stalled else 10,
        num_waiting_consumers=0,
    )
    for d in op.output_dependencies:
        d.num_active_tasks = MagicMock(return_value=0 if stalled else 2)


def _set_consumer_starving(policy, op, starving=True):
    """Make an external consumer look blocked waiting for this op's output."""
    policy._topology.__getitem__ = lambda _self, _op: MagicMock(
        total_enqueued_input_blocks=lambda: 10,
        num_waiting_consumers=1 if starving else 0,
    )


def _set_tasks_in_flight(op, num_tasks):
    for pool in op.get_autoscaling_actor_pools():
        pool.num_tasks_in_flight = MagicMock(return_value=num_tasks)


def _set_pool_size(op, size):
    for pool in op.get_autoscaling_actor_pools():
        pool.current_size = MagicMock(return_value=size)


def test_inert_when_disabled():
    """Enabling this must be an explicit decision, not a silent change."""
    current = DataContext.get_current()
    assert current.actor_pool_drain_window_s is None
    assert current.actor_pool_drain_stall_ratio == DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO
    assert (
        current.actor_pool_drain_resume_ratio == DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO
    )
    assert DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO < DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO

    policy = _make_policy(window_s=None)
    op = _make_op()
    assert policy.can_add_input(op) is True
    # ... and it does not even keep per-op state.
    assert not policy._samples


def test_rejects_non_positive_window():
    for bad in (0, -1):
        with pytest.raises(ValueError, match="positive"):
            _make_policy(window_s=bad)


def test_rejects_ratios_without_a_hysteresis_band():
    """Equal or inverted thresholds would make the policy flap on every sample."""
    for kwargs in (
        dict(stall_ratio=0.2, resume_ratio=0.2),
        dict(stall_ratio=0.2, resume_ratio=0.5),
        dict(stall_ratio=1.2, resume_ratio=0.2),
        dict(stall_ratio=0.8, resume_ratio=-0.1),
    ):
        with pytest.raises(ValueError, match="resume_ratio < stall_ratio"):
            _make_policy(**kwargs)

    # Only validated while the policy is enabled: a stray setting must not break
    # every pipeline while the feature is off.
    policy = _make_policy(window_s=None, stall_ratio=0.1, resume_ratio=0.9)
    assert policy.can_add_input(_make_op()) is True


def test_does_not_engage_before_the_window_is_covered():
    """One early sample would otherwise read as a fully stalled window."""
    policy = _make_policy()
    op = _make_op()
    for _ in range(5):
        assert policy.can_add_input(op) is True
    assert policy._stalled_fraction(op) is None
    assert op not in policy._draining


def test_engages_when_stalled_for_the_window():
    # A hair above the ratio: the share is summed from float time deltas, so
    # asserting on the exact boundary would be asserting on rounding.
    for ratio in (1.0, DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO + 0.05):
        policy = _make_policy()
        op = _make_op()
        _fill_window(policy, op, ratio)
        assert policy.can_add_input(op) is False
        assert op in policy._draining
        assert op.notify_actor_pool_drain.call_args_list[-1] == call(True)


def test_does_not_engage_when_the_output_keeps_flowing():
    """Backpressure alone is not enough: the op has to be unable to progress."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO - 0.2)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining

    # ... and the share is time weighted, not a sample count: this is called once
    # per scheduling iteration, so dispatch bursts produce far more samples than
    # the blocked stretches they alternate with. Only ~5% of this window was
    # stalled, even though most of its samples are.
    policy = _make_policy()
    op = _make_op()
    now = time.monotonic()
    policy._watching_since[op] = now - 2 * policy._window_s
    policy._samples[op] = deque(
        [
            (now - policy._window_s + 0.5, False),  # most of the window, un-stalled
            (now - 0.05, True),  # a burst of samples covering 50ms of it
            (now - 0.04, True),
        ]
    )
    fraction = policy._stalled_fraction(op)
    assert fraction is not None and fraction < 0.1
    assert policy.can_add_input(op) is True


def test_stalled_fraction_measures_the_whole_window(monkeypatch):
    """A short recent stall must not look like a fully stalled window.

    The sample at/before the cutoff carries the state in effect when the window
    starts. Dropping it would leave only the recent samples and make their short
    interval the denominator -- here a 1s stall would read as 100% instead of 10%.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy()
    op = _make_op(in_output_backpressure=False)
    policy._watching_since[op] = clock[0] - 2 * policy._window_s
    policy._samples[op] = deque(
        [
            (clock[0] - 100, False),  # readable before the window
            (clock[0] - 1, True),  # the window's only stalled second
        ]
    )

    fraction = policy._stalled_fraction(op)
    assert fraction == pytest.approx(0.1, abs=0.02)


def test_sampling_is_bounded_by_the_window():
    """A hot scheduling loop must not grow the window without bound."""
    policy = _make_policy()
    op = _make_op()
    for _ in range(5000):
        policy.can_add_input(op)
    assert len(policy._samples[op]) <= SAMPLES_PER_WINDOW // 4

    # Samples older than the window are evicted.
    policy._samples[op] = deque(
        (t - 10 * policy._window_s, stalled) for t, stalled in policy._samples[op]
    )
    policy._stalled_fraction(op)
    assert len(policy._samples[op]) <= 2


def test_skips_operators_it_cannot_reclaim():
    """Each guard means either no pool to hand back or a pipeline to starve."""
    cases = {
        "no actor pools": dict(has_actor_pools=False),
        "no output dependencies": dict(has_output_deps=False),
        "at floor": dict(current_size=1, min_size=1),
        "nothing in flight": dict(tasks_in_flight=0),
    }
    for name, kwargs in cases.items():
        policy = _make_policy()
        op = _make_op(**kwargs)
        _fill_window(policy, op, 1.0)
        assert policy.can_add_input(op) is True, name
        assert op not in policy._draining, name

    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    _set_downstream_stalled(policy, op)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining


def test_terminal_pool_with_external_consumer_drains():
    """A DAG root read by a slow external consumer is a valid drain candidate."""
    policy = _make_policy()
    op = _make_op(has_output_deps=False)
    policy._resource_manager._output_operator = op
    policy._resource_manager.has_external_consumer = True
    _fill_window(policy, op, 1.0)

    assert policy.can_add_input(op) is False
    assert op in policy._draining


def test_terminal_pool_without_external_consumer_is_skipped():
    """No downstream and no external consumer: there is nothing to feed."""
    policy = _make_policy()
    op = _make_op(has_output_deps=False)
    policy._resource_manager._output_operator = op
    policy._resource_manager.has_external_consumer = False
    _fill_window(policy, op, 1.0)

    assert policy.can_add_input(op) is True
    assert op not in policy._draining


def test_terminal_pool_does_not_drain_a_waiting_consumer():
    """The consumer safety valve must also work for the DAG root."""
    policy = _make_policy()
    op = _make_op(has_output_deps=False)
    policy._resource_manager._output_operator = op
    policy._resource_manager.has_external_consumer = True
    _set_consumer_starving(policy, op, True)
    _fill_window(policy, op, 1.0)

    assert policy.can_add_input(op) is True
    assert op not in policy._draining


def test_materializing_downstream_clears_an_existing_drain():
    """``_is_blocking_materializing_op`` starts returning True once the
    downstream operator that made this one a candidate finishes. That return
    comes before the floor release, so a flagged drain has to be cleared there --
    otherwise the autoscaler would keep force-shrinking an operator that is
    being fed again."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    policy._resource_manager._is_blocking_materializing_op = MagicMock(
        return_value=True
    )
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_does_not_drain_a_starving_consumer():
    """The guard unblocks a sink precisely so a waiting consumer gets data, so
    that state must not be read as nobody consuming."""
    policy = _make_policy()
    op = _make_op()
    _set_consumer_starving(policy, op)
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining

    # ... and it releases a drain that is already in progress.
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False
    _set_consumer_starving(policy, op)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_holds_until_relief_is_sustained():
    """One readable sample is not relief: the parked tasks become *able* to
    complete exactly when the output starts being read, so releasing there would
    refill the pool at the worst moment. Relief has to show up as a share of the
    window."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    op.in_task_output_backpressure = False
    op.task_output_backpressure_guard_unblocked = False
    assert policy.can_add_input(op) is False
    assert op in policy._draining

    # ... and a window's worth of relief crosses the resume threshold.
    _fill_window(policy, op, 0.0)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_does_not_release_inside_the_hysteresis_band():
    """Between the two thresholds the state is kept: that band is what absorbs
    the flicker of the raw backpressure signal."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    band = (
        DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO + DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO
    ) / 2
    _fill_window(policy, op, band)
    assert policy.can_add_input(op) is False
    assert op in policy._draining


def test_window_keeps_rolling_while_draining():
    """Both ends of the hysteresis are decided on the same rolling window, so
    samples must keep being recorded while the operator is withheld. Frozen, the
    window would keep reporting the state that engaged the drain and relief could
    never be observed."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    # Sample on every call, so one iteration is enough to see the new state.
    policy._sample_interval_s = 0.0
    op.in_task_output_backpressure = False
    op.task_output_backpressure_guard_unblocked = False
    assert policy.can_add_input(op) is False
    assert policy._samples[op][-1][1] is False


def test_release_needs_a_fresh_window_before_re_engaging():
    """A release drops the window: the operator gets a grace period and the next
    drain has to earn its own evidence."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    _fill_window(policy, op, 0.0)
    assert policy.can_add_input(op) is True

    # Stalled again right away: still fed, because the window was dropped.
    assert policy.can_add_input(op) is True
    assert op not in policy._draining

    # ... and a fresh, fully stalled window engages again.
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False
    assert op in policy._draining


def test_holds_a_drained_pool_until_it_is_reclaimed(monkeypatch):
    """The share measures whether the output is being read, so an emptied-out pool
    keeps being withheld -- its idle actors are exactly what is left to reclaim.
    The exit is the pool reaching the floor or the output recovering, not a timer.

    Real time has to advance here: with a frozen clock no sample is ever taken and
    the window could not move either way.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    # Every parked task completes, so nothing is in flight -- but the output is
    # still not being read.
    _set_tasks_in_flight(op, 0)
    for step in range(1, 80):  # two windows' worth of scheduling iterations
        clock[0] = 1000.0 + step * policy._sample_interval_s
        assert policy.can_add_input(op) is False, step
    assert op in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(True)

    # The autoscaler hands the now-idle actors back; at the floor there is
    # nothing left to reclaim.
    _set_pool_size(op, 1)
    clock[0] += policy._sample_interval_s
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_holds_across_the_whole_walk_down_to_the_floor(monkeypatch):
    """The autoscaler reclaims one actor per scheduling pass
    (``_compute_downscale_delta`` returns 1), so a pool above the floor takes
    several passes to empty. The drain has to stay engaged for every one of them
    and release only when the last actor lands: releasing early would feed the
    operator again while there are still actors left to reclaim.

    This pins the *policy state machine* across the walk. The pool sizes here are
    scripted, so it says nothing about actors actually being released -- that is
    asserted end to end in
    ``test_backpressure_e2e.py::test_actor_pool_drain_engages``.

    Real time has to advance, for the same reason as
    ``test_holds_a_drained_pool_until_it_is_reclaimed``.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy()
    op = _make_op(current_size=4, min_size=1)
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    # One actor back per pass: 4 -> 3 -> 2, still above the floor.
    for size in (3, 2):
        _set_pool_size(op, size)
        clock[0] += policy._sample_interval_s
        assert policy.can_add_input(op) is False, size
        assert op in policy._draining, size

    # The last actor lands: nothing left to reclaim, so the drain ends.
    _set_pool_size(op, 1)
    clock[0] += policy._sample_interval_s
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_release_reasons_are_logged(caplog):
    """The reason is how the exit paths are told apart in production logs, so
    each one has to be greppable."""
    caplog.set_level(logging.INFO)

    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    # Nothing left to reclaim.
    _set_pool_size(op, 1)
    assert policy.can_add_input(op) is True
    assert "reason=pool_at_min" in caplog.text

    # Output is being read again.
    caplog.clear()
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False
    _fill_window(policy, op, 0.0)
    assert policy.can_add_input(op) is True
    assert "reason=backpressure_relieved" in caplog.text

    # Safety valve: the downstream ran dry.
    caplog.clear()
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False
    _set_downstream_stalled(policy, op)
    assert policy.can_add_input(op) is True
    assert "reason=downstream_dry" in caplog.text


def test_releases_at_floor():
    """Nothing left to reclaim, so stop withholding."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    _set_pool_size(op, 1)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_releases_when_downstream_stalls():
    """Withholding must never be the thing that stalls the pipeline."""
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    _set_downstream_stalled(policy, op)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining


def test_guard_unblocked_counts_as_stalled():
    """The guard let only 1 byte through; the op is still stalled."""
    policy = _make_policy()
    op = _make_op(in_output_backpressure=False, guard_unblocked=True)
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False


def test_state_dropped_when_operator_finishes():
    policy = _make_policy()
    op = _make_op()
    _fill_window(policy, op, 1.0)
    assert policy.can_add_input(op) is False

    op.has_execution_finished = MagicMock(return_value=True)
    assert policy.can_add_input(op) is True
    assert op not in policy._draining
    assert op not in policy._samples
    assert op not in policy._watching_since
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)


def test_name_and_registration():
    assert _make_policy().name == "ActorPoolDrain"
    # `get_eligible_operators` short-circuits on the first policy returning
    # False, and this one has to see every operator on every iteration to keep
    # its stall window rolling.
    assert ENABLED_BACKPRESSURE_POLICIES[0] is ActorPoolDrainPolicy


def _make_context_returning(policies):
    """A context whose enabled-policy config is ``policies``."""
    ctx = MagicMock()
    ctx.actor_pool_drain_window_s = 10.0
    ctx.actor_pool_drain_stall_ratio = DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO
    ctx.actor_pool_drain_resume_ratio = DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO
    ctx.get_config = MagicMock(return_value=policies)
    return ctx


def test_a_custom_policy_order_still_consults_the_drain_policy_first():
    """The default list is not the only way in: the enabled policies can be
    overridden through `DataContext.set_config`. A custom list may name the drain
    policy anywhere, but it has to run first regardless -- a policy ahead of it
    that returns False stops the chain, which would freeze the stall window and
    skip the drain's own release conditions, leaving a stale drain flag that the
    actor autoscaler keeps acting on."""
    ctx = _make_context_returning(
        [ResourceBudgetBackpressurePolicy, ActorPoolDrainPolicy]
    )

    policies = get_backpressure_policies(ctx, MagicMock(), MagicMock())

    assert [type(p) for p in policies] == [
        ActorPoolDrainPolicy,
        ResourceBudgetBackpressurePolicy,
    ]


def test_a_custom_policy_list_without_the_drain_policy_is_untouched():
    """Reordering must not invent policies that were not asked for."""
    ctx = _make_context_returning([ResourceBudgetBackpressurePolicy])

    policies = get_backpressure_policies(ctx, MagicMock(), MagicMock())

    assert [type(p) for p in policies] == [ResourceBudgetBackpressurePolicy]


def test_the_default_policy_order_is_already_drain_first():
    ctx = _make_context_returning(ENABLED_BACKPRESSURE_POLICIES)

    policies = get_backpressure_policies(ctx, MagicMock(), MagicMock())

    assert type(policies[0]) is ActorPoolDrainPolicy
    assert len(policies) == len(ENABLED_BACKPRESSURE_POLICIES)


def test_adaptive_ceiling_steps_down_while_stalled(monkeypatch):
    """A stalled window multiplies each pool's adaptive ceiling by `cap_decay`."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    _fill_window(policy, op, 1.0)

    policy.can_add_input(op)

    pool = op.get_autoscaling_actor_pools()[0]
    assert pool.dynamic_max_size() == 80
    assert pool.effective_max_size() == 80


def test_an_empty_pool_does_not_step_the_ceiling_down(monkeypatch):
    """An empty pool says nothing about how big the operator needs to be.

    `current_size()` is 0 both when every actor has been handed back and when
    none has started yet (actors still pending). Cutting the ceiling to
    `min_size` off that would pin the pool there until a full window of relief
    let it grow back.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=0, min_size=1, max_size=1000, tasks_in_flight=0)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(80)
    _fill_window(policy, op, 1.0)

    policy.can_add_input(op)

    assert pool.dynamic_max_size() == 80


def test_adaptive_ceiling_steps_at_most_once_per_window(monkeypatch):
    """The ceiling moves at most once per window, so it decays at a bounded rate."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    _fill_window(policy, op, 1.0)
    policy.can_add_input(op)

    pool = op.get_autoscaling_actor_pools()[0]
    assert pool.dynamic_max_size() == 80

    # Same window: no further step, however often the policy is consulted.
    clock[0] += policy._window_s / 2
    policy.can_add_input(op)
    policy.can_add_input(op)
    assert pool.dynamic_max_size() == 80

    # Next window: one more step (80 * 0.8 = 64).
    clock[0] += policy._window_s
    policy.can_add_input(op)
    assert pool.dynamic_max_size() == 64


def test_hysteresis_band_does_not_consume_the_ceiling_window(monkeypatch):
    """Only a real ceiling step may consume the once-per-window budget.

    A sample inside the hysteresis band leaves the ceiling unchanged. It must
    not push the next real step a full window into the future.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    pool = op.get_autoscaling_actor_pools()[0]

    band = (
        DEFAULT_ACTOR_POOL_DRAIN_RESUME_RATIO + DEFAULT_ACTOR_POOL_DRAIN_STALL_RATIO
    ) / 2
    _fill_window(policy, op, band)
    policy.can_add_input(op)
    assert pool.dynamic_max_size() is None
    assert op not in policy._cap_updated_at

    # Crossing into stall immediately must step now, not one window later.
    _fill_window(policy, op, 1.0)
    policy.can_add_input(op)
    assert pool.dynamic_max_size() == 80


def test_adaptive_ceiling_steps_up_when_output_is_read(monkeypatch):
    """A recovered window grows the ceiling additively, up to `max_size`."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8, cap_grow=0.1)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(80)

    _fill_window(policy, op, 0.0)
    policy.can_add_input(op)

    # 80 + ceil(80 * 0.1) = 88
    assert pool.dynamic_max_size() == 88


def test_zero_cap_grow_keeps_the_ceiling(monkeypatch):
    """``cap_grow=0`` must not silently grow the ceiling by one."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8, cap_grow=0.0)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(80)

    _fill_window(policy, op, 0.0)
    policy.can_add_input(op)

    assert pool.dynamic_max_size() == 80


def test_release_at_the_ceiling_keeps_the_ceiling(monkeypatch):
    """Being at the ceiling releases the drain but must not clear the ceiling."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    # cap_decay=1.0 keeps the ceiling fixed, so the pool sits exactly at it.
    policy = _make_policy(cap_decay=1.0)
    op = _make_op(current_size=50, min_size=1, max_size=1000, tasks_in_flight=5)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(50)
    _fill_window(policy, op, 1.0)

    assert policy.can_add_input(op) is True
    assert op.notify_actor_pool_drain.call_args_list[-1] == call(False)
    # The ceiling survives the release: this is what stops the pool from being
    # re-grown straight back into the stall.
    assert pool.dynamic_max_size() == 50


def test_safety_valve_clears_the_adaptive_ceiling(monkeypatch):
    """A starving downstream must be able to grow this operator again."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=1.0)
    op = _make_op(current_size=60, min_size=1, max_size=1000, tasks_in_flight=5)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(50)
    _set_downstream_stalled(policy, op, True)

    assert policy.can_add_input(op) is True
    assert pool.dynamic_max_size() is None


def test_safety_valve_clears_the_ceiling_at_the_floor(monkeypatch):
    """A pool parked at its ceiling must still grow for a starving downstream.

    At the floor the policy stops withholding, but it also keeps the adaptive
    ceiling. If the downstream is now dry, that ceiling has to be cleared before
    the floor release; otherwise the autoscaler is allowed to grow but is capped
    at the ceiling it just drained to.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=1, min_size=1, max_size=1000, tasks_in_flight=1)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(1)
    _fill_window(policy, op, 1.0)
    _set_downstream_stalled(policy, op, True)

    assert policy.can_add_input(op) is True
    assert pool.dynamic_max_size() is None


def test_the_window_is_dropped_at_the_floor_without_a_ceiling(monkeypatch):
    """Without a ceiling the drain has to re-earn its evidence after the floor.

    Keeping the window there would let evidence gathered at `min_size` -- where
    there is nothing left to reclaim -- re-engage the drain the moment the pool
    grows above the floor, i.e. a grow/shrink cycle at the floor.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=1.0)
    op = _make_op(current_size=5, min_size=5, max_size=50, tasks_in_flight=5)
    _fill_window(policy, op, 1.0)

    assert policy.can_add_input(op) is True
    assert op not in policy._samples, "window must be dropped at min_size"
    assert op not in policy._watching_since
    assert op not in policy._cap_updated_at

    # The pool grows above the floor: no immediate re-drain on floor-era evidence.
    _set_pool_size(op, 6)
    assert policy.can_add_input(op) is True


def test_the_window_is_kept_at_the_ceiling(monkeypatch):
    """With a ceiling the window keeps rolling, so the ceiling can keep stepping."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=1.0)
    op = _make_op(current_size=50, min_size=1, max_size=1000, tasks_in_flight=5)
    pool = op.get_autoscaling_actor_pools()[0]
    pool.set_dynamic_max_size(50)
    _fill_window(policy, op, 1.0)

    assert policy.can_add_input(op) is True
    assert op in policy._samples, "window must be kept while a ceiling is in place"
    assert op in policy._watching_since


def test_ceiling_is_cleared_when_the_op_is_no_longer_eligible(monkeypatch):
    """An operator that leaves the eligible set must not stay capped.

    `is_op_eligible` goes False once the operator has finished -- and its
    downstream finishing is exactly when it has to run at full speed.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    _fill_window(policy, op, 1.0)
    policy.can_add_input(op)
    pool = op.get_autoscaling_actor_pools()[0]
    assert pool.dynamic_max_size() == 80

    policy._resource_manager.is_op_eligible = lambda _op: False
    policy.can_add_input(op)

    assert pool.dynamic_max_size() is None
    assert op not in policy._cap_updated_at


def test_ceiling_is_cleared_when_the_op_is_no_longer_a_candidate(monkeypatch):
    """Same for `_is_blocking_materializing_op`, which flips once the downstream
    that made the operator a drain candidate finishes."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=100, min_size=1, max_size=1000, tasks_in_flight=10)
    _fill_window(policy, op, 1.0)
    policy.can_add_input(op)
    pool = op.get_autoscaling_actor_pools()[0]
    assert pool.dynamic_max_size() == 80

    policy._resource_manager._is_blocking_materializing_op = lambda _op: True
    policy.can_add_input(op)

    assert pool.dynamic_max_size() is None


def test_an_upstream_still_drains_while_its_downstream_is_draining(monkeypatch):
    """A draining downstream is idle *on purpose*: it must not read as starved and
    disable the upstream's own drain, or two adjacent drains suppress each other."""
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(current_size=50, min_size=1, max_size=1000, tasks_in_flight=5)
    downstream = op.output_dependencies[0]
    # The downstream ran dry because it withheld its own inputs.
    downstream.actor_pool_drain_in_progress = True
    downstream.num_active_tasks = MagicMock(return_value=0)
    policy._topology.__getitem__ = lambda _self, _op: MagicMock(
        total_enqueued_input_blocks=lambda: 0, num_waiting_consumers=0
    )
    _fill_window(policy, op, 1.0)

    assert policy._downstream_stalled(op) is False
    assert policy.can_add_input(op) is False


def test_an_unbounded_pool_steps_the_ceiling_without_choking(monkeypatch):
    """A pool with no configured `max_size` reports an unbounded ceiling (`inf`).

    Stepping it down used to format that `inf` with `%d` in the log line, which
    raised OverflowError and killed the scheduling loop. Both directions are
    exercised here.
    """
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    policy = _make_policy(cap_decay=0.8)
    op = _make_op(
        current_size=100, min_size=1, max_size=float("inf"), tasks_in_flight=10
    )
    pool = op.get_autoscaling_actor_pools()[0]

    _fill_window(policy, op, 1.0)
    policy.can_add_input(op)  # step down: must not raise
    assert pool.dynamic_max_size() == 80

    # Advance one window *and* refill it: the share is computed over a fully
    # covered window, so a bare clock jump would leave no data (`None`).
    clock[0] += policy._window_s
    _fill_window(policy, op, 0.0)
    policy.can_add_input(op)  # step up: must not raise
    assert pool.dynamic_max_size() == 88


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
