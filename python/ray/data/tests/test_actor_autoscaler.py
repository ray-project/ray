import time
from contextlib import contextmanager
from types import MethodType
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data import ExecutionResources
from ray.data._internal.actor_autoscaler import (
    ActorPoolScalingRequest,
    DefaultActorAutoscaler,
)
from ray.data._internal.actor_autoscaler.default_actor_autoscaler import (
    _get_max_scale_up,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.context import (
    AutoscalingConfig,
    DataContext,
)


def test_actor_pool_scaling():
    """Test `_actor_pool_should_scale_up` and `_actor_pool_should_scale_down`
    in `DefaultAutoscaler`"""

    resource_manager = MagicMock(
        spec=ResourceManager,
        get_budget=MagicMock(return_value=None),
        get_allocation=MagicMock(return_value=None),
    )
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=None,
        ),
        data_context=DataContext.get_current(),
    )

    # Current actor pool utilization is 0.9, which is above the threshold.
    actor_pool: _ActorPool = MagicMock(
        spec=_ActorPool,
        min_size=MagicMock(return_value=5),
        max_size=MagicMock(return_value=15),
        dynamic_max_size=MagicMock(return_value=None),
        effective_max_size=MagicMock(return_value=15),
        current_size=MagicMock(return_value=10),
        num_active_actors=MagicMock(return_value=10),
        num_running_actors=MagicMock(return_value=10),
        num_pending_actors=MagicMock(return_value=0),
        num_tasks_in_flight=MagicMock(return_value=15),
        per_actor_resource_usage=MagicMock(return_value=ExecutionResources(cpu=1)),
        max_tasks_in_flight_per_actor=MagicMock(return_value=2),
        max_actor_concurrency=MagicMock(return_value=1),
        get_pool_util=MagicMock(
            # NOTE: Unittest mocking library doesn't support proxying to actual
            #       non-mocked methods so we have emulate it by directly binding existing
            #       method of `get_pool_util` to a mocked object
            side_effect=lambda: MethodType(_ActorPool.get_pool_util, actor_pool)()
        ),
    )

    op = MagicMock(
        spec=InternalQueueOperatorMixin,
        has_completed=MagicMock(return_value=False),
        _inputs_complete=False,
        input_dependencies=[MagicMock()],
        internal_input_queue_num_blocks=MagicMock(return_value=1),
        metrics=MagicMock(average_num_inputs_per_task=1, num_inputs_received=1),
        num_output_splits=MagicMock(return_value=1),
        actor_pool_drain_in_progress=False,
    )
    op_state = OpState(
        op, inqueues=[MagicMock(__len__=MagicMock(return_value=10), num_blocks=10)]
    )
    op_state._scheduling_status = MagicMock(under_resource_limits=True)

    @contextmanager
    def patch(mock, attr, value, is_method=True):
        original = getattr(mock, attr)
        if is_method:
            value = MagicMock(return_value=value)
        setattr(mock, attr, value)
        yield
        setattr(mock, attr, original)

    def assert_autoscaling_action(
        *, delta: int, expected_reason: Optional[str], force: bool = False
    ):
        nonlocal actor_pool, op, op_state

        assert autoscaler._derive_target_scaling_config(
            actor_pool=actor_pool,
            op=op,
            op_state=op_state,
        ) == ActorPoolScalingRequest(delta=delta, force=force, reason=expected_reason)

    # Should scale up since the util above the threshold.
    assert actor_pool.get_pool_util() == 1.5
    assert_autoscaling_action(
        delta=5,
        expected_reason="utilization of 1.5 >= 1.0",
    )

    # Should scale up immediately when the actor pool has no running actors.
    with patch(actor_pool, "num_running_actors", 0):
        with patch(actor_pool, "get_pool_util", float("inf")):
            assert_autoscaling_action(
                delta=1,
                expected_reason="no running actors, scale up immediately",
            )

    # Should be no-op since the util is below the threshold.
    with patch(actor_pool, "num_tasks_in_flight", 9):
        assert actor_pool.get_pool_util() == 0.9
        assert_autoscaling_action(
            delta=0, expected_reason="utilization of 0.9 w/in limits [0.5, 1.0]"
        )

    # Should be no-op since there are pending actors (no downscaling while pending)
    with patch(actor_pool, "num_pending_actors", 1):
        with patch(actor_pool, "num_tasks_in_flight", 4):
            assert actor_pool.get_pool_util() == 0.4
            assert_autoscaling_action(
                delta=0,
                expected_reason="no downscaling while actors are pending",
            )

    # Should be no-op since we have reached the max size (ie could not scale
    # up even though utilization > threshold)
    with patch(actor_pool, "current_size", 15):
        with patch(actor_pool, "num_tasks_in_flight", 20):
            assert_autoscaling_action(
                delta=0,
                expected_reason="reached max size",
            )

    # Should be no-op since we have reached the min size (ie could not scale
    # down even though utilization < threshold)
    with patch(actor_pool, "current_size", 5):
        with patch(actor_pool, "num_tasks_in_flight", 2):
            assert_autoscaling_action(
                delta=0,
                expected_reason="reached min size",
            )

    # Should scale up since the pool is below the min size.
    with patch(actor_pool, "current_size", 4):
        assert_autoscaling_action(
            delta=1,
            expected_reason="pool below min size",
        )

    # Should scale down since if the op is completed, or
    # the op has no more inputs.
    with patch(op, "has_completed", True):
        # NOTE: We simulate actor pool dipping below min size upon
        #       completion (to verify that it will be able to scale to 0)
        with patch(actor_pool, "current_size", 5):
            assert_autoscaling_action(
                delta=-1,
                expected_reason="consumed all inputs",
                force=True,
            )

    # Should scale down only once all inputs have been already dispatched AND
    # no new inputs ar expected
    with patch(op_state.input_queues[0], "num_blocks", 0, is_method=False):
        with patch(op, "internal_input_queue_num_blocks", 0):
            with patch(op, "_inputs_complete", True, is_method=False):
                assert_autoscaling_action(
                    delta=-1,
                    force=True,
                    expected_reason="consumed all inputs",
                )

            # With no enqueued inputs but inputs not being complete still,
            # the autoscaler should still scale up based on utilization
            assert_autoscaling_action(
                delta=5,
                expected_reason="utilization of 1.5 >= 1.0",
            )

    # Should be no-op since the op doesn't have enough resources.
    with patch(
        op_state._scheduling_status,
        "under_resource_limits",
        False,
        is_method=False,
    ):
        assert_autoscaling_action(
            delta=0,
            expected_reason="operator exceeding resource quota",
        )

    # Should be a no-op since the op has enough available concurrency slots for
    # the existing inputs.
    with patch(actor_pool, "num_tasks_in_flight", 7):
        assert_autoscaling_action(
            delta=0,
            expected_reason="utilization of 0.7 w/in limits [0.5, 1.0]",
        )

    # Should scale down since the util is below the threshold.
    with patch(actor_pool, "num_tasks_in_flight", 4):
        assert actor_pool.get_pool_util() == 0.4
        assert_autoscaling_action(
            delta=-1,
            expected_reason="utilization of 0.4 <= 0.5",
        )

    # Should scale down since the pool is above the max size.
    with patch(actor_pool, "current_size", 16):
        assert_autoscaling_action(
            delta=-1,
            expected_reason="pool exceeding max size",
        )

    # Should no-op because the op has no budget.
    with patch(resource_manager, "get_budget", ExecutionResources.zero()):
        assert_autoscaling_action(
            delta=0,
            expected_reason="exceeded resource limits",
        )

    # Should no-op because the op has not received any inputs.
    with patch(op.metrics, "num_inputs_received", 0, is_method=False):
        assert_autoscaling_action(
            delta=0,
            expected_reason="no inputs received",
        )

    # --- Resource budget enforcement (downscaling) ---
    # get_allocation and get_op_usage are patched to simulate an operator that
    # has exceeded its total resource allocation. The over-budget check fires
    # before utilization logic, so even high utilization (1.5x) is overridden.

    # CPU over-budget by 2 actors: allocation=8 CPUs, usage=10 CPUs, 1 CPU/actor.
    # allocation - usage = -2 → scale down by ceil(2/1) = 2.
    with patch(resource_manager, "get_allocation", ExecutionResources(cpu=8)):
        with patch(resource_manager, "get_op_usage", ExecutionResources(cpu=10)):
            assert_autoscaling_action(
                delta=-2,
                expected_reason="actor pool exceeds resource allocation",
            )

    # Over-budget but current_size=6 (min_size+1): required=2 but can only
    # release 1 actor (max_can_release = 6 - 5 = 1).
    with patch(resource_manager, "get_allocation", ExecutionResources(cpu=8)):
        with patch(resource_manager, "get_op_usage", ExecutionResources(cpu=10)):
            with patch(actor_pool, "current_size", 6):
                assert_autoscaling_action(
                    delta=-1,
                    expected_reason="actor pool exceeds resource allocation",
                )

    # Over-budget but pool is at min_size (current=5): cannot release any actors.
    with patch(resource_manager, "get_allocation", ExecutionResources(cpu=8)):
        with patch(resource_manager, "get_op_usage", ExecutionResources(cpu=10)):
            with patch(actor_pool, "current_size", 5):
                assert_autoscaling_action(
                    delta=0,
                    expected_reason="actor pool exceeds resource allocation "
                    "but cannot scale below min size",
                )

    # GPU pool: allocation=3 GPUs, usage=6 GPUs, 1 GPU/actor.
    # allocation - usage = -3 → scale down by 3.
    with patch(actor_pool, "per_actor_resource_usage", ExecutionResources(gpu=1)):
        with patch(resource_manager, "get_allocation", ExecutionResources(gpu=3)):
            with patch(resource_manager, "get_op_usage", ExecutionResources(gpu=6)):
                assert_autoscaling_action(
                    delta=-3,
                    expected_reason="actor pool exceeds resource allocation",
                )

    # Cross-resource: GPU-only pool (per_actor.cpu=0) with negative CPU budget
    # but positive GPU budget. CPU over-budget doesn't trigger since the pool
    # doesn't consume CPU. GPU headroom = floor(5/1)=5, capped by
    # max_size(15)-current_size(10)=5.
    with patch(actor_pool, "per_actor_resource_usage", ExecutionResources(gpu=1)):
        with patch(
            resource_manager, "get_allocation", ExecutionResources(cpu=8, gpu=10)
        ):
            with patch(
                resource_manager, "get_op_usage", ExecutionResources(cpu=10, gpu=5)
            ):
                assert_autoscaling_action(
                    delta=5,
                    expected_reason="utilization of 1.5 >= 1.0",
                )

    # Memory bottleneck: allocation=4 GB, usage=5 GB, 500 MB/actor.
    # allocation - usage = -1 GB → ceil(1 GB / 500 MB) = 2 actors to remove.
    # CPU is within budget (allocation.cpu > usage.cpu), so CPU does not trigger.
    with patch(
        actor_pool,
        "per_actor_resource_usage",
        ExecutionResources(cpu=1, memory=500_000_000),
    ):
        with patch(
            resource_manager,
            "get_allocation",
            ExecutionResources(cpu=15, memory=4_000_000_000),
        ):
            with patch(
                resource_manager,
                "get_op_usage",
                ExecutionResources(cpu=10, memory=5_000_000_000),
            ):
                assert_autoscaling_action(
                    delta=-2,
                    expected_reason="actor pool exceeds resource allocation",
                )

    # A draining pool still has to respect its resource allocation: the hard
    # budget wins over the (possibly higher) drain floor.
    op.actor_pool_drain_in_progress = True
    with patch(resource_manager, "get_allocation", ExecutionResources(cpu=8)):
        with patch(resource_manager, "get_op_usage", ExecutionResources(cpu=10)):
            assert_autoscaling_action(
                delta=-2,
                expected_reason="actor pool exceeds resource allocation",
            )
    op.actor_pool_drain_in_progress = False


def test_actor_pool_scaling_reclaims_a_draining_operator(
    autoscaler_max_upscaling_delta_setup,
):
    """A draining operator's utilization is meaningless -- the tasks that keep it
    high are parked on the output budget, not doing work -- so its pool is shrunk
    without waiting for utilization to drop below the downscaling threshold."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
        data_context=DataContext.get_current(),
    )
    op.actor_pool_drain_in_progress = True
    assert actor_pool.get_pool_util() == 2.0  # would otherwise scale up

    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    # Forced: a drain already established that more actors cannot help.
    assert request == ActorPoolScalingRequest(
        delta=-1, force=True, reason="draining a stalled actor pool"
    )

    # Nothing to reclaim at min size.
    actor_pool.current_size = MagicMock(return_value=5)
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request == ActorPoolScalingRequest.no_op(reason="reached min size")

    # And without a drain the utilization-based decision is unchanged.
    op.actor_pool_drain_in_progress = False
    actor_pool.current_size = MagicMock(return_value=10)
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request.delta > 0


def test_draining_operator_is_not_preempted_by_consumed_inputs(
    autoscaler_max_upscaling_delta_setup,
):
    """A drain still has to be honored once all inputs have been dispatched.

    By the time the stall window fills, an operator's inputs are commonly
    already dispatched and its input queue empty. The end-of-input shortcut used
    to preempt the drain branch in exactly that state, so the drain reason never
    reached the pool and no actor was reclaimed for it.
    """
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
        data_context=DataContext.get_current(),
    )
    op.actor_pool_drain_in_progress = True
    op._inputs_complete = True
    op_state.total_enqueued_input_blocks = MagicMock(return_value=0)
    actor_pool.current_size = MagicMock(return_value=10)

    request = autoscaler._derive_target_scaling_config(actor_pool, op, op_state)
    assert request == ActorPoolScalingRequest(
        delta=-1, force=True, reason="draining a stalled actor pool"
    )

    # Once the drain releases, the end-of-input shortcut takes over again.
    op.actor_pool_drain_in_progress = False
    request = autoscaler._derive_target_scaling_config(actor_pool, op, op_state)
    assert request == ActorPoolScalingRequest(
        delta=-1, force=True, reason="consumed all inputs"
    )


def test_draining_operator_walks_the_pool_down_one_actor_per_pass(
    autoscaler_max_upscaling_delta_setup,
):
    """``_compute_downscale_delta`` returns 1, so a drain reclaims a single actor
    per scheduling pass and re-issues the forced request on every pass rather than
    giving up: a pool at 4 with min_size 1 takes three passes to reach the floor.

    Worth pinning, because the walk is what paces the release: the pool steps down
    one actor at a time and re-decides on each pass, instead of dropping straight to
    the floor in one go. Note what keeps a *busy* actor from being removed is not
    the step size -- it is the in-flight check in
    ``_ActorPool._try_remove_idle_actor``, which holds for any delta.
    """
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
        data_context=DataContext.get_current(),
    )
    op.actor_pool_drain_in_progress = True
    actor_pool.min_size = MagicMock(return_value=1)

    # Apply each request the way the executor would, so the walk is simulated
    # rather than hand-written. Bounded on purpose: a pool of 4 with min_size 1 can
    # only take three steps, so needing a fourth means the floor is not being
    # respected -- fail there rather than spin.
    size = 4
    walked = []
    for _ in range(4):
        actor_pool.current_size = MagicMock(return_value=size)
        request = autoscaler._derive_target_scaling_config(
            actor_pool=actor_pool,
            op=op,
            op_state=op_state,
        )
        if request.delta == 0:
            break
        assert request.force is True
        assert request.reason == "draining a stalled actor pool"
        assert request.delta < 0, "a drain must never scale up"
        size += request.delta
        walked.append(size)

    assert walked == [3, 2, 1]
    assert size == 1, "the drain walked below min_size"
    assert request == ActorPoolScalingRequest.no_op(reason="reached min size")


@pytest.fixture
def autoscaler_max_upscaling_delta_setup():
    resource_manager = MagicMock(
        spec=ResourceManager,
        get_budget=MagicMock(return_value=None),
        get_allocation=MagicMock(return_value=None),
    )

    actor_pool = MagicMock(
        spec=_ActorPool,
        min_size=MagicMock(return_value=5),
        max_size=MagicMock(return_value=20),
        dynamic_max_size=MagicMock(return_value=None),
        effective_max_size=MagicMock(return_value=20),
        current_size=MagicMock(return_value=10),
        get_current_size=MagicMock(return_value=10),
        num_pending_actors=MagicMock(return_value=0),
        num_tasks_in_flight=MagicMock(return_value=40),
        max_tasks_in_flight_per_actor=MagicMock(return_value=4),
        get_pool_util=MagicMock(return_value=2.0),
    )

    op = MagicMock(
        spec=InternalQueueOperatorMixin,
        has_completed=MagicMock(return_value=False),
        _inputs_complete=False,
        metrics=MagicMock(average_num_inputs_per_task=1, num_inputs_received=1),
        actor_pool_drain_in_progress=False,
    )
    op_state = MagicMock(
        spec=OpState,
        total_enqueued_input_blocks=MagicMock(return_value=1),
    )
    op_state.op = op
    op_state._scheduling_status = MagicMock(under_resource_limits=True)
    return resource_manager, actor_pool, op, op_state


def test_actor_pool_scaling_respects_small_max_upscaling_delta(
    autoscaler_max_upscaling_delta_setup,
):
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=3,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    # With current_size=10, util=2.0, threshold=1.0:
    # plan_delta = ceil(10 * (2.0/1.0 - 1)) = ceil(10) = 10
    # However, delta is limited by max_upscaling_delta=3, so delta = min(10, 3) = 3
    assert request.delta == 3


def test_actor_pool_scaling_respects_large_max_upscaling_delta(
    autoscaler_max_upscaling_delta_setup,
):
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=100,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    # With current_size=10, util=2.0, threshold=1.0:
    # plan_delta = ceil(10 * (2.0/1.0 - 1)) = ceil(10) = 10
    # max_upscaling_delta=100 is large enough, but delta is limited by max_size:
    # max_size(20) - current_size(10) = 10, so delta = min(10, 100, 10) = 10
    assert request.delta == 10


def test_actor_pool_scaling_respects_the_adaptive_ceiling(
    autoscaler_max_upscaling_delta_setup,
):
    """The drain policy's ceiling bounds upscaling, below the configured max."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    # Drain policy stepped the pool's ceiling down to 12: even though the delta
    # plan is 10 and max_size is 20, only (12 - 10) = 2 actors may be added.
    actor_pool.effective_max_size = MagicMock(return_value=12)
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=100,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request.delta == 2


def test_actor_pool_scaling_stops_at_the_adaptive_ceiling(
    autoscaler_max_upscaling_delta_setup,
):
    """A pool already at its ceiling is not scaled up at all."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    actor_pool.effective_max_size = MagicMock(return_value=10)
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=100,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request.delta == 0
    assert "dynamic max size" in request.reason


def test_draining_operator_stops_at_the_adaptive_ceiling(
    autoscaler_max_upscaling_delta_setup,
):
    """A drained pool is walked down to its ceiling, not to `min_size`."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    op.actor_pool_drain_in_progress = True
    # Ceiling equals the current size: nothing left to reclaim.
    actor_pool.dynamic_max_size = MagicMock(return_value=10)
    actor_pool.effective_max_size = MagicMock(return_value=10)
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request.delta == 0
    assert request.reason == "reached drain target"


def test_draining_operator_walks_down_to_the_adaptive_ceiling(
    autoscaler_max_upscaling_delta_setup,
):
    """While draining, the walk-down stops at the ceiling instead of `min_size`."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    op.actor_pool_drain_in_progress = True
    actor_pool.dynamic_max_size = MagicMock(return_value=6)
    actor_pool.effective_max_size = MagicMock(return_value=6)
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    # One actor per pass, as before -- just towards the ceiling (6) not min_size (5).
    assert request.delta == -1
    assert "draining a stalled actor pool" in request.reason


def test_adaptive_ceiling_prevents_regrowth_after_drain_release(
    autoscaler_max_upscaling_delta_setup,
):
    """A pool parked at its adaptive ceiling must not bounce back up.

    The drain policy clears ``actor_pool_drain_in_progress`` once the pool reaches
    its adaptive floor. The ceiling must still block the utilization-based upscale
    that would otherwise immediately undo the drain and start a shrink/grow cycle.
    """
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=100,
        ),
        data_context=DataContext.get_current(),
    )

    size = 10
    ceiling = 6
    actor_pool.current_size = MagicMock(side_effect=lambda: size)
    actor_pool.dynamic_max_size = MagicMock(return_value=ceiling)
    actor_pool.effective_max_size = MagicMock(return_value=ceiling)
    op.actor_pool_drain_in_progress = True

    # Walk down to the adaptive floor exactly as repeated scheduling passes do.
    while size > ceiling:
        request = autoscaler._derive_target_scaling_config(actor_pool, op, op_state)
        assert request.delta == -1
        assert request.force
        size += request.delta

    # The policy releases its drain flag at the floor. Utilization remains high,
    # but the surviving ceiling must prevent an immediate regrowth.
    op.actor_pool_drain_in_progress = False
    for _ in range(3):
        request = autoscaler._derive_target_scaling_config(actor_pool, op, op_state)
        assert request == ActorPoolScalingRequest.no_op(
            reason="reached dynamic max size (drain cap)"
        )
        assert size == ceiling


def test_upscale_gate_prevents_regrowth_without_an_adaptive_ceiling(
    autoscaler_max_upscaling_delta_setup,
):
    """With AIMD disabled, blocked output still must not cause grow/shrink cycles."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    resource_manager.downstream_can_absorb = MagicMock(return_value=False)
    ctx = DataContext.get_current().copy()
    ctx.actor_pool_drain_window_s = 5.0
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=100,
        ),
        data_context=ctx,
    )

    # Once the pool reaches min_size and releases its drain flag, high utilization
    # alone must not grow the still-blocked pool again.
    actor_pool.current_size = MagicMock(return_value=actor_pool.min_size())
    op.actor_pool_drain_in_progress = False
    for _ in range(3):
        request = autoscaler._derive_target_scaling_config(actor_pool, op, op_state)
        assert request == ActorPoolScalingRequest.no_op(
            reason="downstream cannot absorb more output"
        )


def test_upscale_is_refused_when_the_downstream_cannot_absorb(
    autoscaler_max_upscaling_delta_setup,
):
    """A pool whose output is parked must not grow: the extra actors would produce
    blocks nobody reads, which is what makes drain/re-grow cycles oscillate."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    resource_manager.downstream_can_absorb = MagicMock(return_value=False)
    ctx = DataContext.get_current().copy()
    ctx.actor_pool_drain_window_s = 5.0
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=100,
        ),
        data_context=ctx,
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request.delta == 0
    assert "downstream cannot absorb" in request.reason


def test_a_pool_with_no_actors_is_never_gated(autoscaler_max_upscaling_delta_setup):
    """The operator must be able to (re)start one actor even while its output is
    parked, otherwise it could never make progress again."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    resource_manager.downstream_can_absorb = MagicMock(return_value=False)
    actor_pool.get_pool_util = MagicMock(return_value=float("inf"))
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
        data_context=DataContext.get_current(),
    )
    request = autoscaler._derive_target_scaling_config(
        actor_pool=actor_pool,
        op=op,
        op_state=op_state,
    )
    assert request.delta == 1
    assert request.reason == "no running actors, scale up immediately"


def test_upscale_gate_follows_the_drain_feature_by_default(
    autoscaler_max_upscaling_delta_setup,
):
    """`None` (the default) means "auto": the gate belongs to the drain feature
    family, so a pipeline that never enabled draining keeps its old behaviour."""
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    resource_manager.downstream_can_absorb = MagicMock(return_value=False)
    config = AutoscalingConfig(
        actor_pool_util_upscaling_threshold=1.0,
        actor_pool_util_downscaling_threshold=0.5,
        actor_pool_max_upscaling_delta=100,
    )

    def _gate_is_on(window_s):
        # A snapshot copy, exactly like the read APIs hand to the executor: the
        # executor's context is *not* the live ambient one, and the gate has to
        # follow the context the drain policy was built from.
        ctx = DataContext.get_current().copy()
        ctx.actor_pool_drain_window_s = window_s
        autoscaler = DefaultActorAutoscaler(
            topology=MagicMock(),
            resource_manager=resource_manager,
            config=config,
            data_context=ctx,
        )
        request = autoscaler._derive_target_scaling_config(
            actor_pool=actor_pool,
            op=op,
            op_state=op_state,
        )
        return request

    # Drain feature off in the snapshot: growth is not gated.
    request = _gate_is_on(None)
    assert request.delta == 10

    # Drain feature on in the snapshot: the gate applies.
    request = _gate_is_on(5.0)
    assert request.delta == 0
    assert "downstream cannot absorb" in request.reason


def test_upscale_gate_ignores_the_ambient_context(
    autoscaler_max_upscaling_delta_setup,
):
    """The gate must follow the context the executor was built with, not the live
    ambient one.

    The read APIs snapshot the context (`DataContext.get_current().copy()`) when a
    Dataset is created, and the drain policy is built from that snapshot. Reading
    the ambient context instead would let the gate be on for a pipeline whose
    drain policy is inert -- and vice versa -- whenever the two diverge, which is
    exactly what happens when the feature is toggled between creating a Dataset
    and executing it.
    """
    resource_manager, actor_pool, op, op_state = autoscaler_max_upscaling_delta_setup
    resource_manager.downstream_can_absorb = MagicMock(return_value=False)

    # The executor's snapshot has draining off...
    snapshot = DataContext.get_current().copy()
    snapshot.actor_pool_drain_window_s = None
    # ...while the live ambient context has it on.
    original_window = DataContext.get_current().actor_pool_drain_window_s
    DataContext.get_current().actor_pool_drain_window_s = 5.0
    try:
        autoscaler = DefaultActorAutoscaler(
            topology=MagicMock(),
            resource_manager=resource_manager,
            config=AutoscalingConfig(
                actor_pool_util_upscaling_threshold=1.0,
                actor_pool_util_downscaling_threshold=0.5,
                actor_pool_max_upscaling_delta=100,
            ),
            data_context=snapshot,
        )
        request = autoscaler._derive_target_scaling_config(
            actor_pool=actor_pool,
            op=op,
            op_state=op_state,
        )
        # The gate follows the snapshot, so growth is untouched.
        assert request.delta == 10
    finally:
        DataContext.get_current().actor_pool_drain_window_s = original_window


class BarrierWaiter:
    def __init__(self, barrier):
        self._barrier = barrier

    def __call__(self, x):
        ray.get(self._barrier.wait.remote(), timeout=10)
        return x


@ray.remote(max_concurrency=10)
class Barrier:
    def __init__(self, n, delay=0):
        self.n = n
        self.delay = delay
        self.max_waiters = 0
        self.cur_waiters = 0

    def wait(self):
        self.cur_waiters += 1
        if self.cur_waiters > self.max_waiters:
            self.max_waiters = self.cur_waiters
        self.n -= 1
        print("wait", self.n)
        while self.n > 0:
            time.sleep(0.1)
        time.sleep(self.delay)
        print("wait done")
        self.cur_waiters -= 1

    def get_max_waiters(self):
        return self.max_waiters


def test_actor_pool_scales_up(ray_start_10_cpus_shared, restore_data_context):
    # The Ray cluster started by the fixture might not have much object store memory.
    # To prevent the actor pool from getting backpressured, we decrease the max block
    # size.
    ctx = ray.data.DataContext.get_current()
    ctx.target_max_block_size = 1 * 1024**2

    # The `BarrierWaiter` UDF blocks until there are 2 actors running. If we don't
    # scale up, the UDF raises a timeout.
    barrier = Barrier.remote(2)
    # We produce 3 blocks (1 elem each) such that
    #   - We start wiht actor pool of min_size
    #   - 2 tasks could be submitted to an actor (utilization reaches 200%)
    #   - Autoscaler kicks in and creates another actor
    #   - 3 task is submitted to a new actor (unblocking the barrier)
    ray.data.range(3, override_num_blocks=3).map(
        BarrierWaiter,
        fn_constructor_args=(barrier,),
        compute=ray.data.ActorPoolStrategy(
            min_size=1, max_size=2, max_tasks_in_flight_per_actor=2
        ),
    ).take_all()


def test_actor_pool_respects_max_size(ray_start_10_cpus_shared, restore_data_context):
    # The Ray cluster started by the fixture might not have much object store memory.
    # To prevent the actor pool from getting backpressured, we decrease the max block
    # size.
    ctx = ray.data.DataContext.get_current()
    ctx.target_max_block_size = 1 * 1024**2

    # The `BarrierWaiter` UDF blocks until there are 3 actors running. Since the max
    # pool size is 2, the UDF should eventually timeout.
    barrier = Barrier.remote(3)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.data.range(2, override_num_blocks=2).map(
            BarrierWaiter,
            fn_constructor_args=(barrier,),
            compute=ray.data.ActorPoolStrategy(min_size=1, max_size=2),
        ).take_all()


def test_autoscaling_config_validation_warnings(
    ray_start_10_cpus_shared, restore_data_context
):
    """Test that validation warnings are emitted when actor pool config won't allow scaling up."""

    class SimpleMapper:
        """Simple callable class for testing autoscaling validation."""

        def __call__(self, row):
            # Map operates on rows which are dicts
            return {"value": row["id"] * 2}

    # Test #1: Invalid config (should warn)
    #   - max_tasks_in_flight / max_concurrency == 1
    #   - Default upscaling threshold (200%)
    with patch(
        "ray.data._internal.actor_autoscaler.default_actor_autoscaler.logger.warning"
    ) as mock_warning:
        ds = ray.data.range(2, override_num_blocks=2).map_batches(
            SimpleMapper,
            compute=ray.data.ActorPoolStrategy(
                max_tasks_in_flight_per_actor=1,
            ),
            max_concurrency=1,
        )
        # Take just one item to minimize execution time
        ds.take_all()

    # Check that warning was called with expected message
    warn_log_args_str = str(mock_warning.call_args_list)
    expected_message = (
        "⚠️  Actor Pool configuration of the "
        "ActorPoolMapOperator[MapBatches(SimpleMapper)] will not allow it to scale up: "
        "configured utilization threshold (175.0%) couldn't be reached with "
        "configured max_concurrency=1 and max_tasks_in_flight_per_actor=1 "
        "(max utilization will be max_tasks_in_flight_per_actor / max_concurrency = 100%)"
    )

    assert expected_message in warn_log_args_str

    # Test #2: Provided config is valid (no warnings)
    #   - max_tasks_in_flight / max_concurrency == 2 (default)
    #   - Default upscaling threshold (200%)
    with patch(
        "ray.data._internal.actor_autoscaler.default_actor_autoscaler.logger.warning"
    ) as mock_warning:
        ds = ray.data.range(2, override_num_blocks=2).map_batches(
            SimpleMapper,
            compute=ray.data.ActorPoolStrategy(
                max_tasks_in_flight_per_actor=2,
            ),
            max_concurrency=1,
        )
        ds.take_all()

    # Check that this warning hasn't been emitted
    warn_log_args_str = str(mock_warning.call_args_list)
    expected_message = (
        "⚠️  Actor Pool configuration of the "
        "ActorPoolMapOperator[MapBatches(SimpleMapper)] will not allow it to scale up: "
    )

    assert expected_message not in warn_log_args_str

    # Test #3: Default config is valid (no warnings)
    #   - max_tasks_in_flight / max_concurrency == 4 (default)
    #   - Default upscaling threshold (200%)
    with patch(
        "ray.data._internal.actor_autoscaler.default_actor_autoscaler.logger.warning"
    ) as mock_warning:
        ds = ray.data.range(2, override_num_blocks=2).map_batches(
            SimpleMapper, compute=ray.data.ActorPoolStrategy()
        )
        ds.take_all()

    # Check that this warning hasn't been emitted
    warn_log_args_str = str(mock_warning.call_args_list)
    expected_message = (
        "⚠️  Actor Pool configuration of the "
        "ActorPoolMapOperator[MapBatches(SimpleMapper)] will not allow it to scale up: "
    )

    assert expected_message not in warn_log_args_str

    # Test #4: Fixed-size pool with invalid config (no warnings)
    #   - max_tasks_in_flight / max_concurrency == 1
    #   - Default upscaling threshold (200%)
    #   - Even though config would normally trigger warning, fixed-size pools
    #     don't scale up by design, so warning should not be emitted
    with patch(
        "ray.data._internal.actor_autoscaler.default_actor_autoscaler.logger.warning"
    ) as mock_warning:
        ds = ray.data.range(2, override_num_blocks=2).map_batches(
            SimpleMapper,
            compute=ray.data.ActorPoolStrategy(
                size=2,
                max_tasks_in_flight_per_actor=1,
            ),
            max_concurrency=1,
        )
        ds.take_all()

    # Check that this warning hasn't been emitted for fixed-size pool
    warn_log_args_str = str(mock_warning.call_args_list)
    expected_message = (
        "⚠️  Actor Pool configuration of the "
        "ActorPoolMapOperator[MapBatches(SimpleMapper)] will not allow it to scale up: "
    )

    assert expected_message not in warn_log_args_str


@pytest.fixture
def autoscaler_config_mocks():
    resource_manager = MagicMock(spec=ResourceManager)
    topology = MagicMock()
    topology.items = MagicMock(return_value=[])
    return resource_manager, topology


def test_autoscaling_config_validation_zero_delta(autoscaler_config_mocks):
    resource_manager, topology = autoscaler_config_mocks

    with pytest.raises(
        ValueError, match="actor_pool_max_upscaling_delta must be positive"
    ):
        DefaultActorAutoscaler(
            topology=topology,
            resource_manager=resource_manager,
            config=AutoscalingConfig(
                actor_pool_util_upscaling_threshold=1.0,
                actor_pool_util_downscaling_threshold=0.5,
                actor_pool_max_upscaling_delta=0,
            ),
            data_context=DataContext.get_current(),
        )


def test_autoscaling_config_validation_negative_delta(autoscaler_config_mocks):
    resource_manager, topology = autoscaler_config_mocks

    with pytest.raises(
        ValueError, match="actor_pool_max_upscaling_delta must be positive"
    ):
        DefaultActorAutoscaler(
            topology=topology,
            resource_manager=resource_manager,
            config=AutoscalingConfig(
                actor_pool_util_upscaling_threshold=1.0,
                actor_pool_util_downscaling_threshold=0.5,
                actor_pool_max_upscaling_delta=-1,
            ),
            data_context=DataContext.get_current(),
        )


def test_autoscaling_config_validation_positive_delta(autoscaler_config_mocks):
    resource_manager, topology = autoscaler_config_mocks

    autoscaler = DefaultActorAutoscaler(
        topology=topology,
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            actor_pool_max_upscaling_delta=5,
        ),
        data_context=DataContext.get_current(),
    )
    assert autoscaler._actor_pool_max_upscaling_delta == 5


def test_autoscaling_config_validation_zero_upscaling_threshold(
    autoscaler_config_mocks,
):
    resource_manager, topology = autoscaler_config_mocks

    with pytest.raises(
        ValueError, match="actor_pool_util_upscaling_threshold must be positive"
    ):
        DefaultActorAutoscaler(
            topology=topology,
            resource_manager=resource_manager,
            config=AutoscalingConfig(
                actor_pool_util_upscaling_threshold=0,
                actor_pool_util_downscaling_threshold=0.5,
                actor_pool_max_upscaling_delta=5,
            ),
            data_context=DataContext.get_current(),
        )


def test_autoscaling_config_validation_negative_upscaling_threshold(
    autoscaler_config_mocks,
):
    resource_manager, topology = autoscaler_config_mocks

    with pytest.raises(
        ValueError, match="actor_pool_util_upscaling_threshold must be positive"
    ):
        DefaultActorAutoscaler(
            topology=topology,
            resource_manager=resource_manager,
            config=AutoscalingConfig(
                actor_pool_util_upscaling_threshold=-1.0,
                actor_pool_util_downscaling_threshold=0.5,
                actor_pool_max_upscaling_delta=5,
            ),
            data_context=DataContext.get_current(),
        )


def test_get_max_scale_up_tolerates_float_drift():
    """Regression test for #64291.

    A budget can carry tiny float drift (e.g. ``gpu=-1e-16``) from chained
    arithmetic. ``_get_max_scale_up`` reads raw fields (non-negativity assert +
    ``floordiv``), so this must not trip the assert or yield a negative
    scale-up. ``ExecutionResources`` rounds at construction, so the drift
    collapses to 0.
    """
    actor_pool = MagicMock()
    actor_pool.per_actor_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1.0, gpu=0.25, memory=0.0)
    )
    # gpu drift rounds to 0 -> 0 actors fit on the gpu dimension -> scale-up 0.
    budget = ExecutionResources(cpu=4, gpu=-1e-16, memory=0.0)
    assert _get_max_scale_up(actor_pool, budget) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
