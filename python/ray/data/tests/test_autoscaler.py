from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from ray.data import ExecutionResources
from ray.data._internal.execution.autoscaler.default_autoscaler import (
    DefaultAutoscaler,
    _AutoscalingAction,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.streaming_executor_state import OpState


def test_actor_pool_scaling():
    """Test `_actor_pool_should_scale_up` and `_actor_pool_should_scale_down`
    in `DefaultAutoscaler`"""

    autoscaler = DefaultAutoscaler(
        topology=MagicMock(),
        resource_manager=MagicMock(),
        execution_id="execution_id",
        actor_pool_scaling_up_threshold=0.8,
        actor_pool_scaling_down_threshold=0.5,
    )

    # Current actor pool utilization is 0.9, which is above the threshold.
    actor_pool = MagicMock(
        min_size=MagicMock(return_value=5),
        max_size=MagicMock(return_value=15),
        current_size=MagicMock(return_value=10),
        num_active_actors=MagicMock(return_value=9),
        num_free_task_slots=MagicMock(return_value=5),
    )

    op = MagicMock(
        spec=InternalQueueOperatorMixin,
        completed=MagicMock(return_value=False),
        _inputs_complete=False,
        input_dependencies=[MagicMock()],
        internal_queue_size=MagicMock(return_value=1),
    )
    op_state = OpState(op, inqueues=[MagicMock(__len__=MagicMock(return_value=10))])
    op_state._scheduling_status = MagicMock(under_resource_limits=True)

    @contextmanager
    def patch(mock, attr, value, is_method=True):
        original = getattr(mock, attr)
        if is_method:
            value = MagicMock(return_value=value)
        setattr(mock, attr, value)
        yield
        setattr(mock, attr, original)

    def assert_autoscaling_action(expected_action):
        nonlocal actor_pool, op, op_state

        assert (
            autoscaler._derive_scaling_action(
                actor_pool=actor_pool,
                op=op,
                op_state=op_state,
            )
            == expected_action
        )

    # Should scale up since the util above the threshold.
    assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.9
    assert_autoscaling_action(_AutoscalingAction.SCALE_UP)

    # Should be no-op since the util is below the threshold.
    with patch(actor_pool, "num_active_actors", 7):
        assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.7
        assert_autoscaling_action(_AutoscalingAction.NO_OP)

    # Should be no-op since we have reached the max size.
    with patch(actor_pool, "current_size", 15):
        assert_autoscaling_action(_AutoscalingAction.NO_OP)

    # Should scale up since the pool is below the min size.
    with patch(actor_pool, "current_size", 4):
        assert_autoscaling_action(_AutoscalingAction.SCALE_UP)

    # Should scale down since if the op is completed, or
    # the op has no more inputs.
    with patch(op, "completed", True):
        assert_autoscaling_action(_AutoscalingAction.SCALE_DOWN)

    # Should scale down only once all inputs have been already dispatched
    with patch(op_state.input_queues[0], "__len__", 0):
        with patch(op, "internal_queue_size", 0):
            with patch(op, "_inputs_complete", True, is_method=False):
                assert_autoscaling_action(_AutoscalingAction.SCALE_DOWN)

            with patch(op, "_inputs_complete", False, is_method=False):
                assert_autoscaling_action(_AutoscalingAction.NO_OP)


    # Should be no-op since the op doesn't have enough resources.
    with patch(
        op_state._scheduling_status,
        "under_resource_limits",
        False,
        is_method=False,
    ):
        assert_autoscaling_action(_AutoscalingAction.NO_OP)

    # Should be a no-op since the op has enough free slots for
    # the existing inputs.
    with patch(op_state, "total_enqueued_input_bundles", 5):
        assert_autoscaling_action(_AutoscalingAction.NO_OP)

    # Should scale down since the util is below the threshold.
    with patch(actor_pool, "num_active_actors", 4):
        assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.4
        assert_autoscaling_action(_AutoscalingAction.SCALE_DOWN)

    # Should scale down since the pool is above the max size.
    with patch(actor_pool, "current_size", 16):
        assert_autoscaling_action(_AutoscalingAction.SCALE_DOWN)


def test_cluster_scaling():
    """Test `_try_scale_up_cluster` in `DefaultAutoscaler`"""
    op1 = MagicMock(
        input_dependencies=[],
        incremental_resource_usage=MagicMock(
            return_value=ExecutionResources(cpu=1, gpu=0, object_store_memory=0)
        ),
        num_active_tasks=MagicMock(return_value=1),
    )
    op_state1 = MagicMock(
        _pending_dispatch_input_bundles_count=MagicMock(return_value=0),
        _scheduling_status=MagicMock(
            runnable=False,
        ),
    )
    op2 = MagicMock(
        input_dependencies=[op1],
        incremental_resource_usage=MagicMock(
            return_value=ExecutionResources(cpu=2, gpu=0, object_store_memory=0)
        ),
        num_active_tasks=MagicMock(return_value=1),
    )
    op_state2 = MagicMock(
        _pending_dispatch_input_bundles_count=MagicMock(return_value=1),
        _scheduling_status=MagicMock(
            runnable=False,
        ),
    )
    topology = {
        op1: op_state1,
        op2: op_state2,
    }

    autoscaler = DefaultAutoscaler(
        topology=topology,
        resource_manager=MagicMock(),
        execution_id="execution_id",
    )

    autoscaler._send_resource_request = MagicMock()
    autoscaler._try_scale_up_cluster()

    autoscaler._send_resource_request.assert_called_once_with(
        [{"CPU": 1}, {"CPU": 2}, {"CPU": 2}]
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
