from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from ray.data import ExecutionResources
from ray.data._internal.execution.autoscaler.default_autoscaler import DefaultAutoscaler


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
        completed=MagicMock(return_value=False),
        _inputs_complete=False,
        internal_queue_size=MagicMock(return_value=1),
    )
    op_state = MagicMock(num_queued=MagicMock(return_value=10))
    op_scheduling_status = MagicMock(under_resource_limits=True)
    op_state._scheduling_status = op_scheduling_status

    @contextmanager
    def patch(mock, attr, value, is_method=True):
        original = getattr(mock, attr)
        if is_method:
            value = MagicMock(return_value=value)
        setattr(mock, attr, value)
        yield
        setattr(mock, attr, original)

    # === Test scaling up ===

    def assert_should_scale_up(expected):
        nonlocal actor_pool, op, op_state

        assert (
            autoscaler._actor_pool_should_scale_up(
                actor_pool=actor_pool,
                op=op,
                op_state=op_state,
            )
            == expected
        )

    # Should scale up since the util above the threshold.
    assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.9
    assert_should_scale_up(True)

    # Shouldn't scale up since the util is below the threshold.
    with patch(actor_pool, "num_active_actors", 7):
        assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.7
        assert_should_scale_up(False)

    # Shouldn't scale up since we have reached the max size.
    with patch(actor_pool, "current_size", 15):
        assert_should_scale_up(False)

    # Should scale up since the pool is below the min size.
    with patch(actor_pool, "current_size", 4):
        assert_should_scale_up(True)

    # Shouldn't scale up since if the op is completed, or
    # the op has no more inputs.
    with patch(op, "completed", True):
        assert_should_scale_up(False)
    with patch(op, "_inputs_complete", True, is_method=False):
        with patch(op, "internal_queue_size", 0):
            assert_should_scale_up(False)

    # Shouldn't scale up since the op doesn't have enough resources.
    with patch(
        op_scheduling_status,
        "under_resource_limits",
        False,
        is_method=False,
    ):
        assert_should_scale_up(False)

    # Shouldn't scale up since the op has enough free slots for
    # the existing inputs.
    with patch(op_state, "num_queued", 5):
        assert_should_scale_up(False)

    # === Test scaling down ===

    def assert_should_scale_down(expected):
        assert (
            autoscaler._actor_pool_should_scale_down(
                actor_pool=actor_pool,
                op=op,
            )
            == expected
        )

    # Shouldn't scale down since the util above the threshold.
    assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.9
    assert_should_scale_down(False)

    # Should scale down since the util is below the threshold.
    with patch(actor_pool, "num_active_actors", 4):
        assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.4
        assert_should_scale_down(True)

    # Should scale down since the pool is above the max size.
    with patch(actor_pool, "current_size", 16):
        assert_should_scale_down(True)

    # Shouldn't scale down since we have reached the min size.
    with patch(actor_pool, "current_size", 5):
        assert_should_scale_down(False)

    # Should scale down since if the op is completed, or
    # the op has no more inputs.
    with patch(op, "completed", True):
        assert_should_scale_down(True)
    with patch(op, "_inputs_complete", True, is_method=False):
        with patch(op, "internal_queue_size", 0):
            assert_should_scale_down(True)


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
        num_queued=MagicMock(return_value=0),
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
        num_queued=MagicMock(return_value=1),
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
