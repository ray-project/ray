import time
from contextlib import contextmanager
from types import MethodType
from typing import Optional
from unittest.mock import MagicMock

import pytest

import ray
from ray.data import ExecutionResources
from ray.data._internal.execution.autoscaler.default_autoscaler import (
    ActorPoolScalingRequest,
    DefaultAutoscaler,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.context import (
    AutoscalingConfig,
)


def test_actor_pool_scaling():
    """Test `_actor_pool_should_scale_up` and `_actor_pool_should_scale_down`
    in `DefaultAutoscaler`"""

    autoscaler = DefaultAutoscaler(
        topology=MagicMock(),
        resource_manager=MagicMock(),
        execution_id="execution_id",
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
        ),
    )

    # Current actor pool utilization is 0.9, which is above the threshold.
    actor_pool: _ActorPool = MagicMock(
        spec=_ActorPool,
        min_size=MagicMock(return_value=5),
        max_size=MagicMock(return_value=15),
        current_size=MagicMock(return_value=10),
        num_active_actors=MagicMock(return_value=10),
        num_running_actors=MagicMock(return_value=10),
        num_pending_actors=MagicMock(return_value=0),
        num_free_task_slots=MagicMock(return_value=5),
        num_tasks_in_flight=MagicMock(return_value=15),
        _max_actor_concurrency=1,
        get_pool_util=MagicMock(
            # NOTE: Unittest mocking library doesn't support proxying to actual
            #       non-mocked methods so we have emulate it by directly binding existing
            #       method of `get_pool_util` to a mocked object
            side_effect=lambda: MethodType(_ActorPool.get_pool_util, actor_pool)()
        ),
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

    def assert_autoscaling_action(*, delta: int, expected_reason: Optional[str]):
        nonlocal actor_pool, op, op_state

        assert autoscaler._derive_target_scaling_config(
            actor_pool=actor_pool,
            op=op,
            op_state=op_state,
        ) == ActorPoolScalingRequest(delta=delta, reason=expected_reason)

    # Should scale up since the util above the threshold.
    assert actor_pool.get_pool_util() == 1.5
    assert_autoscaling_action(
        delta=1,
        expected_reason="utilization of 1.5 >= 1.0",
    )

    # Should be no-op since the util is below the threshold.
    with patch(actor_pool, "num_tasks_in_flight", 9):
        assert actor_pool.get_pool_util() == 0.9
        assert_autoscaling_action(
            delta=0, expected_reason="utilization of 0.9 w/in limits [0.5, 1.0]"
        )

    # Should be no-op since previous scaling hasn't finished yet
    with patch(actor_pool, "num_pending_actors", 1):
        assert_autoscaling_action(delta=0, expected_reason="pending actors")

    # Should be no-op since we have reached the max size (ie could not scale
    # up even though utilization > threshold)
    with patch(actor_pool, "current_size", 15):
        with patch(actor_pool, "num_tasks_in_flight", 15):
            assert_autoscaling_action(
                delta=0,
                expected_reason="reached max size",
            )

    # Should be no-op since we have reached the min size (ie could not scale
    # down up even though utilization < threshold))
    with patch(actor_pool, "current_size", 5):
        with patch(actor_pool, "num_tasks_in_flight", 4):
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
    with patch(op, "completed", True):
        # NOTE: We simulate actor pool dipping below min size upon
        #       completion (to verify that it will be able to scale to 0)
        with patch(actor_pool, "current_size", 5):
            assert_autoscaling_action(
                delta=-1,
                expected_reason="consumed all inputs",
            )

    # Should scale down only once all inputs have been already dispatched AND
    # no new inputs ar expected
    with patch(op_state.input_queues[0], "__len__", 0):
        with patch(op, "internal_queue_size", 0):
            with patch(op, "_inputs_complete", True, is_method=False):
                assert_autoscaling_action(
                    delta=-1,
                    expected_reason="consumed all inputs",
                )

            # If the input queue is empty but inputs did not complete,
            # allow to scale up still
            assert_autoscaling_action(
                delta=1,
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
        config=AutoscalingConfig(),
    )

    autoscaler._send_resource_request = MagicMock()
    autoscaler._try_scale_up_cluster()

    autoscaler._send_resource_request.assert_called_once_with(
        [{"CPU": 1}, {"CPU": 2}, {"CPU": 2}]
    )


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
