import time
from contextlib import contextmanager
from types import MethodType
from typing import Optional
from unittest.mock import MagicMock

import pytest

import ray
from ray.data import ExecutionResources
from ray.data._internal.actor_autoscaler import (
    ActorPoolScalingRequest,
    DefaultActorAutoscaler,
)
from ray.data._internal.cluster_autoscaler import DefaultClusterAutoscaler
from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.context import (
    AutoscalingConfig,
)


def test_actor_pool_scaling():
    """Test `_actor_pool_should_scale_up` and `_actor_pool_should_scale_down`
    in `DefaultAutoscaler`"""

    resource_manager = MagicMock(
        spec=ResourceManager, get_budget=MagicMock(return_value=None)
    )
    autoscaler = DefaultActorAutoscaler(
        topology=MagicMock(),
        resource_manager=resource_manager,
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
        per_actor_resource_usage=MagicMock(return_value=ExecutionResources(cpu=1)),
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
        completed=MagicMock(return_value=False),
        _inputs_complete=False,
        input_dependencies=[MagicMock()],
        internal_input_queue_num_blocks=MagicMock(return_value=1),
        metrics=MagicMock(average_num_inputs_per_task=1),
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
        delta=1,
        expected_reason="utilization of 1.5 >= 1.0",
    )

    # Should scale up immediately when the actor pool has no running actors.
    with patch(actor_pool, "num_running_actors", 0):
        with patch(actor_pool, "num_free_task_slots", 0):
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

            # Should be no-op since the op has enough free task slots to consume the existing inputs (which is 0).
            assert_autoscaling_action(
                delta=0,
                expected_reason="enough free task slots to consume the existing inputs",
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


@pytest.fixture
def autoscaler_max_upscaling_delta_setup():
    resource_manager = MagicMock(
        spec=ResourceManager, get_budget=MagicMock(return_value=None)
    )

    actor_pool = MagicMock(
        spec=_ActorPool,
        min_size=MagicMock(return_value=5),
        max_size=MagicMock(return_value=20),
        current_size=MagicMock(return_value=10),
        get_current_size=MagicMock(return_value=10),
        num_pending_actors=MagicMock(return_value=0),
        num_free_task_slots=MagicMock(return_value=0),
        get_pool_util=MagicMock(return_value=2.0),
    )

    op = MagicMock(
        spec=InternalQueueOperatorMixin,
        completed=MagicMock(return_value=False),
        _inputs_complete=False,
        metrics=MagicMock(average_num_inputs_per_task=1),
    )
    op_state = MagicMock(
        spec=OpState,
        total_enqueued_input_blocks=MagicMock(return_value=1),
    )
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
        has_pending_bundles=MagicMock(return_value=False),
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
        has_pending_bundles=MagicMock(return_value=True),
        _scheduling_status=MagicMock(
            runnable=False,
        ),
    )
    topology = {
        op1: op_state1,
        op2: op_state2,
    }

    autoscaler = DefaultClusterAutoscaler(
        topology=topology,
        resource_manager=MagicMock(),
        execution_id="execution_id",
    )

    autoscaler._send_resource_request = MagicMock()
    autoscaler.try_trigger_scaling()

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


def test_autoscaling_config_validation_warnings(
    ray_start_10_cpus_shared, restore_data_context
):
    """Test that validation warnings are emitted when actor pool config won't allow scaling up."""
    from unittest.mock import patch

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
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
