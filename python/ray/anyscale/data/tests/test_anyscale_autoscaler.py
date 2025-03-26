import math
from typing import OrderedDict
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from ray.anyscale.data.autoscaler.anyscale_autoscaler import (
    AnyscaleAutoscaler,
    _NodeResourceSpec,
    _TimeWindowAverageCalculator,
    DefaultActorPoolResizingPolicy,
    _normalize_scaling_up_factor,
    _get_scaling_up_factor,
)
from ray.data._internal.execution.autoscaler.autoscaling_actor_pool import (
    AutoscalingActorPool,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.streaming_executor_state import OpState


@pytest.fixture(autouse=True)
def enable_actor_pool_downscaling(monkeypatch):
    monkeypatch.setenv("RAY_DATA_DISABLE_ACTOR_POOL_SCALING_DOWN", "0")
    yield


@pytest.fixture(autouse=True)
def patch_autoscaling_coordinator():
    with patch(
        "ray.anyscale.air._internal.autoscaling_coordinator.get_or_create_autoscaling_coordinator"  # noqa: E501
    ):
        yield


@pytest.fixture
def current_time():
    class MutableInt:
        def __init__(self, value: int = 0):
            self.value = value

        def __repr__(self):
            return f"MutableInt({self.value})"

        def increment(self):
            self.value += 1

        def get_value(self) -> int:
            return self.value

    _current_time = MutableInt()

    def time():
        return _current_time.get_value()

    with patch("time.time", time):
        yield _current_time


def test_calcuate_time_window_average(current_time):
    """Test _TimeWindowAverageCalculator."""
    window_s = 10
    values_to_report = [i + 1 for i in range(20)]

    calculator = _TimeWindowAverageCalculator(window_s)
    assert calculator.get_average() is None

    for value in values_to_report:
        # Report values, test `get_average`.
        # and proceed the time by 1 second each time.
        calculator.report(value)
        avg = calculator.get_average()
        values_in_window = values_to_report[
            max(current_time.get_value() - 10, 0) : current_time.get_value() + 1
        ]
        expected = sum(values_in_window) / len(values_in_window)
        assert avg == expected, current_time.get_value()
        current_time.increment()

    for _ in range(10):
        # Keep proceeding the time, and test `get_average`.
        avg = calculator.get_average()
        values_in_window = values_to_report[max(current_time.get_value() - 10, 0) : 20]
        expected = sum(values_in_window) / len(values_in_window)
        assert avg == expected, current_time.get_value()
        current_time.increment()

    # Now no values in the time window, `get_average` should return None.
    assert calculator.get_average() is None


@pytest.mark.parametrize(
    "input_factor, expected_normalized, capacity_ratios, expected_scaling_factors",
    [
        # Test with a single float value
        (
            2.0,
            OrderedDict([(1, 2.0)]),
            [0.5, 0.9],
            [2.0, 2.0],
        ),
        # Test with a dictionary with multiple thresholds
        (
            {0.2: 5.0, 0.5: 3.0, 0.8: 1.5},
            OrderedDict([(0.2, 5.0), (0.5, 3.0), (0.8, 1.5)]),
            [0.1, 0.3, 0.5, 0.7, 0.9],
            [5.0, 3.0, 3.0, 1.5, 1.0],
        ),
        # Test with a single threshold at 1.0
        (
            {1.0: 2.5},
            OrderedDict([(1.0, 2.5)]),
            [0.5, 1.0],
            [2.5, 2.5],
        ),
    ],
)
def test_scaling_up_factor_functions(
    input_factor, expected_normalized, capacity_ratios, expected_scaling_factors
):
    """Test _normalize_scaling_up_factor and _get_scaling_up_factor."""
    # Test _normalize_scaling_up_factor
    normalized = _normalize_scaling_up_factor(input_factor)
    assert isinstance(normalized, OrderedDict)
    assert list(normalized.items()) == list(expected_normalized.items())

    # Test _get_scaling_up_factor with different capacity ratio values
    for i, capacity_ratio in enumerate(capacity_ratios):
        factor = _get_scaling_up_factor(capacity_ratio, normalized)
        assert (
            factor == expected_scaling_factors[i]
        ), f"Failed for capacity_ratio={capacity_ratio}"


@pytest.mark.parametrize(
    "invalid_factor",
    [
        1.0,  # Scaling factor must be > 1
        {1.2: 2.0},  # Keys must be between 0 and 1
        {0.5: 1.0},  # Values must be > 1
    ],
)
def test_invalid_scaling_up_factors(invalid_factor):
    """Test that invalid inputs for _normalize_scaling_up_factor raise assertion errors."""
    with pytest.raises(AssertionError):
        _normalize_scaling_up_factor(invalid_factor)


class TestClusterAutoscaling(unittest.TestCase):
    """Tests for cluster autoscaling functions in AnyscaleAutoscaler."""

    def setup_class(self):
        self._node_type1 = {
            "CPU": 4,
            "memory": 1000,
            "object_store_memory": 500,
        }
        self._node_type2 = {
            "CPU": 8,
            "memory": 2000,
            "object_store_memory": 500,
        }
        self._node_type3 = {
            "CPU": 4,
            "GPU": 1,
            "memory": 1000,
            "object_store_memory": 500,
        }
        self._head_node = {
            "CPU": 4,
            "memory": 1000,
            "object_store_memory": 500,
            "node:__internal_head__": 1.0,
        }

    def test_get_node_resource_spec_and_count(self):
        # Test _get_node_resource_spec_and_count
        autoscaler = AnyscaleAutoscaler(
            topology=MagicMock(),
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
        )

        node_table = [
            {
                "Resources": self._head_node,
                "Alive": True,
            },
            {
                "Resources": self._node_type1,
                "Alive": True,
            },
            {
                "Resources": self._node_type2,
                "Alive": True,
            },
            {
                "Resources": self._node_type3,
                "Alive": True,
            },
            {
                "Resources": self._node_type1,
                "Alive": True,
            },
            {
                "Resources": self._node_type2,
                "Alive": False,
            },
        ]

        expected = {
            _NodeResourceSpec.of(
                self._node_type1["CPU"], self._node_type1["memory"]
            ): 2,
            _NodeResourceSpec.of(
                self._node_type2["CPU"], self._node_type2["memory"]
            ): 1,
        }

        with patch("ray.nodes", return_value=node_table):
            assert autoscaler._get_node_resource_spec_and_count() == expected

    @patch(
        "ray.anyscale.data.autoscaler.anyscale_autoscaler.AnyscaleAutoscaler._send_resource_request",  # noqa: E501
    )
    def test_try_scale_up_cluster(self, _send_resource_request):
        # Test _try_scale_up_cluster
        scaling_up_factor = 1.5
        autoscaler = AnyscaleAutoscaler(
            topology=MagicMock(),
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            cluster_scaling_up_factor=scaling_up_factor,
        )
        _send_resource_request.assert_called_with([])

        resource_spec1 = _NodeResourceSpec.of(
            self._node_type1["CPU"], self._node_type1["memory"]
        )
        resource_spec2 = _NodeResourceSpec.of(
            self._node_type2["CPU"], self._node_type2["memory"]
        )
        autoscaler._get_node_resource_spec_and_count = MagicMock(
            return_value={
                resource_spec1: 2,
                resource_spec2: 1,
            },
        )

        # Test different CPU/memory utilization combinations.
        scale_up_threshold = (
            AnyscaleAutoscaler.DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD
        )
        for cpu_util in [scale_up_threshold / 2, scale_up_threshold]:
            for mem_util in [scale_up_threshold / 2, scale_up_threshold]:
                # Should scale up if either CPU or memory utilization is above
                # the threshold.
                should_scale_up = (
                    cpu_util >= scale_up_threshold or mem_util >= scale_up_threshold
                )
                autoscaler._get_cluster_cpu_and_mem_util = MagicMock(
                    return_value=(cpu_util, mem_util),
                )
                autoscaler._try_scale_up_cluster()
                if not should_scale_up:
                    _send_resource_request.assert_called_with([])
                else:
                    expected_resource_request = [
                        {
                            "CPU": self._node_type1["CPU"],
                            "memory": self._node_type1["memory"],
                        }
                    ] * math.ceil(scaling_up_factor * 2)
                    expected_resource_request.extend(
                        [
                            {
                                "CPU": self._node_type2["CPU"],
                                "memory": self._node_type2["memory"],
                            }
                        ]
                        * math.ceil(scaling_up_factor * 1)
                    )
                    _send_resource_request.assert_called_with(expected_resource_request)


class MockAutoscalingActorPool(AutoscalingActorPool):
    def __init__(
        self,
        min_size,
        max_size,
        max_tasks_in_flight_per_actor,
        per_actor_resource_usage,
    ):
        self._min_size = min_size
        self._max_size = max_size
        self._current_size = 0
        self._num_running_actors = 0
        self._num_active_actors = 0
        self._num_pending_actors = 0
        self._max_tasks_in_flight_per_actor = max_tasks_in_flight_per_actor
        self._current_in_flight_tasks = 0
        self._per_actor_resource_usage = per_actor_resource_usage

    def min_size(self):
        return self._min_size

    def max_size(self):
        return self._max_size

    def current_size(self):
        return self._current_size

    def num_running_actors(self):
        return self._num_running_actors

    def num_active_actors(self):
        return self._num_active_actors

    def num_pending_actors(self):
        return self._num_pending_actors

    def max_tasks_in_flight_per_actor(self):
        return self._max_tasks_in_flight_per_actor

    def current_in_flight_tasks(self):
        return self._current_in_flight_tasks

    def scale_up(self, num_actors: int) -> int:
        self._current_size += num_actors
        self._num_pending_actors += num_actors
        return num_actors

    def scale_down(self, num_actors: int) -> int:
        self._current_size -= num_actors
        num_pending_to_decrease = min(num_actors, self._num_pending_actors)
        self._num_pending_actors -= num_pending_to_decrease
        num_running_to_decrease = num_actors - num_pending_to_decrease
        assert num_running_to_decrease <= self._num_running_actors
        self._num_running_actors -= num_running_to_decrease
        return num_actors

    def pending_to_running(self):
        assert self._num_pending_actors > 0
        self._num_pending_actors -= 1
        self._num_running_actors += 1

    def per_actor_resource_usage(self) -> ExecutionResources:
        return self._per_actor_resource_usage


class TestActorPoolAutoscaling:
    """Tests for actor pool autoscaling functions in AnyscaleAutoscaler."""

    def test_actor_pool_autoscaling(self, current_time):
        """Test `_try_scale_up_or_down_actor_pool`,
        including actor pool utilization check and number of actors to scale up/down,
        not including other scaling up/down conditions.
        """
        min_size = 2
        max_size = 8
        max_tasks_in_flight_per_actor = 4
        actor_pool = MockAutoscalingActorPool(
            min_size=min_size,
            max_size=max_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight_per_actor,
            per_actor_resource_usage=ExecutionResources(cpu=1),
        )
        scaling_up_threadhold = 0.8
        scaling_down_threadhold = 0.5
        scaling_up_factor = 3

        op = MagicMock(
            spec=PhysicalOperator,
            get_autoscaling_actor_pools=MagicMock(return_value=[actor_pool]),
            completed=MagicMock(return_value=False),
            _inputs_complete=False,
            internal_queue_size=MagicMock(return_value=1),
        )
        op_state = MagicMock(spec=OpState, num_queued=MagicMock(return_value=10))
        op_scheduling_status = MagicMock(under_resource_limits=True)
        op_state._scheduling_status = op_scheduling_status

        resource_manager = MagicMock()
        resource_manager._op_resource_allocator.get_budget = MagicMock(
            return_value=ExecutionResources.for_limits()
        )
        autoscaler = AnyscaleAutoscaler(
            topology={op: op_state},
            resource_manager=resource_manager,
            execution_id="test_execution_id",
            actor_pool_scaling_up_threshold=scaling_up_threadhold,
            actor_pool_scaling_down_threshold=scaling_down_threadhold,
            actor_pool_util_check_interval_s=0,
            actor_pool_util_avg_window_s=0.1,
            actor_pool_resizing_policy=DefaultActorPoolResizingPolicy(
                scaling_up_factor
            ),
        )

        # Manually scale up to min_size.
        # Actor pool should be None since there is no running actor.
        actor_pool.scale_up(min_size)
        assert autoscaler._calculate_actor_pool_util(actor_pool) is None
        current_time.increment()

        # Move pending actors to running.
        # Actor pool should be 0 since there are running actors now.
        for _ in range(min_size):
            actor_pool.pending_to_running()
        assert autoscaler._calculate_actor_pool_util(actor_pool) == 0
        current_time.increment()

        # Updated the number of used task slots and check the util.
        actor_pool._current_in_flight_tasks = 7
        util = autoscaler._calculate_actor_pool_util(actor_pool)
        assert util is not None
        # 7 / (2 * 4) = 0.875 > 0.8
        assert util == pytest.approx(7 / (min_size * max_tasks_in_flight_per_actor))
        # Scale-up should be triggered.
        autoscaler._try_scale_up_or_down_actor_pool()
        assert actor_pool.current_size() == math.ceil(
            min_size * scaling_up_factor
        )  # current_size = 2 * 3 = 6
        current_time.increment()

        # Mark all actors as running.
        for _ in range(actor_pool.num_pending_actors()):
            actor_pool.pending_to_running()
        # Updated the number of used task slots and check the util.
        actor_pool._current_in_flight_tasks = 24
        util = autoscaler._calculate_actor_pool_util(actor_pool)
        assert util is not None
        assert util == pytest.approx(
            24 / (actor_pool.current_size() * max_tasks_in_flight_per_actor)
        )  # 24 / (6 * 4) = 1.0 > 0.8
        # Scale-up should be triggered.
        # The size should be capped by max_size.
        autoscaler._try_scale_up_or_down_actor_pool()
        assert actor_pool.current_size() == max_size  # current_size = 8
        current_time.increment()

        # Mark all actors as running.
        for _ in range(actor_pool.num_pending_actors()):
            actor_pool.pending_to_running()
        # Updated the number of used task slots and check the util.
        actor_pool._current_in_flight_tasks = 15
        util = autoscaler._calculate_actor_pool_util(actor_pool)
        assert util is not None
        assert util == pytest.approx(
            15 / (actor_pool.current_size() * max_tasks_in_flight_per_actor),
        )  # 15 / (8 * 4) = 0.46875 < 0.5
        # Scale-down should be triggered.
        autoscaler._try_scale_up_or_down_actor_pool()
        assert actor_pool.current_size() == max_size - 1  # current_size = 8 - 1 = 7
        current_time.increment()

        # Check the util again.
        util = autoscaler._calculate_actor_pool_util(actor_pool)
        assert util is not None
        assert util == pytest.approx(
            util,
            15 / (actor_pool.current_size() * max_tasks_in_flight_per_actor),
        )  # 15 / (7 * 4) = 0.5357 > 0.5
        # Neither scale-up nor scale-down should be triggered.
        autoscaler._try_scale_up_or_down_actor_pool()
        assert actor_pool.current_size() == max_size - 1
        current_time.increment()

    def test_actor_pool_scaling_at_different_capacity_ratios(self, current_time):
        """Test actor pool scaling up with different capacity ratio thresholds."""
        min_size = 2
        max_size = 20
        max_tasks_in_flight_per_actor = 4

        # Define multiple capacity ratio thresholds with different scaling factors
        capacity_based_scaling_config = {0.25: 4.0, 0.5: 2.0, 1.0: 1.5}

        actor_pool = MockAutoscalingActorPool(
            min_size=min_size,
            max_size=max_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight_per_actor,
            per_actor_resource_usage=ExecutionResources(cpu=1),
        )

        op = MagicMock(
            spec=PhysicalOperator,
            get_autoscaling_actor_pools=MagicMock(return_value=[actor_pool]),
            completed=MagicMock(return_value=False),
            _inputs_complete=False,
            internal_queue_size=MagicMock(return_value=1),
        )
        op_state = MagicMock(spec=OpState, num_queued=MagicMock(return_value=100))
        op_scheduling_status = MagicMock(under_resource_limits=True)
        op_state._scheduling_status = op_scheduling_status

        resource_manager = MagicMock()
        resource_manager._op_resource_allocator.get_budget = MagicMock(
            return_value=ExecutionResources.for_limits()
        )

        autoscaler = AnyscaleAutoscaler(
            topology={op: op_state},
            resource_manager=resource_manager,
            execution_id="test_execution_id",
            actor_pool_scaling_up_threshold=0.8,  # High utilization threshold to ensure scaling
            actor_pool_util_check_interval_s=0,
            actor_pool_util_avg_window_s=0.1,
            actor_pool_resizing_policy=DefaultActorPoolResizingPolicy(
                capacity_based_scaling_config
            ),
        )

        # Start with min_size and mark all as running
        actor_pool.scale_up(min_size)
        for _ in range(min_size):
            actor_pool.pending_to_running()

        # Set high task utilization to trigger scaling
        actor_pool._current_in_flight_tasks = min_size * max_tasks_in_flight_per_actor

        # Test scaling at different capacity ratios
        expected_sizes = []

        # First scale (very low capacity ratio - should use 4.0 scaling factor)
        # Current capacity ratio: 2/20 = 0.1 < 0.25, so factor = 4.0
        autoscaler._try_scale_up_or_down_actor_pool()
        expected_sizes.append(
            min(max_size, math.ceil(min_size * 4.0))
        )  # min_size * 4.0 = 8
        assert actor_pool.current_size() == expected_sizes[-1]

        # Mark all actors as running
        for _ in range(actor_pool.num_pending_actors()):
            actor_pool.pending_to_running()
        current_time.increment()

        # Second scale (low capacity ratio - should use 2.0 scaling factor)
        # Current capacity ratio: 8/20 = 0.4 > 0.25 but < 0.5, so factor = 2.0
        actor_pool._current_in_flight_tasks = (
            actor_pool.current_size() * max_tasks_in_flight_per_actor
        )
        autoscaler._try_scale_up_or_down_actor_pool()
        expected_sizes.append(
            min(max_size, math.ceil(expected_sizes[-1] * 2.0))
        )  # 8 * 2.0 = 16
        assert actor_pool.current_size() == expected_sizes[-1]

        # Mark all actors as running
        for _ in range(actor_pool.num_pending_actors()):
            actor_pool.pending_to_running()
        current_time.increment()

        # Third scale (high capacity ratio - should use 1.5 scaling factor)
        # Current capacity ratio: 16/20 = 0.8 > 0.5 but < 1.0, so factor = 1.5
        actor_pool._current_in_flight_tasks = (
            actor_pool.current_size() * max_tasks_in_flight_per_actor
        )
        autoscaler._try_scale_up_or_down_actor_pool()
        # Should hit max_size limit (16 * 1.5 = 24, but max is 20)
        assert actor_pool.current_size() == max_size

    @pytest.mark.parametrize(
        "min_size, max_size, per_actor_resource_usage, budget, expected_scale_up",
        (
            # Budget is 1 CPU and each actor uses 1 CPU. So, we can scale up by 1 actor.
            [
                1,
                100,
                ExecutionResources(cpu=1),
                ExecutionResources.for_limits(cpu=1),
                1,
            ],
            # Budget is 4 CPU and each actor uses 2 CPU. So, we can scale up by
            # 4 / 2 = 2 actors.
            [
                1,
                100,
                ExecutionResources(cpu=2),
                ExecutionResources.for_limits(cpu=4),
                2,
            ],
            # Budget is unbounded, so we can scale up to max size.
            [1, 2, ExecutionResources(cpu=1), ExecutionResources.for_limits(), 1],
        ),
    )
    def test_actor_pool_autoscaling_respects_budgets(
        self,
        min_size,
        max_size,
        per_actor_resource_usage,
        budget,
        expected_scale_up,
        current_time,
    ):
        max_tasks_in_flight_per_actor = 4
        actor_pool = MockAutoscalingActorPool(
            min_size=min_size,
            max_size=max_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight_per_actor,
            per_actor_resource_usage=per_actor_resource_usage,
        )

        op = MagicMock(
            spec=PhysicalOperator,
            get_autoscaling_actor_pools=MagicMock(return_value=[actor_pool]),
            completed=MagicMock(return_value=False),
            _inputs_complete=False,
            internal_queue_size=MagicMock(return_value=1),
        )
        op_state = MagicMock(spec=OpState, num_queued=MagicMock(return_value=10))
        op_scheduling_status = MagicMock(under_resource_limits=True)
        op_state._scheduling_status = op_scheduling_status

        resource_manager = MagicMock()
        resource_manager._op_resource_allocator.get_budget = MagicMock(
            return_value=budget
        )
        scaling_up_threadhold = 0.8
        scaling_down_threadhold = 0.5
        scaling_up_factor = 3
        autoscaler = AnyscaleAutoscaler(
            topology={op: op_state},
            resource_manager=resource_manager,
            execution_id="test_execution_id",
            actor_pool_scaling_up_threshold=scaling_up_threadhold,
            actor_pool_scaling_down_threshold=scaling_down_threadhold,
            actor_pool_util_check_interval_s=0,
            actor_pool_util_avg_window_s=0.1,
            actor_pool_resizing_policy=DefaultActorPoolResizingPolicy(
                scaling_up_factor
            ),
        )

        # Manually scale up to min_size and move pending actors to running.
        # Actor pool should be None since there is no running actor.
        actor_pool.scale_up(min_size)
        for _ in range(min_size):
            actor_pool.pending_to_running()
        current_time.increment()

        # Set the utilization to 100% and try to scale up.
        actor_pool._current_in_flight_tasks = min_size * max_tasks_in_flight_per_actor
        autoscaler._try_scale_up_or_down_actor_pool()
        actual_scale_up = actor_pool.current_size() - min_size
        assert actual_scale_up == expected_scale_up

    def test_should_scale_up_and_down_conditions(self):
        """Test conditions for `_actor_pool_should_scale_up` and
        `_actor_pool_should_scale_down`."""
        # Current actor pool utilization is 0.9, which is above the threshold.
        actor_pool = MagicMock(
            min_size=MagicMock(return_value=5),
            max_size=MagicMock(return_value=15),
            current_size=MagicMock(return_value=10),
            num_free_task_slots=MagicMock(return_value=5),
            num_pending_actors=MagicMock(return_value=0),
        )

        op = MagicMock(
            spec=PhysicalOperator,
            get_autoscaling_actor_pools=MagicMock(return_value=[actor_pool]),
            completed=MagicMock(return_value=False),
            _inputs_complete=False,
            internal_queue_size=MagicMock(return_value=1),
        )
        op_state = MagicMock(spec=OpState, num_queued=MagicMock(return_value=10))
        op_scheduling_status = MagicMock(under_resource_limits=True)
        op_state._scheduling_status = op_scheduling_status

        scaling_up_threadhold = 0.8
        scaling_down_threadhold = 0.5
        autoscaler = AnyscaleAutoscaler(
            topology={op: op_state},
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            actor_pool_scaling_up_threshold=scaling_up_threadhold,
            actor_pool_scaling_down_threshold=scaling_down_threadhold,
            actor_pool_util_check_interval_s=0,
            actor_pool_util_avg_window_s=0.1,
        )
        autoscaler._calculate_actor_pool_util = MagicMock(return_value=0.9)

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

            util = autoscaler._calculate_actor_pool_util(actor_pool)
            assert util is not None

            assert (
                autoscaler._actor_pool_should_scale_up(
                    actor_pool=actor_pool,
                    op=op,
                    op_state=op_state,
                    util=util,
                )
                == expected
            )

        # Should scale up since the util above the threshold.
        assert_should_scale_up(True)

        # Shouldn't scale up since the util is below the threshold.
        with patch(autoscaler, "_calculate_actor_pool_util", 0.7):
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

        # Shouldn't scale up when there are pending actors.
        with patch(actor_pool, "num_pending_actors", 1):
            assert_should_scale_up(False)

        # === Test scaling down ===

        def assert_should_scale_down(expected):

            util = autoscaler._calculate_actor_pool_util(actor_pool)
            assert util is not None

            assert (
                autoscaler._actor_pool_should_scale_down(
                    actor_pool=actor_pool,
                    op=op,
                    util=util,
                )
                == expected
            )

        # Shouldn't scale down since the util above the threshold.
        assert autoscaler._calculate_actor_pool_util(actor_pool) == 0.9
        assert_should_scale_down(False)

        # Should scale down since the util is below the threshold.
        with patch(autoscaler, "_calculate_actor_pool_util", 0.4):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
