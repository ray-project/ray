from typing import Optional
from unittest.mock import MagicMock

import pytest

from ray.data._internal.actor_autoscaler import (
    ACTOR_AUTOSCALER_ENV_KEY,
    ActorPoolScalingRequest,
    AutoscalingActorPool,
    BacklogAwareActorAutoscaler,
    DefaultActorAutoscaler,
    create_actor_autoscaler,
)
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    AutoscalingActorConfig,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.context import AutoscalingConfig


class MockAutoscalingActorPool(AutoscalingActorPool):
    def __init__(
        self,
        min_size,
        max_size,
        max_tasks_in_flight_per_actor,
        per_actor_resource_usage,
    ):
        config = AutoscalingActorConfig(
            min_size=min_size,
            max_size=max_size,
            initial_size=min_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight_per_actor,
            max_actor_concurrency=1,
            per_actor_resource_usage=per_actor_resource_usage,
        )
        super().__init__(config)
        self._min_size = min_size
        self._max_size = max_size
        self._current_size = 0
        self._num_running_actors = 0
        self._num_restarting_actors = 0
        self._num_active_actors = 0
        self._num_pending_actors = 0
        self._max_tasks_in_flight_per_actor = max_tasks_in_flight_per_actor
        self._current_in_flight_tasks = 0
        self._per_actor_resource_usage = per_actor_resource_usage

    def max_actor_concurrency(self) -> int:
        return 1

    def min_size(self):
        return self._min_size

    def max_size(self):
        return self._max_size

    def current_size(self):
        return self._current_size

    def num_running_actors(self):
        return self._num_running_actors

    def num_restarting_actors(self) -> int:
        return self._num_restarting_actors

    def num_active_actors(self):
        return self._num_running_actors

    def num_pending_actors(self):
        return self._num_pending_actors

    def max_tasks_in_flight_per_actor(self):
        return self._max_tasks_in_flight_per_actor

    def num_tasks_in_flight(self):
        return self._current_in_flight_tasks

    def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
        if req.delta > 0:
            num_actors = req.delta

            self._current_size += num_actors
            self._num_pending_actors += num_actors

            return num_actors

        elif req.delta < 0:
            num_actors = -req.delta

            self._current_size -= num_actors
            num_pending_to_decrease = min(num_actors, self._num_pending_actors)
            self._num_pending_actors -= num_pending_to_decrease
            num_running_to_decrease = num_actors - num_pending_to_decrease
            assert num_running_to_decrease <= self._num_running_actors
            self._num_running_actors -= num_running_to_decrease

            return -num_actors

    def refresh_actor_state(self) -> None:
        pass

    def on_task_submitted(self, actor) -> None:
        pass

    def on_task_completed(self, actor) -> None:
        pass

    def select_actors(
        self,
        bundle=None,
        actor_locality_enabled=False,
    ):
        if self.num_running_actors() == 0:
            return None
        if (
            self.num_tasks_in_flight()
            >= self.num_running_actors() * self.max_tasks_in_flight_per_actor()
        ):
            return None
        return MagicMock()

    def get_pending_actor_refs(self):
        return []

    def pending_to_running(self, ready_ref=None):
        assert self._num_pending_actors > 0
        self._num_pending_actors -= 1
        self._num_running_actors += 1
        return None

    def get_actor_location(self, actor) -> str:
        return "node1"

    def shutdown(self, force: bool = False) -> None:
        self._num_running_actors = 0
        self._num_pending_actors = 0
        self._num_active_actors = 0
        self._current_size = 0
        self._current_in_flight_tasks = 0

    def per_actor_resource_usage(self) -> ExecutionResources:
        return self._per_actor_resource_usage


def _make_autoscaler(
    actor_pool: MockAutoscalingActorPool,
    *,
    enqueued_input_blocks: int = 10,
    budget: Optional[ExecutionResources] = None,
    allocation: Optional[ExecutionResources] = None,
    op_usage: Optional[ExecutionResources] = None,
) -> BacklogAwareActorAutoscaler:
    """Build a single-op ``BacklogAwareActorAutoscaler`` around ``actor_pool``.

    ``budget`` and ``allocation`` default to unbounded limits and ``op_usage``
    to zero usage, so callers only override them when exercising the
    resource-constrained paths.
    """
    if budget is None:
        budget = ExecutionResources.for_limits()
    if allocation is None:
        allocation = ExecutionResources.for_limits()
    if op_usage is None:
        op_usage = ExecutionResources()

    op = MagicMock(
        spec=PhysicalOperator,
        get_autoscaling_actor_pools=MagicMock(return_value=[actor_pool]),
        has_completed=MagicMock(return_value=False),
        _inputs_complete=False,
        internal_input_queue_num_blocks=MagicMock(return_value=1),
        metrics=MagicMock(average_num_inputs_per_task=1, num_inputs_received=1),
    )
    op_state = MagicMock(
        spec=OpState,
        total_enqueued_input_blocks=MagicMock(return_value=enqueued_input_blocks),
    )
    op_state._scheduling_status = MagicMock(under_resource_limits=True)
    op_state.op = op

    resource_manager = MagicMock()
    resource_manager.get_budget = MagicMock(return_value=budget)
    resource_manager.get_allocation = MagicMock(return_value=allocation)
    resource_manager.get_op_usage = MagicMock(return_value=op_usage)

    return BacklogAwareActorAutoscaler(
        topology={op: op_state},
        resource_manager=resource_manager,
        config=AutoscalingConfig(
            actor_pool_util_upscaling_threshold=1.0,
            actor_pool_util_downscaling_threshold=0.5,
            # Pinned explicitly: these tests assert exact backlog-derived
            # deltas, which any per-iteration cap would truncate.
            actor_pool_max_upscaling_delta=None,
        ),
    )


class TestActorPoolAutoscaling:
    """Tests for actor pool autoscaling in ``BacklogAwareActorAutoscaler``."""

    def test_actor_pool_autoscaling(self):
        """Test `try_trigger_scaling`, including actor pool utilization check and
        number of actors to scale up/down, not including other scaling up/down
        conditions.
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
        autoscaler = _make_autoscaler(actor_pool, enqueued_input_blocks=10)

        # Manually scale up to min_size.
        # Actor pool util should be 0 since there are no tasks, but there are
        # pending actors
        actor_pool.scale(ActorPoolScalingRequest(delta=min_size))
        assert actor_pool.get_pool_util() == 0

        # Move pending actors to running.
        for _ in range(min_size):
            actor_pool.pending_to_running()
        assert actor_pool.get_pool_util() == 0

        # Updated the number of used task slots and check the util.
        actor_pool._current_in_flight_tasks = 7
        util = actor_pool.get_pool_util()
        assert util == pytest.approx(
            7 / (actor_pool.max_actor_concurrency() * actor_pool.num_running_actors())
        )
        # Scale-up should be triggered:
        #   - total_tasks = 7 in-flight + 10 backlog = 17
        #   - min required size = ceil(17 / (1 * 1.0)) = 17
        #   - Uncapped delta = 17 - 2 = 15, capped by max_size to 6
        autoscaler.try_trigger_scaling()
        assert actor_pool.current_size() == max_size  # 2 + 6 = 8

        # Mark all actors as running.
        for _ in range(actor_pool.num_pending_actors()):
            actor_pool.pending_to_running()
        # Set tasks to 30 (high utilization): 30 / (1 * 8) = 3.75 > 1.0
        actor_pool._current_in_flight_tasks = 30
        util = actor_pool.get_pool_util()
        assert util == pytest.approx(
            30 / (actor_pool.max_actor_concurrency() * actor_pool.num_running_actors())
        )
        # Scale-up should not be triggered since already at max_size.
        autoscaler.try_trigger_scaling()
        assert actor_pool.current_size() == max_size  # still 8

        # Updated the number of used task slots and check the util.
        actor_pool._current_in_flight_tasks = 3
        util = actor_pool.get_pool_util()
        assert util == pytest.approx(
            3 / (actor_pool.max_actor_concurrency() * actor_pool.num_running_actors()),
        )  # 3 / (1 * 8) = 0.375 < 0.5
        # Scale-down should be triggered.
        autoscaler.try_trigger_scaling()
        assert actor_pool.current_size() == max_size - 1  # current_size = 8 - 1 = 7

        # Check the util again.
        actor_pool._current_in_flight_tasks = 4
        util = actor_pool.get_pool_util()
        assert util == pytest.approx(
            4 / (actor_pool.max_actor_concurrency() * actor_pool.num_running_actors()),
        )  # 4 / (1 * 7) = 0.5714 > 0.5
        # Neither scale-up nor scale-down should be triggered.
        autoscaler.try_trigger_scaling()
        assert actor_pool.current_size() == max_size - 1

    @pytest.mark.parametrize(
        "in_flight_tasks, enqueued_input_blocks, avg_inputs_per_task, expected_size",
        (
            # A single in-flight task saturates the 1-actor pool, and the
            # backlog carries it the rest of the way: ceil((1 + 12) / 1) = 13.
            [1, 12, 1, 13],
            # Utilization still gates upscaling: an idle pool doesn't grow, no
            # matter how large the backlog is.
            [0, 12, 1, 1],
            # In-flight tasks and backlog are summed: ceil((4 + 12) / 1) = 16.
            [4, 12, 1, 16],
            # 12 blocks at 4 blocks/task is only ceil(12 / 4) = 3 tasks, so the
            # pool needs 4 + 3 = 7 actors.
            [4, 12, 4, 7],
            # No backlog at all: the pool is sized off in-flight tasks only.
            [4, 0, 1, 4],
        ),
    )
    def test_upscale_delta_accounts_for_backlog(
        self, in_flight_tasks, enqueued_input_blocks, avg_inputs_per_task, expected_size
    ):
        """The upscale delta is sized off in-flight tasks plus enqueued backlog."""
        min_size = 1
        actor_pool = MockAutoscalingActorPool(
            min_size=min_size,
            max_size=100,
            max_tasks_in_flight_per_actor=self._MAX_TASKS_IN_FLIGHT_PER_ACTOR,
            per_actor_resource_usage=ExecutionResources(cpu=1),
        )
        autoscaler = _make_autoscaler(
            actor_pool, enqueued_input_blocks=enqueued_input_blocks
        )
        op_state = next(iter(autoscaler._topology.values()))
        op_state.op.metrics.average_num_inputs_per_task = avg_inputs_per_task

        actor_pool.scale(ActorPoolScalingRequest(delta=min_size))
        actor_pool.pending_to_running()
        actor_pool._current_in_flight_tasks = in_flight_tasks

        autoscaler.try_trigger_scaling()
        assert actor_pool.current_size() == expected_size

    # Default per-actor in-flight capacity used by the helper below. Tests that
    # need full task-slot saturation can set ``in_flight_tasks`` to
    # ``initial_size * _MAX_TASKS_IN_FLIGHT_PER_ACTOR``.
    _MAX_TASKS_IN_FLIGHT_PER_ACTOR = 4

    def _run_autoscaling_scenario(
        self,
        *,
        min_size,
        max_size,
        initial_size,
        per_actor_resource_usage,
        in_flight_tasks=0,
        budget=None,
        allocation=None,
        op_usage=None,
    ):
        """Build a single-op autoscaler, fire ``try_trigger_scaling`` once, and
        return the resulting actor pool.

        ``in_flight_tasks`` defaults to ``0``, which models an idle pool. This
        is not a no-op for utilization-based scaling: if ``initial_size`` is
        greater than ``min_size``, idle utilization may trigger downscaling.
        """
        actor_pool = MockAutoscalingActorPool(
            min_size=min_size,
            max_size=max_size,
            max_tasks_in_flight_per_actor=self._MAX_TASKS_IN_FLIGHT_PER_ACTOR,
            per_actor_resource_usage=per_actor_resource_usage,
        )
        autoscaler = _make_autoscaler(
            actor_pool,
            budget=budget,
            allocation=allocation,
            op_usage=op_usage,
        )

        # Bring the pool up to ``initial_size`` running actors before scaling.
        actor_pool.scale(ActorPoolScalingRequest(delta=initial_size))
        for _ in range(initial_size):
            actor_pool.pending_to_running()

        actor_pool._current_in_flight_tasks = in_flight_tasks
        autoscaler.try_trigger_scaling()
        return actor_pool

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
            [
                1,
                2,
                ExecutionResources(cpu=1),
                ExecutionResources.for_limits(),
                1,
            ],
        ),
    )
    def test_actor_pool_autoscaling_respects_budgets(
        self,
        min_size,
        max_size,
        per_actor_resource_usage,
        budget,
        expected_scale_up,
    ):
        """Test that the upscale delta is capped by the operator's budget.

        ``allocation``/``op_usage`` are left at the helper's neutral defaults
        so the over-allocation scale-down branch is a no-op (covered by
        ``test_actor_pool_scales_down_when_over_allocation``).
        """
        actor_pool = self._run_autoscaling_scenario(
            min_size=min_size,
            max_size=max_size,
            initial_size=min_size,
            per_actor_resource_usage=per_actor_resource_usage,
            # Saturate every actor so utilization triggers scale-up.
            in_flight_tasks=min_size * self._MAX_TASKS_IN_FLIGHT_PER_ACTOR,
            budget=budget,
        )
        assert actor_pool.current_size() - min_size == expected_scale_up

    @pytest.mark.parametrize(
        "min_size, initial_size, per_actor_resource_usage, allocation, op_usage, expected_scale_down",
        (
            # Over allocation by 1 CPU; per_actor=1 → release 1 actor.
            [
                1,
                2,
                ExecutionResources(cpu=1),
                ExecutionResources(cpu=1),
                ExecutionResources(cpu=2),
                1,
            ],
            # Over allocation by 3 CPU; per_actor=2 → release ceil(3/2) = 2 actors.
            [
                1,
                3,
                ExecutionResources(cpu=2),
                ExecutionResources(cpu=2),
                ExecutionResources(cpu=5),
                2,
            ],
            # Over allocation but pinned to min_size: cannot release any actor.
            [
                1,
                1,
                ExecutionResources(cpu=1),
                ExecutionResources(cpu=1),
                ExecutionResources(cpu=10),
                0,
            ],
        ),
    )
    def test_actor_pool_scales_down_when_over_allocation(
        self,
        min_size,
        initial_size,
        per_actor_resource_usage,
        allocation,
        op_usage,
        expected_scale_down,
    ):
        """Test that the autoscaler sheds actors when the operator exceeds its
        resource allocation, and is pinned to ``min_size`` otherwise.

        The over-allocation check uses ``allocation - op_usage`` (not
        ``budget``), so this test leaves ``budget`` unbounded (the helper's
        default) and only varies ``allocation`` / ``op_usage``.
        """
        actor_pool = self._run_autoscaling_scenario(
            min_size=min_size,
            # Make sure max_size doesn't constrain the test setup.
            max_size=max(initial_size, min_size + 1),
            initial_size=initial_size,
            per_actor_resource_usage=per_actor_resource_usage,
            allocation=allocation,
            op_usage=op_usage,
        )
        assert initial_size - actor_pool.current_size() == expected_scale_down


@pytest.mark.parametrize(
    "env_value, expected_type",
    (
        [None, BacklogAwareActorAutoscaler],
        ["DEFAULT", DefaultActorAutoscaler],
        ["BACKLOG_AWARE", BacklogAwareActorAutoscaler],
    ),
)
def test_create_actor_autoscaler_selection(monkeypatch, env_value, expected_type):
    """``RAY_DATA_ACTOR_AUTOSCALER`` selects the implementation, defaulting to
    ``BacklogAwareActorAutoscaler`` when unset."""
    if env_value is None:
        monkeypatch.delenv(ACTOR_AUTOSCALER_ENV_KEY, raising=False)
    else:
        monkeypatch.setenv(ACTOR_AUTOSCALER_ENV_KEY, env_value)

    autoscaler = create_actor_autoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )
    assert type(autoscaler) is expected_type


def test_create_actor_autoscaler_rejects_unknown_version(monkeypatch):
    monkeypatch.setenv(ACTOR_AUTOSCALER_ENV_KEY, "NOT_A_VERSION")

    with pytest.raises(ValueError, match="isn't a valid option"):
        create_actor_autoscaler(
            topology={},
            resource_manager=MagicMock(),
            config=AutoscalingConfig(),
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
