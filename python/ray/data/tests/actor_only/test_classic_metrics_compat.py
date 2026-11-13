"""The classic per-operator metrics the actor-only backend backfills for the
existing Data dashboard."""
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.callbacks.resource_allocator_prometheus_callback import (  # noqa: E501
    ResourceAllocatorPrometheusCallback,
    _actor_only_budget,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.streaming_executor import (
    ExperimentalStreamingExecutor,
)


def _actor_pool_op(*, current_size=3, output_limit=300.0, pull_bytes=40.0):
    op = MagicMock(spec=ExperimentalActorPoolMapOperator)
    op.metrics = MagicMock()
    op.has_completed.return_value = False
    op.remaining_pull_bytes.return_value = pull_bytes
    pool = op.actor_pool
    pool.current_size.return_value = current_size
    pool.per_actor_resource_usage.return_value = ExecutionResources(
        cpu=2, gpu=1, memory=1000
    )
    pool.total_output_bytes_limit.return_value = output_limit
    return op


def _executor(topology, *, last_updated):
    executor = ExperimentalStreamingExecutor.__new__(ExperimentalStreamingExecutor)
    executor._topology = topology
    executor._metrics_last_updated = last_updated
    executor._resource_bank = MagicMock()
    executor._resource_bank.live_object_store.return_value.total_bytes.return_value = (
        123
    )
    return executor


@pytest.mark.parametrize(
    "force_update,last_updated,fills",
    [
        pytest.param(True, float("inf"), True, id="forced"),
        pytest.param(False, 0.0, True, id="interval-elapsed"),
        pytest.param(False, float("inf"), False, id="between-exports"),
    ],
)
def test_stats_export_fills_classic_metrics(
    monkeypatch, force_update, last_updated, fills
):
    # obj_store_mem_used comes from the bank and the actor-pool gauges are
    # refreshed, but only on ticks the base class actually exports.
    exported = []
    monkeypatch.setattr(
        StreamingExecutor,
        "_update_stats_metrics",
        lambda self, state, force_update=False: exported.append(force_update),
    )
    op, op_state = _actor_pool_op(), MagicMock()
    op.metrics.obj_store_mem_used = 0
    executor = _executor({op: op_state}, last_updated=last_updated)

    executor._update_stats_metrics("RUNNING", force_update=force_update)

    assert exported == [force_update]
    assert op.metrics.obj_store_mem_used == (123 if fills else 0)
    assert op_state.refresh_actor_metrics.called == fills


def test_actor_only_budget_is_held_actors_times_shape():
    # Every actor holding resources counts (current_size includes pending and
    # draining ones); the object store budget is their summed output limits.
    budget = _actor_only_budget(_actor_pool_op(current_size=3))
    assert budget is not None
    assert (budget.cpu, budget.gpu, budget.memory) == (6, 3, 3000)
    assert budget.object_store_memory == 300.0
    # Ops without an actor pool have no budget, as under the classic backend.
    assert _actor_only_budget(MagicMock()) is None


def test_total_output_bytes_limit_counts_draining_actors():
    def actor(status, limit):
        return SimpleNamespace(status=status, max_num_output_bytes=limit)

    pool = SimpleNamespace(
        _running_actors={
            "a": actor(ActorStatus.ACTIVE, 100),
            "b": actor(ActorStatus.IDLE, 50),
            "c": actor(ActorStatus.DRAINING, 1000),
        }
    )
    # Draining actors keep their outputs until consumed, so they count.
    # pyrefly: ignore[bad-argument-type]  # partial test double
    assert _ExperimentalActorPool.total_output_bytes_limit(pool) == 1150


def test_remaining_pull_bytes_sums_per_actor_budgets():
    op = SimpleNamespace(
        actor_pool=SimpleNamespace(running_actor_ids=lambda: ["a", "b"]),
        _actor_pull_budget=lambda actor_id: SimpleNamespace(
            bytes_to_read={"a": 10.0, "b": 32.0}[actor_id]
        ),
    )
    # pyrefly: ignore[bad-argument-type]  # partial test double
    assert ExperimentalActorPoolMapOperator.remaining_pull_bytes(op) == 42.0


@pytest.mark.parametrize("actor_only", [True, False])
def test_budget_callback_picks_the_budget_source(actor_only):
    callback = ResourceAllocatorPrometheusCallback()
    cpu_gauge = MagicMock()
    for gauge in ("_cpu", "_gpu", "_memory", "_osm"):
        setattr(callback, f"{gauge}_budget_gauge", MagicMock())
    callback._cpu_budget_gauge = cpu_gauge
    callback._max_bytes_to_read_gauge = MagicMock()
    op = _actor_pool_op()
    op_state = MagicMock()
    op_state.op_tag.return_value = "Map"
    executor = MagicMock()
    executor._actor_only_backend = actor_only
    executor._topology = {op: op_state}
    executor._resource_manager.get_budget.return_value = ExecutionResources(cpu=7)
    executor._resource_manager.op_resource_allocator.get_output_budget.return_value = (
        None
    )

    callback.on_execution_step(executor)

    cpu = cpu_gauge.set.call_args.args[0]
    assert cpu == (6 if actor_only else 7)
    assert executor._resource_manager.get_budget.called != actor_only
    if actor_only:
        # The op's remaining pull budget, summed over its actors.
        assert callback._max_bytes_to_read_gauge.set.call_args.args[0] == 40.0
    else:
        # The classic allocator reports no output budget here: left unset.
        assert not callback._max_bytes_to_read_gauge.set.called


@pytest.mark.parametrize(
    "queued,running,expected",
    [
        pytest.param(True, 2, True, id="queued"),
        pytest.param(False, 2, False, id="nothing-queued"),
        # Actors still starting up: not backpressure.
        pytest.param(True, 0, False, id="no-running-actors"),
    ],
)
def test_launch_tasks_reports_submission_backpressure(queued, running, expected):
    # A ready bundle no running actor will take is submission backpressure.
    op = _actor_pool_op()
    op.can_submit_task.return_value = False
    op.has_queued_bundle.return_value = queued
    op.actor_pool.num_running_actors.return_value = running
    executor = _executor({op: MagicMock()}, last_updated=0.0)

    executor._launch_tasks()

    op.notify_in_task_submission_backpressure.assert_called_once_with(
        expected, "ActorAdmission"
    )


@pytest.mark.parametrize(
    "blocked,running,completed,expected",
    [
        pytest.param(3, 3, False, True, id="all-blocked"),
        pytest.param(2, 3, False, False, id="some-unblocked"),
        pytest.param(0, 0, False, False, id="no-actors"),
        pytest.param(3, 3, True, False, id="completed"),
    ],
)
def test_output_backpressure_is_sampled_every_step(
    blocked, running, completed, expected
):
    op = _actor_pool_op()
    op.cached_output_backpressured_actors.return_value = (blocked, running)
    op.has_completed.return_value = completed
    topology = {op: MagicMock()}
    executor = _executor(topology, last_updated=0.0)

    executor._sample_output_backpressure(topology)

    op.notify_in_task_output_backpressure.assert_called_once_with(expected, "Output")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
