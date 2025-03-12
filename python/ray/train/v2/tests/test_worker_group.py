import collections
import os
import time

import pytest

import ray
from ray.exceptions import RayActorError
from ray.train.v2._internal.constants import (
    ENV_VARS_TO_PROPAGATE,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
    WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
)
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
    WorkerHealthCheckFailedError,
    WorkerHealthCheckTimeoutError,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.context import TrainRunContext, get_train_context
from ray.train.v2._internal.execution.worker_group import (
    ActorMetadata,
    RayTrainWorker,
    Worker,
    WorkerGroup,
    WorkerGroupContext,
)
from ray.train.v2.api.config import RunConfig


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def _default_inactive_worker_group(**kwargs):
    default_config = {
        "train_run_context": TrainRunContext(RunConfig()),
        "worker_group_context": _default_worker_group_context(),
    }
    default_config.update(kwargs)

    return WorkerGroup(**default_config)


def _default_worker_group_context(**kwargs):
    default_config = {
        "run_attempt_id": "test_run_attempt_id",
        "train_fn": lambda: None,
        "num_workers": 4,
        "resources_per_worker": {"CPU": 1},
    }
    default_config.update(kwargs)
    return WorkerGroupContext(**default_config)


def test_worker_group_create():
    """Test WorkerGroup.create() factory method."""
    train_run_context = TrainRunContext(run_config=RunConfig())

    worker_group = WorkerGroup.create(
        train_run_context=train_run_context,
        worker_group_context=_default_worker_group_context(),
    )

    assert len(worker_group) == 4
    assert worker_group.has_started()

    with pytest.raises(ValueError, match="Worker group is active"):
        worker_group._start()

    worker_group.shutdown()
    with pytest.raises(ValueError, match="Worker group is not active"):
        worker_group.get_workers()


def test_actor_start_failure():
    class FailingWorker(RayTrainWorker):
        def __init__(self):
            raise RuntimeError("Worker failed to start.")

    wg = _default_inactive_worker_group()
    wg._worker_cls = FailingWorker

    with pytest.raises(WorkerGroupStartupFailedError):
        wg._start()


@pytest.mark.parametrize("error_type", [RayActorError, RuntimeError])
def test_callback_start_failure(error_type):
    class FailingCallback(WorkerGroupCallback):
        def after_worker_group_start(self, worker_group):
            raise error_type

    wg = _default_inactive_worker_group(callbacks=[FailingCallback()])

    if error_type is RayActorError:
        # Actor errors are wrapped in WorkerGroupStartupFailedError.
        with pytest.raises(WorkerGroupStartupFailedError):
            wg._start()
    else:
        # Other errors are bugs in user code and should not be wrapped.
        with pytest.raises(error_type):
            wg._start()

    wg.shutdown()


def test_start_timeout(monkeypatch):
    from ray.util.placement_group import PlacementGroup

    @ray.remote(num_cpus=0)
    def hanging_task(*args, **kwargs):
        time.sleep(60)

    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "0.1")
    monkeypatch.setattr(PlacementGroup, "ready", hanging_task.remote)

    wg = _default_inactive_worker_group()

    with pytest.raises(WorkerGroupStartupTimeoutError):
        # Not enough CPU resources are available, so the workers will not start.
        wg._start()


def test_poll_status_running():
    worker_group_context = WorkerGroupContext(
        run_attempt_id="test_run_attempt_id",
        train_fn=lambda: time.sleep(60),
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )
    wg = _default_inactive_worker_group(worker_group_context=worker_group_context)
    wg._start()
    status = wg.poll_status()
    wg.shutdown()

    assert len(status.worker_statuses) == 4
    assert not status.finished
    assert not status.errors


def test_poll_status_finished():
    worker_group_context = WorkerGroupContext(
        run_attempt_id="test_run_attempt_id",
        train_fn=lambda: "done",
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )
    wg = _default_inactive_worker_group(worker_group_context=worker_group_context)
    wg._start()

    # Wait for the workers to finish the training fn before polling.
    # Otherwise, the poll_status call may return before the workers finish.
    while not wg.poll_status().finished:
        time.sleep(0.01)

    status = wg.poll_status()
    wg.shutdown()

    assert len(status.worker_statuses) == 4
    assert status.finished
    assert not status.errors


@pytest.mark.parametrize("training_failure", [True, False])
@pytest.mark.parametrize("poll_failure", [True, False])
def test_poll_status_failures(monkeypatch, training_failure, poll_failure):
    def train_fn():
        if training_failure:
            raise RuntimeError("train error")

    if poll_failure:

        def patched_poll_status(worker_self):
            raise RuntimeError("poll error")

        monkeypatch.setattr(RayTrainWorker, "poll_status", patched_poll_status)

    worker_group_context = WorkerGroupContext(
        run_attempt_id="test_run_attempt_id",
        train_fn=train_fn,
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )
    wg = _default_inactive_worker_group(worker_group_context=worker_group_context)
    wg._start()
    while not wg.poll_status().finished:
        time.sleep(0.01)

    status = wg.poll_status()
    wg.shutdown()

    assert len(status.worker_statuses) == 4
    assert status.finished
    if poll_failure:
        assert len(status.errors) == 4
        assert ["poll" in str(error) for error in status.errors.values()]
        assert [
            isinstance(error, WorkerHealthCheckFailedError)
            for error in status.errors.values()
        ]
        assert [
            isinstance(error.health_check_failure, RuntimeError)
            for error in status.errors.values()
        ]
    elif training_failure:
        assert len(status.errors) == 4
        assert ["train" in str(error) for error in status.errors.values()]
    else:
        assert not status.errors


def test_poll_status_healthcheck_timeout(monkeypatch):
    monkeypatch.setenv(WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR, "0")

    def hanging_poll_status(worker_self):
        time.sleep(60)

    monkeypatch.setattr(RayTrainWorker, "poll_status", hanging_poll_status)

    wg = _default_inactive_worker_group()

    # Try 2x to ensure that shutdown clears the health-check hanging timer.
    for _ in range(2):
        wg._start()

        status = wg.poll_status(timeout=0.01)

        assert len(status.errors) == 4
        assert all(
            [
                isinstance(error, WorkerHealthCheckTimeoutError)
                for error in status.errors.values()
            ]
        )

        wg.shutdown()


def test_group_workers_by_ip():
    def create_workers(node_ids):
        return [
            Worker(
                actor=None,
                metadata=ActorMetadata(
                    node_id=node_id,
                    node_ip="dummy",
                    hostname="dummy",
                    accelerator_ids={},
                    pid=0,
                ),
                resources={"CPU": 1},
            )
            for node_id in node_ids
        ]

    workers = create_workers(["2", "3", "1", "4", "2", "1", "3", "3", "4", "2"])
    workers = WorkerGroup._sort_workers_by_node_id_and_gpu_id(workers)
    expected = ["2", "2", "2", "3", "3", "3", "1", "1", "4", "4"]
    ips = [w.metadata.node_id for w in workers]
    assert ips == expected, (
        "Workers should be grouped by node ID "
        "and follow the same original order of IDs encountered (2, 3, 1, 4)."
    )

    workers = create_workers(["2", "3", "1", "4", "2", "1", "3", "3", "4", "2"])
    workers = WorkerGroup._sort_workers_by_node_id_and_gpu_id(workers, _first_id="1")
    expected = ["1", "1", "2", "2", "2", "3", "3", "3", "4", "4"]
    ips = [w.metadata.node_id for w in workers]
    assert (
        ips == expected
    ), "Workers should be grouped by ID, with the first ID being 1."


def test_local_rank_assignment():
    def create_workers(pids, node_ids, gpu_ids):
        return [
            Worker(
                actor=None,
                metadata=ActorMetadata(
                    node_id=node_id,
                    node_ip="dummy",
                    hostname="dummy",
                    accelerator_ids={"GPU": gpu_id.split(",") if gpu_id else []},
                    pid=pid,
                ),
                resources={"CPU": 1},
            )
            for pid, node_id, gpu_id in zip(pids, node_ids, gpu_ids)
        ]

    def setup_and_check_worker_group(pids, node_ids, gpu_ids, expected_local_ranks):
        """
        Create a worker group, group workers by IP, and check local ranks assignment.

        Args:
            pids: List of unique process IDs.
            ids: List of node ids corresponding to each PID.
            gpu_ids: List of GPU IDs or None for each PID.
            expected_local_ranks: Dictionary mapping PID to the
                expected local rank.
        """
        workers = create_workers(pids=pids, node_ids=node_ids, gpu_ids=gpu_ids)
        workers = WorkerGroup._sort_workers_by_node_id_and_gpu_id(workers)

        # Build local ranks according to the logics in
        # TODO: Replace this with the actual implementation later
        node_id_dict = collections.defaultdict(int)
        local_ranks_map = collections.defaultdict(int)
        for w in workers:
            local_ranks_map[w.metadata.pid] = node_id_dict[w.metadata.node_id]
            node_id_dict[w.metadata.node_id] += 1

        local_ranks = [local_ranks_map[pid] for pid in pids]

        assert (
            local_ranks == expected_local_ranks
        ), "Incorrect local ranks allocation!\n"
        f"Expect: {expected_local_ranks}\nGot: {local_ranks}"

    # Define the worker configurations for different scenarios
    # For workers without GPU resources, their original order will be preserved
    cpu_workers_config = {
        "pids": [0, 1, 2, 3, 4, 5, 6, 7],
        "node_ids": ["2", "2", "1", "1", "2", "1", "1", "2"],
        "gpu_ids": [None] * 8,
        "expected_local_ranks": [0, 1, 0, 1, 2, 2, 3, 3],
    }

    gpu_workers_single_gpu_config = {
        "pids": [0, 1, 2, 3, 4, 5, 6, 7],
        "node_ids": ["2", "2", "1", "1", "2", "1", "1", "2"],
        "gpu_ids": ["1", "0", "3", "2", "2", "0", "1", "3"],
        "expected_local_ranks": [1, 0, 3, 2, 2, 0, 1, 3],
    }

    # For workers with multiple gpus, sort by their lowest gpu id
    gpu_workers_multiple_gpus_config = {
        "pids": [0, 1, 2, 3],
        "node_ids": ["2", "1", "1", "2"],
        "gpu_ids": ["1,3", "2,1", "0,3", "0,2"],
        "expected_local_ranks": [1, 1, 0, 0],
    }

    # Setup and check worker groups for each configuration
    setup_and_check_worker_group(**cpu_workers_config)
    setup_and_check_worker_group(**gpu_workers_single_gpu_config)
    setup_and_check_worker_group(**gpu_workers_multiple_gpus_config)


def test_setup_worker_group(tmp_path):
    num_workers = 4
    worker_group = WorkerGroup(
        train_run_context=TrainRunContext(
            RunConfig(name="test", storage_path=str(tmp_path))
        ),
        worker_group_context=_default_worker_group_context(num_workers=num_workers),
    )
    worker_group._start()

    def get_world_size():
        return ray.train.get_context().get_world_size()

    def get_world_rank():
        return ray.train.get_context().get_world_rank()

    def get_storage_context_name():
        return ray.train.get_context().get_storage().experiment_dir_name

    assert worker_group.execute(get_world_size) == [num_workers] * num_workers
    assert sorted(worker_group.execute(get_world_rank)) == list(range(num_workers))
    assert worker_group.execute(get_storage_context_name) == ["test"] * num_workers

    worker_group.shutdown()


@pytest.mark.parametrize("queue_backlog_length", [0, 1, 3])
def test_flush_worker_result_queue(queue_backlog_length):
    """Make sure that the result queue is fully consumed before the worker exits."""
    wg = _default_inactive_worker_group()
    wg._start()

    def populate_result_queue():
        # Note that the result queue is a thread-safe queue of maxsize 1.
        get_train_context().get_result_queue().put("result")

    for _ in range(queue_backlog_length):
        wg.execute(populate_result_queue)

        status = wg.poll_status()
        assert all(
            worker_status.training_result
            for worker_status in status.worker_statuses.values()
        )

    status = wg.poll_status()
    assert status.finished

    wg.shutdown()


def test_env_var_propagation(monkeypatch):
    """Ray Train should automatically propagate some environment variables
    from the driver to the workers."""
    test_env_var = list(ENV_VARS_TO_PROPAGATE)[0]
    monkeypatch.setenv(test_env_var, "1")
    wg = _default_inactive_worker_group()
    wg._start()
    env_vars = wg.execute(lambda: os.environ.get(test_env_var))
    wg.shutdown()

    assert env_vars == ["1"] * 4


def test_worker_group_callback():
    """Check that all worker group callback hooks are called."""

    class AssertCallback(WorkerGroupCallback):
        def __init__(self):
            self.start_hook_called = False
            self.training_start_hook_called = False
            self.shutdown_hook_called = False
            self.poll_status_hook_called = False

        def after_worker_group_start(self, worker_group):
            self.start_hook_called = True

        def after_worker_group_training_start(self, worker_group):
            self.training_start_hook_called = True

        def before_worker_group_shutdown(self, worker_group):
            self.shutdown_hook_called = True

        def after_worker_group_poll_status(self, worker_group_status):
            assert len(worker_group_status.worker_statuses) == 4
            self.poll_status_hook_called = True

    hooks = AssertCallback()
    wg = _default_inactive_worker_group(callbacks=[hooks])

    wg._start()
    assert hooks.start_hook_called
    assert hooks.training_start_hook_called
    wg.poll_status()
    assert hooks.poll_status_hook_called
    wg.shutdown()
    assert hooks.shutdown_hook_called


def test_worker_log_file_paths():
    """Test that log file paths are correctly assigned to workers."""
    wg = _default_inactive_worker_group()
    wg._start()

    # Check that all workers have log file paths assigned
    workers = wg.get_workers()
    for worker in workers:
        assert worker.log_file_path is not None
        assert "ray-train-app-worker" in worker.log_file_path

    wg.shutdown()


def test_shutdown_hook_with_dead_actors():
    """Check that the shutdown hook raises correctly if run
    on a mix of alive and dead actors."""

    class ShutdownCallback(WorkerGroupCallback):
        def before_worker_group_shutdown(self, worker_group):
            # Mock a hanging collective call on the remaining workers.
            def f():
                print(ray.train.get_context().get_world_rank())
                time.sleep(10)

            wg.execute(f)

    def conditional_failure():
        if ray.train.get_context().get_world_rank() % 2 == 0:
            ray.actor.exit_actor()

    wg = _default_inactive_worker_group(callbacks=[ShutdownCallback()])
    wg._start()

    # Kill some of the actors
    try:
        wg.execute(conditional_failure)
    except RayActorError:
        pass

    # The shutdown hook should not hang here and should immediately raise.
    start = time.monotonic()
    with pytest.raises(RayActorError):
        wg.shutdown()

    # Should not wait for the full 10 seconds.
    assert time.monotonic() - start < 1

    # TODO: This test leaves the WorkerGroup in a bad state.
    # If more tests are added below this, they may not be able to run.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
