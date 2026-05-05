import collections
import os
import time
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.state import state as ray_state
from ray.exceptions import RayActorError, RayTaskError
from ray.runtime_env import RuntimeEnv
from ray.train.v2._internal.callbacks import backend_setup
from ray.train.v2._internal.callbacks.backend_setup import BackendSetupCallback
from ray.train.v2._internal.constants import (
    ENV_VARS_TO_PROPAGATE,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
    WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
)
from ray.train.v2._internal.exceptions import (
    InsufficientClusterResourcesError,
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
    WorkerHealthCheckFailedError,
    WorkerHealthCheckTimeoutError,
)
from ray.train.v2._internal.execution.callback import (
    ReplicaGroupCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    get_train_context,
)
from ray.train.v2._internal.execution.worker_group import (
    ActorMetadata,
    RayTrainWorker,
    Worker,
    WorkerGroup,
    WorkerGroupContext,
    WorkerGroupState,
)
from ray.train.v2._internal.util import ObjectRefWrapper
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.tests.util import DummyObjectRefWrapper, create_dummy_run_context
from ray.util.state import list_actors

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def _default_inactive_worker_group(**kwargs):
    default_config = {
        "train_run_context": create_dummy_run_context(),
        "worker_group_context": _default_worker_group_context(),
    }
    default_config.update(kwargs)

    return WorkerGroup(**default_config)


def _default_worker_group_context(**kwargs):
    default_config = {
        "run_attempt_id": "test_run_attempt_id",
        "train_fn_ref": DummyObjectRefWrapper(lambda: None),
        "num_workers": 4,
        "resources_per_worker": {"CPU": 1},
    }
    default_config.update(kwargs)
    return WorkerGroupContext(**default_config)


def test_worker_group_create():
    """Test WorkerGroup.create() factory method."""

    worker_group = WorkerGroup.create(
        train_run_context=create_dummy_run_context(),
        worker_group_context=_default_worker_group_context(),
    )

    assert len(worker_group) == 4
    assert worker_group.has_started()

    with pytest.raises(ValueError, match="Worker group is active"):
        worker_group._start()

    worker_group.shutdown()
    with pytest.raises(ValueError, match="Worker group is not active"):
        worker_group.get_workers()


def test_replace_replica_group():
    """Test that replace_replica_group correctly replaces a failing replica group."""
    wg = _default_inactive_worker_group()
    wg._start()

    # Remember old state.
    old_workers = wg.get_workers()
    old_state = wg.get_worker_group_state()
    old_replica_groups = wg.get_replica_groups()
    old_rg0_workers = old_replica_groups[0].get_workers()
    old_rg1_workers = old_replica_groups[1].get_workers()

    # Replace replica group 0 and get new state.
    wg.replace_replica_group(0)
    new_workers = wg.get_workers()
    new_state = wg.get_worker_group_state()
    new_replica_groups = wg.get_replica_groups()

    # Assert most of WorkerGroupState is preserved.
    assert len(new_workers) == len(old_workers)
    assert new_state.start_time == old_state.start_time
    assert new_state.placement_group_handle is old_state.placement_group_handle
    assert new_state.sync_actor is old_state.sync_actor

    # Assert replica group 0 workers are replaced but with same distributed contexts.
    new_rg0_workers = new_replica_groups[0].get_workers()
    for old_w, new_w in zip(old_rg0_workers, new_rg0_workers):
        assert new_w is not old_w
    new_rg1_workers = new_replica_groups[1].get_workers()
    for old_w, new_w in zip(old_rg1_workers, new_rg1_workers):
        assert new_w is old_w
    for old_w, new_w in zip(old_rg0_workers, new_rg0_workers):
        assert (
            new_w.distributed_context.world_rank == old_w.distributed_context.world_rank
        )

    # Assert other state is as expected.
    for w in new_rg0_workers:
        assert (
            wg._worker_rank_to_replica_group_rank[w.distributed_context.world_rank] == 0
        )
    for old_w in old_rg0_workers:
        assert (
            old_w.distributed_context.world_rank not in wg._world_rank_to_ongoing_poll
        )

    wg.shutdown()


def test_replace_replica_group_succeed_on_retry():
    """Test that replace_replica_group raises WorkerGroupStartupFailedError
    when a replacement worker fails to initialize."""

    class FailingWorker(RayTrainWorker):
        def __init__(self):
            raise RuntimeError("Replacement worker failed to start.")

    wg = _default_inactive_worker_group()
    wg._start()

    # Swap the worker class so replacement workers will fail.
    wg._worker_cls = FailingWorker

    with pytest.raises(WorkerGroupStartupFailedError):
        wg.replace_replica_group(0)

    # Swap worker class so second attempt succeeds.
    wg._worker_cls = RayTrainWorker
    wg.replace_replica_group(0)

    wg.shutdown()


@pytest.mark.parametrize(
    "runtime_env",
    [{"env_vars": {"DUMMY_VAR": "abcd"}}, RuntimeEnv(env_vars={"DUMMY_VAR": "abcd"})],
)
def test_worker_group_create_with_runtime_env(runtime_env):
    """Test WorkerGroup.create() factory method with a custom runtime environment."""

    run_config = RunConfig(worker_runtime_env=runtime_env)
    train_run_context = create_dummy_run_context(run_config=run_config)

    worker_group_context = _default_worker_group_context()

    worker_group = WorkerGroup.create(
        train_run_context=train_run_context,
        worker_group_context=worker_group_context,
    )

    env_vars = worker_group.execute(lambda: os.environ.get("DUMMY_VAR"))
    assert env_vars == ["abcd"] * worker_group_context.num_workers

    worker_group.shutdown()


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


def test_actor_start_failure():
    class FailingWorker(RayTrainWorker):
        def __init__(self):
            raise RuntimeError("Worker failed to start.")

    wg = _default_inactive_worker_group()
    wg._worker_cls = FailingWorker

    with pytest.raises(WorkerGroupStartupFailedError):
        wg._start()
    # TODO: this and other tests should verify that we shut down the worker group.


def test_callback_start_failure():
    class FailingCallback(WorkerGroupCallback):
        def after_worker_group_start(self, worker_group):
            raise RuntimeError("Worker failed to start.")

    wg = _default_inactive_worker_group(callbacks=[FailingCallback()])

    with pytest.raises(RuntimeError):
        wg._start()

    wg.shutdown()


def test_start_timeout(monkeypatch):
    from ray.train.v2._internal.execution.worker_group.placement_group_handle import (
        DefaultPlacementGroupHandle,
    )

    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "0.1")
    monkeypatch.setattr(
        DefaultPlacementGroupHandle,
        "wait",
        lambda self, timeout_seconds=None: False,
    )

    wg = _default_inactive_worker_group()

    with pytest.raises(WorkerGroupStartupTimeoutError):
        # Not enough CPU resources are available, so the workers will not start.
        wg._start()


def test_zombie_actor_termination(ray_start_4_cpus):
    """This test checks that RayTrainWorker actors are terminated correctly even if python garbage collection hangs on actor shutdown."""
    NUM_WORKERS = 4

    def is_process_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        else:
            return True

    class Node:
        def __init__(self, name):
            self.name = name
            self.other = None

        def __del__(self):
            # Simulate hang during garbage collection
            while True:
                time.sleep(1)

    def train_fn():
        # Create a circular reference to delay garbage collection
        a, b = Node("a"), Node("b")
        a.other = b
        b.other = a

    train_fn_ref = ObjectRefWrapper(train_fn)

    train_run_context = create_dummy_run_context(
        scaling_config=ScalingConfig(num_workers=NUM_WORKERS)
    )
    worker_group_context = _default_worker_group_context(
        train_fn_ref=train_fn_ref,
        num_workers=NUM_WORKERS,
    )

    # Starts the worker group and runs the train function
    worker_group = WorkerGroup.create(
        train_run_context=train_run_context,
        worker_group_context=worker_group_context,
        callbacks=[],
    )

    train_worker_pids = [
        actor.pid
        for actor in list_actors()
        if actor.class_name == RayTrainWorker.__name__ and actor.state == "ALIVE"
    ]

    assert len(train_worker_pids) == NUM_WORKERS

    worker_group.shutdown()

    # ray.kill is async, allow some time for the processes to terminate
    TIMEOUT_S = 5
    deadline = time.monotonic() + TIMEOUT_S
    remaining = set(train_worker_pids)
    while remaining and time.monotonic() < deadline:
        remaining = {pid for pid in remaining if is_process_alive(pid)}
        if remaining:
            time.sleep(0.1)

    assert not remaining


def test_insufficient_cluster_resources_startup_failure(monkeypatch):
    """Test that WorkerGroup startup fails when cluster has insufficient resources.

    This test mocks the cluster resources to match the test environment and
    verifies that the resource check properly catches insufficient resources.
    """
    # Mock the cluster resources to return the test cluster configuration (4 CPUs)
    monkeypatch.setattr(
        ray_state, "get_max_resources_from_cluster_config", lambda: {"CPU": 4.0}
    )

    # The test cluster has 4 CPUs, so requesting 8 workers with 1 CPU each should fail
    worker_group_context = _default_worker_group_context(
        num_workers=8,  # More workers than available CPUs
        resources_per_worker={"CPU": 1.0},
    )

    wg = _default_inactive_worker_group(worker_group_context=worker_group_context)

    # This should fail during startup due to insufficient resources
    with pytest.raises(
        InsufficientClusterResourcesError, match="Insufficient cluster resources"
    ):
        wg._start()


# TODO: consider test_poll_status methods that verify that _world_rank_to_ongoing_poll
# is updated correctly.


def test_poll_status_running():
    worker_group_context = _default_worker_group_context(
        train_fn_ref=DummyObjectRefWrapper(lambda: time.sleep(60)),
    )
    wg = _default_inactive_worker_group(worker_group_context=worker_group_context)
    wg._start()
    status = wg.poll_status()
    wg.shutdown()

    assert len(status.worker_statuses) == 4
    assert not status.finished
    assert not status.errors
    assert status.worker_rank_to_replica_group_rank == {0: 0, 1: 1, 2: 2, 3: 3}
    assert status.failing_replica_group_indices == set()


def test_poll_status_finished():
    worker_group_context = _default_worker_group_context(
        train_fn_ref=DummyObjectRefWrapper(lambda: "done"),
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
    assert status.worker_rank_to_replica_group_rank == {0: 0, 1: 1, 2: 2, 3: 3}
    assert status.failing_replica_group_indices == set()


@pytest.mark.parametrize("actor_failure", [True, False])
def test_poll_status_failures(monkeypatch, tmp_path, actor_failure):
    """Tests that the worker group raises the correct errors when the
    actor fails or the user code raises an error on any worker."""

    dummy_file = tmp_path / "dummy.txt"

    def train_fn():
        # Error when the worker group initialization is finished.
        while not dummy_file.exists():
            time.sleep(0.01)

        if actor_failure:
            os._exit(1)
        else:
            raise RuntimeError("Mock user code error")

    worker_group_context = _default_worker_group_context(
        train_fn_ref=DummyObjectRefWrapper(train_fn),
    )
    wg = _default_inactive_worker_group(worker_group_context=worker_group_context)
    wg._start()

    dummy_file.touch()
    while not wg.poll_status().finished:
        time.sleep(0.01)

    status = wg.poll_status()
    wg.shutdown()

    assert len(status.worker_statuses) == 4
    assert status.finished
    assert status.worker_rank_to_replica_group_rank == {0: 0, 1: 1, 2: 2, 3: 3}
    assert status.failing_replica_group_indices == {0, 1, 2, 3}
    if actor_failure:
        assert len(status.errors) == 4
        assert [
            isinstance(error, WorkerHealthCheckFailedError)
            for error in status.errors.values()
        ]
        assert [
            isinstance(error.health_check_failure, RuntimeError)
            for error in status.errors.values()
        ]
    else:
        assert len(status.errors) == 4
        assert all(
            ["user code error" in str(error) for error in status.errors.values()]
        )


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
        assert status.failing_replica_group_indices == {0, 1, 2, 3}

        wg.shutdown()


@pytest.mark.parametrize("queue_backlog_length", [0, 1, 3])
def test_flush_worker_result_queue(queue_backlog_length):
    """Test that the worker group is still considered running while the
    result queue is not fully consumed."""
    wg = _default_inactive_worker_group()
    wg._start()

    def populate_result_queue():
        # Note that the result queue is a thread-safe queue of maxsize 1.
        get_train_context().get_result_queue().put("result")

    for _ in range(queue_backlog_length):
        wg.execute(populate_result_queue)

        status = wg.poll_status()
        assert all(
            worker_status.training_report
            for worker_status in status.worker_statuses.values()
        )
        assert not status.finished

    # Wait for the workers to finish the training fn and for any pending
    # training_report(s) to be flushed/consumed.
    timeout_s = 5
    deadline = time.monotonic() + timeout_s
    while True:
        status = wg.poll_status()
        if status.finished:
            break
        assert (
            time.monotonic() < deadline
        ), f"Timed out waiting for worker group to finish. Last status: {status}"
        time.sleep(0.01)

    assert all(
        worker_status.training_report is None
        for worker_status in status.worker_statuses.values()
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
    workers = WorkerGroup._sort_workers_by_gpu_id_grouped_by_node(workers)
    expected = ["2", "2", "2", "3", "3", "3", "1", "1", "4", "4"]
    ips = [w.metadata.node_id for w in workers]
    assert ips == expected, (
        "Workers should be grouped by node ID "
        "and follow the same original order of IDs encountered (2, 3, 1, 4)."
    )

    workers = create_workers(["2", "3", "1", "4", "2", "1", "3", "3", "4", "2"])
    workers = WorkerGroup._sort_workers_by_gpu_id_grouped_by_node(
        workers, _first_id="1"
    )
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
        workers = WorkerGroup._sort_workers_by_gpu_id_grouped_by_node(workers)

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


@pytest.mark.parametrize("replace_rg", [False, True], ids=["start", "replace_rg"])
def test_setup_worker_group(tmp_path, replace_rg):
    num_workers = 4
    worker_group = WorkerGroup(
        train_run_context=create_dummy_run_context(
            run_config=RunConfig(name="test", storage_path=str(tmp_path))
        ),
        worker_group_context=_default_worker_group_context(num_workers=num_workers),
    )
    if replace_rg:
        worker_group._manages_replica_groups = True
    worker_group._start()

    if replace_rg:
        worker_group.replace_replica_group(0)

    def get_world_size():
        return ray.train.get_context().get_world_size()

    def get_world_rank():
        return ray.train.get_context().get_world_rank()

    def get_storage_context_name():
        return ray.train.get_context().get_storage().experiment_dir_name

    def get_local_rank():
        return ray.train.get_context().get_local_rank()

    def get_local_world_size():
        return ray.train.get_context().get_local_world_size()

    def get_node_rank():
        return ray.train.get_context().get_node_rank()

    if replace_rg:
        assert worker_group.execute(get_local_rank) == [0] * num_workers
        assert worker_group.execute(get_local_world_size) == [1] * num_workers
    else:
        assert worker_group.execute(get_local_rank) == list(range(num_workers))
        assert worker_group.execute(get_local_world_size) == [num_workers] * num_workers
    assert worker_group.execute(get_node_rank) == [0] * num_workers
    assert worker_group.execute(get_world_size) == [num_workers] * num_workers
    assert sorted(worker_group.execute(get_world_rank)) == list(range(num_workers))
    assert worker_group.execute(get_storage_context_name) == ["test"] * num_workers

    worker_group.shutdown()


def test_worker_group_callback():
    """Check that all worker group callback hooks are called."""

    class AssertCallback(WorkerGroupCallback):
        def __init__(self):
            self.start_hook_called = False
            self.training_start_hook_called = False
            self.shutdown_hook_called = False
            self.poll_status_hook_called = False
            self.abort_hook_called = False

        def after_worker_group_start(self, worker_group):
            self.start_hook_called = True

        def after_worker_group_training_start(self, worker_group):
            self.training_start_hook_called = True

        def before_worker_group_shutdown(self, worker_group):
            self.shutdown_hook_called = True

        def after_worker_group_shutdown(self, worker_group_context):
            self.after_worker_group_shutdown_hook_called = True

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
    assert hooks.after_worker_group_shutdown_hook_called


def _make_backend_setup_callback_with_failing_shutdown(
    error: Exception,
) -> BackendSetupCallback:
    """Build a `BackendSetupCallback` whose backend raises `error` from `on_shutdown`."""
    failing_backend = MagicMock()
    failing_backend.on_shutdown.side_effect = error
    backend_config = MagicMock()
    backend_config.backend_cls.return_value = failing_backend
    cb = BackendSetupCallback(backend_config)
    cb._backend = failing_backend
    return cb


@pytest.mark.parametrize(
    "shutdown_error",
    [
        RayActorError(actor_id="abc", error_msg="actor died"),
        RayTaskError(
            function_name="_shutdown_torch",
            traceback_str="traceback",
            cause=RuntimeError("NCCL error: remote process exited"),
            proctitle="test",
            pid=1,
            ip="127.0.0.1",
        ),
    ],
    ids=["RayActorError", "RayTaskError"],
)
def test_backend_setup_callback_swallows_shutdown_failure(shutdown_error):
    """Test `BackendSetupCallback` swallows both RayActorError and RayTaskError so
    `WorkerGroup.shutdown()` does not propagate the cleanup failure.
    """
    cb = _make_backend_setup_callback_with_failing_shutdown(shutdown_error)
    failing_backend = cb._backend

    wg = _default_inactive_worker_group(callbacks=[cb])
    wg._start()

    with patch.object(backend_setup, "logger") as mock_logger:
        wg.shutdown()  # must not raise

    failing_backend.on_shutdown.assert_called_once()
    mock_logger.warning.assert_called_once()
    msg = mock_logger.warning.call_args.args[0]
    assert "Graceful shutdown of backend failed" in msg
    # exc_info=True keeps the underlying NCCL/actor failure in the logs.
    assert mock_logger.warning.call_args.kwargs.get("exc_info") is True


def test_backend_setup_callback_propagates_unexpected_shutdown_error():
    """Non-Ray exceptions from `on_shutdown` must propagate so they aren't
    silently masked."""
    cb = _make_backend_setup_callback_with_failing_shutdown(
        ValueError("unexpected backend bug")
    )
    with pytest.raises(ValueError, match="unexpected backend bug"):
        cb.before_execution_group_shutdown(MagicMock())


@pytest.mark.parametrize("replace_rg", [False, True], ids=["start", "replace_rg"])
def test_worker_log_file_paths(replace_rg):
    """Test that log file paths are correctly assigned to workers."""
    wg = _default_inactive_worker_group()
    wg._start()

    if replace_rg:
        wg.replace_replica_group(0)

    # Check that all workers have log file paths assigned
    workers = wg.get_workers()
    for worker in workers:
        assert worker.log_file_path is not None
        assert "ray-train-app-worker" in worker.log_file_path

    wg.shutdown()


def test_replica_group_callback():
    """Check that replica group callback hooks are called during replace_replica_group."""

    class AssertCallback(ReplicaGroupCallback):
        def __init__(self):
            self.shutdown_rg = None
            self.start_rg = None
            self.init_context_workers = None

        def before_replica_group_shutdown(self, replica_group):
            self.shutdown_rg = replica_group

        def after_replica_group_start(self, replica_group):
            self.start_rg = replica_group

        def before_init_train_context(self, workers):
            self.init_context_workers = workers
            return {}

    hooks = AssertCallback()
    wg = _default_inactive_worker_group(callbacks=[hooks])
    wg._start()

    old_rg = wg.get_replica_groups()[0]
    wg.replace_replica_group(0)
    new_rg = wg.get_replica_groups()[0]

    assert hooks.shutdown_rg is old_rg
    assert hooks.start_rg is new_rg
    assert hooks.start_rg is not hooks.shutdown_rg
    assert hooks.init_context_workers == new_rg.get_workers()

    wg.shutdown()


def test_worker_group_abort(monkeypatch):
    class AssertCallback(WorkerGroupCallback):
        def __init__(self):
            self.abort_hook_called = False

        def before_worker_group_abort(self, worker_group_context):
            self.abort_hook_called = True

        def after_worker_group_abort(self, worker_group_context):
            self.after_worker_group_abort_hook_called = True

    hooks = AssertCallback()
    wg = _default_inactive_worker_group(callbacks=[hooks])

    wg._start()

    # Track shutdown calls without preventing actual cleanup
    shutdown_call_count = 0
    original_shutdown = WorkerGroupState.shutdown

    def track_shutdown_calls(self):
        nonlocal shutdown_call_count
        shutdown_call_count += 1
        return original_shutdown(self)

    monkeypatch.setattr(WorkerGroupState, "shutdown", track_shutdown_calls)

    wg.abort()
    assert (
        shutdown_call_count == 1
    ), f"Expected shutdown to be called once, but was called {shutdown_call_count} times"
    assert hooks.abort_hook_called
    assert hooks.after_worker_group_abort_hook_called

    # Bypass _assert_active method, allowing for shutdown
    monkeypatch.setattr(wg, "_assert_active", lambda: None)

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


def test_check_cluster_resources_and_raise_if_insufficient(monkeypatch):
    """Test _check_cluster_resources_and_raise_if_insufficient static method."""

    def _assert_resource_check(
        available_resources, resources_per_worker, num_workers, should_raise
    ):
        """Helper to test resource checking with different scenarios."""
        monkeypatch.setattr(
            ray_state,
            "get_max_resources_from_cluster_config",
            lambda: available_resources,
        )

        if should_raise:
            with pytest.raises(
                InsufficientClusterResourcesError,
                match="Insufficient cluster resources",
            ):
                WorkerGroup._check_cluster_resources_and_raise_if_insufficient(
                    resources_per_worker=resources_per_worker, num_workers=num_workers
                )
        else:
            # Should not raise
            WorkerGroup._check_cluster_resources_and_raise_if_insufficient(
                resources_per_worker=resources_per_worker, num_workers=num_workers
            )

    # Test case 1: Sufficient resources - should not raise
    _assert_resource_check(
        available_resources={"CPU": 8.0, "GPU": 4.0},
        resources_per_worker={"CPU": 1.0, "GPU": 0.5},
        num_workers=4,
        should_raise=False,
    )

    # Test case 2: Insufficient CPU resources - should raise
    _assert_resource_check(
        available_resources={"CPU": 8.0, "GPU": 4.0},
        resources_per_worker={"CPU": 3.0},
        num_workers=4,  # Requires 12 CPU but only 8 available
        should_raise=True,
    )

    # Test case 3: Insufficient GPU resources - should raise
    _assert_resource_check(
        available_resources={"CPU": 8.0, "GPU": 4.0},
        resources_per_worker={"GPU": 2.0},
        num_workers=3,  # Requires 6 GPU but only 4 available
        should_raise=True,
    )

    # Test case 4: Missing resource type in cluster - should raise
    _assert_resource_check(
        available_resources={"CPU": 8.0, "GPU": 4.0},
        resources_per_worker={"TPU": 1.0},
        num_workers=1,  # TPU not available in cluster
        should_raise=True,
    )

    # Test case 5: Resource available but zero - should raise
    _assert_resource_check(
        available_resources={"CPU": 8.0, "GPU": 0},
        resources_per_worker={"GPU": 1.0},
        num_workers=1,
        should_raise=True,
    )

    # Test case 6: Empty cluster resources - should not raise
    _assert_resource_check(
        available_resources={},
        resources_per_worker={"CPU": 1.0},
        num_workers=2,
        should_raise=False,
    )

    # Test case 7: None cluster resources - should not raise
    _assert_resource_check(
        available_resources=None,
        resources_per_worker={"CPU": 1.0},
        num_workers=2,
        should_raise=False,
    )

    # Test case 8: Edge case with zero resources - should not raise
    _assert_resource_check(
        available_resources={"CPU": 4.0},
        resources_per_worker={"CPU": 0.0},
        num_workers=10,
        should_raise=False,
    )

    # Test case 9: Exact resource match - should not raise
    _assert_resource_check(
        available_resources={"CPU": 4.0},
        resources_per_worker={"CPU": 1.0},
        num_workers=4,  # Exactly matches 4.0 CPU available
        should_raise=False,
    )


def _make_worker(node_id, node_ip, gpu_ids=None):
    """Helper to create a Worker with minimal metadata for rank assignment tests."""
    return Worker(
        actor=None,
        metadata=ActorMetadata(
            node_id=node_id,
            node_ip=node_ip,
            hostname="dummy",
            accelerator_ids={"GPU": gpu_ids} if gpu_ids else {},
            pid=0,
        ),
        resources={"GPU": 1} if gpu_ids else {"CPU": 1},
    )


@pytest.mark.parametrize(
    "workers, starting_world_rank, world_size, replica_group_size, "
    "expected_contexts",
    [
        pytest.param(
            # 4 workers on 2 nodes, 2 GPUs each
            [
                _make_worker("node0", "10.0.0.1", ["1"]),
                _make_worker("node1", "10.0.0.2", ["1"]),
                _make_worker("node0", "10.0.0.1", ["0"]),
                _make_worker("node1", "10.0.0.2", ["0"]),
            ],
            0,
            None,
            None,
            # After sorting: node0/gpu0, node0/gpu1, node1/gpu0, node1/gpu1
            [
                DistributedContext(
                    local_rank=0,
                    local_world_size=2,
                    world_rank=0,
                    world_size=4,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=1,
                    local_world_size=2,
                    world_rank=1,
                    world_size=4,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=2,
                    world_rank=2,
                    world_size=4,
                    node_rank=1,
                ),
                DistributedContext(
                    local_rank=1,
                    local_world_size=2,
                    world_rank=3,
                    world_size=4,
                    node_rank=1,
                ),
            ],
            id="no_replica_groups",
        ),
        pytest.param(
            # 4 workers on 2 nodes — each worker is its own replica group
            [
                _make_worker("node0", "10.0.0.1", ["1"]),
                _make_worker("node1", "10.0.0.2", ["0"]),
                _make_worker("node0", "10.0.0.1", ["0"]),
                _make_worker("node1", "10.0.0.2", ["1"]),
            ],
            0,
            None,
            1,
            # After sorting: node0/gpu0, node0/gpu1, node1/gpu0, node1/gpu1
            # Each worker is its own replica group, so local_rank=0,
            # local_world_size=1, node_rank=0 for all.
            [
                DistributedContext(
                    local_rank=0,
                    local_world_size=1,
                    world_rank=0,
                    world_size=4,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=1,
                    world_rank=1,
                    world_size=4,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=1,
                    world_rank=2,
                    world_size=4,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=1,
                    world_rank=3,
                    world_size=4,
                    node_rank=0,
                ),
            ],
            id="replica_group_size_1",
        ),
        pytest.param(
            # 8 workers across 3 nodes (2-4-2 GPUs), replica_group_size=4
            [
                _make_worker("nodeA", "10.0.0.1", ["1"]),
                _make_worker("nodeB", "10.0.0.2", ["3"]),
                _make_worker("nodeA", "10.0.0.1", ["0"]),
                _make_worker("nodeB", "10.0.0.2", ["0"]),
                _make_worker("nodeC", "10.0.0.3", ["1"]),
                _make_worker("nodeB", "10.0.0.2", ["2"]),
                _make_worker("nodeB", "10.0.0.2", ["1"]),
                _make_worker("nodeC", "10.0.0.3", ["0"]),
            ],
            0,
            None,
            4,
            [
                # RG0: A/gpu0, A/gpu1, B/gpu0, B/gpu1
                DistributedContext(
                    local_rank=0,
                    local_world_size=2,
                    world_rank=0,
                    world_size=8,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=1,
                    local_world_size=2,
                    world_rank=1,
                    world_size=8,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=2,
                    world_rank=2,
                    world_size=8,
                    node_rank=1,
                ),
                DistributedContext(
                    local_rank=1,
                    local_world_size=2,
                    world_rank=3,
                    world_size=8,
                    node_rank=1,
                ),
                # RG1: B/gpu2, B/gpu3, C/gpu0, C/gpu1
                DistributedContext(
                    local_rank=0,
                    local_world_size=2,
                    world_rank=4,
                    world_size=8,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=1,
                    local_world_size=2,
                    world_rank=5,
                    world_size=8,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=2,
                    world_rank=6,
                    world_size=8,
                    node_rank=1,
                ),
                DistributedContext(
                    local_rank=1,
                    local_world_size=2,
                    world_rank=7,
                    world_size=8,
                    node_rank=1,
                ),
            ],
            id="replica_group_size_4_node_straddling",
        ),
        pytest.param(
            # Simulates replacing replica group 1 in a 4-worker setup with replica_group_size=2.
            [
                _make_worker("node0", "10.0.0.1", ["0"]),
                _make_worker("node1", "10.0.0.2", ["0"]),
            ],
            2,
            4,
            2,
            [
                DistributedContext(
                    local_rank=0,
                    local_world_size=1,
                    world_rank=2,
                    world_size=4,
                    node_rank=0,
                ),
                DistributedContext(
                    local_rank=0,
                    local_world_size=1,
                    world_rank=3,
                    world_size=4,
                    node_rank=1,
                ),
            ],
            id="replica_group_size_2_replace",
        ),
    ],
)
def test_assign_worker_ranks(
    workers,
    starting_world_rank,
    world_size,
    replica_group_size,
    expected_contexts,
):
    result = WorkerGroup._assign_worker_ranks(
        workers,
        starting_world_rank=starting_world_rank,
        world_size=world_size,
        replica_group_size=replica_group_size,
    )
    assert len(result) == len(expected_contexts)
    for i, (worker, expected) in enumerate(zip(result, expected_contexts)):
        ctx = worker.distributed_context
        assert ctx == expected, f"Worker {i}: expected {expected}, got {ctx}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
