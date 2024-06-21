import collections
import os
import time

import pytest

import ray
from ray.train.v2._internal.constants import (
    ENV_VARS_TO_PROPAGATE,
    MAX_CONSECUTIVE_HEALTH_CHECK_MISSES_ENV_VAR,
)
from ray.train.v2._internal.exceptions import (
    WorkerHealthCheckFailedError,
    WorkerHealthCheckMissedError,
)
from ray.train.v2._internal.execution.context import get_train_context
from ray.train.v2._internal.execution.worker_group import (
    ActorMetadata,
    RayTrainWorker,
    Worker,
    WorkerGroup,
)
from ray.train.v2.api.config import RunConfig


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_poll_status_running(ray_start_4_cpus):
    wg = WorkerGroup()
    wg.start(num_workers=4, resources_per_worker={"CPU": 1})
    wg.run_train_fn(lambda: time.sleep(60))
    status = wg.poll_status()
    wg.shutdown()

    assert status.num_workers == 4
    assert not status.finished
    assert not status.errors


def test_poll_status_finished(ray_start_4_cpus):
    wg = WorkerGroup()
    wg.start(num_workers=4, resources_per_worker={"CPU": 1})
    wg.run_train_fn(lambda: "done")

    # Wait for the workers to finish the training fn before polling.
    # Otherwise, the poll_status call may return before the workers finish.
    ray.wait(wg._get_train_fn_tasks(), num_returns=len(wg))

    status = wg.poll_status()
    wg.shutdown()

    assert status.num_workers == 4
    assert status.finished
    assert not status.errors


@pytest.mark.parametrize("training_failure", [True, False])
@pytest.mark.parametrize("poll_failure", [True, False])
def test_poll_status_failures(
    ray_start_4_cpus, monkeypatch, training_failure, poll_failure
):
    def train_fn():
        if training_failure:
            raise RuntimeError("train error")

    def patched_poll_status(worker_self):
        if poll_failure:
            raise RuntimeError("poll error")

    monkeypatch.setattr(RayTrainWorker, "poll_status", patched_poll_status)

    wg = WorkerGroup()
    wg.start(num_workers=4, resources_per_worker={"CPU": 1})
    wg.run_train_fn(train_fn)
    ray.wait(wg._get_train_fn_tasks(), num_returns=len(wg))

    status = wg.poll_status()
    wg.shutdown()

    assert status.num_workers == 4
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


@pytest.mark.parametrize("max_consecutive_misses", [1, 3])
def test_poll_status_healthcheck_miss_handling(
    ray_start_4_cpus, monkeypatch, max_consecutive_misses
):
    monkeypatch.setenv(
        MAX_CONSECUTIVE_HEALTH_CHECK_MISSES_ENV_VAR, str(max_consecutive_misses)
    )

    def hanging_poll_status(worker_self):
        time.sleep(60)

    monkeypatch.setattr(RayTrainWorker, "poll_status", hanging_poll_status)

    wg = WorkerGroup()

    # Try 2x to ensure that shutdown clears the health-check miss count.
    for _ in range(2):
        wg.start(num_workers=4, resources_per_worker={"CPU": 1})
        wg.run_train_fn(lambda: None)

        for _ in range(max_consecutive_misses - 1):
            status = wg.poll_status(timeout=0.01)
            assert not status.errors

        status = wg.poll_status(timeout=0.01)

        assert len(status.errors) == 4
        assert all(
            [
                isinstance(error, WorkerHealthCheckMissedError)
                for error in status.errors.values()
            ]
        )

        wg.shutdown()


def test_group_workers_by_ip(ray_start_4_cpus):
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


def test_local_rank_assignment(ray_start_4_cpus):
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


def test_setup_worker_group(ray_start_4_cpus, tmp_path):
    num_workers = 4
    worker_group = WorkerGroup(
        run_config=RunConfig(name="test", storage_path=str(tmp_path))
    )
    worker_group.start(num_workers=num_workers, resources_per_worker={"CPU": 1})

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
def test_flush_worker_result_queue(ray_start_4_cpus, queue_backlog_length):
    """Make sure that the result queue is fully consumed before the worker exits."""
    wg = WorkerGroup()
    wg.start(num_workers=4, resources_per_worker={"CPU": 1})

    def populate_result_queue():
        get_train_context().get_result_queue().put("result")

    for _ in range(queue_backlog_length):
        wg.execute(populate_result_queue)

    wg.run_train_fn(lambda: None)
    status = wg.poll_status()

    for _ in range(queue_backlog_length - 1):
        assert not status.finished
        status = wg.poll_status()

    assert status.finished

    wg.shutdown()


def test_env_var_propagation(ray_start_4_cpus, monkeypatch):
    """Ray Train should automatically propagate some environment variables
    from the driver to the workers."""
    test_env_var = ENV_VARS_TO_PROPAGATE[0]
    monkeypatch.setenv(test_env_var, "1")
    w = WorkerGroup()
    w.start(num_workers=4, resources_per_worker={"CPU": 1})
    env_vars = w.execute(lambda: os.environ.get(test_env_var))
    w.shutdown()

    assert env_vars == ["1"] * 4


if __name__ == "__main__":
    pytest.main(["-v", __file__])
