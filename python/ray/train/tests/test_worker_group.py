import time
from collections import defaultdict

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray.cluster_utils import Cluster
from ray.train._internal.worker_group import Worker, WorkerGroup, WorkerMetadata
from ray.util.state import list_actors


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus_and_gpus():
    address_info = ray.init(num_cpus=2, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus_and_neuron_core_accelerator():
    address_info = ray.init(num_cpus=2, resources={ray_constants.NEURON_CORES: 2})
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus_and_10kb_memory():
    address_info = ray.init(num_cpus=2, _memory=10_000)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_5_nodes_with_memory():
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=4, memory=500)
    cluster.add_node(num_cpus=4, memory=2_000)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def test_worker_creation(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    wg = WorkerGroup(num_workers=2)
    assert len(wg.workers) == 2
    time.sleep(1)
    # Make sure both CPUs are being used by the actors.
    assert "CPU" not in ray.available_resources()
    wg.shutdown()


def test_worker_creation_num_cpus(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    wg = WorkerGroup(resources_per_worker={"CPU": 2})
    time.sleep(1)
    assert len(wg.workers) == 1
    # Make sure both CPUs are being used by the actor.
    assert "CPU" not in ray.available_resources()
    wg.shutdown()


def test_worker_creation_with_memory(ray_start_5_nodes_with_memory):
    resources_per_worker = {"memory": 1_000}
    wg = WorkerGroup(num_workers=2, resources_per_worker=resources_per_worker)
    assert len(wg.workers) == 2

    nodes = ray.nodes()
    large_node = [node for node in nodes if node["Resources"]["memory"] == 2_000][0]
    large_node_id = large_node["NodeID"]

    def validate_scheduling():
        resources = ray.get_runtime_context().get_assigned_resources()
        assert resources == resources_per_worker, "Resources should include memory."

        node_id = ray.get_runtime_context().get_node_id()
        assert (
            node_id == large_node_id
        ), "Workers should be scheduled on the large node."

    wg.execute(validate_scheduling)


def test_worker_shutdown(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    wg = WorkerGroup(num_workers=2)
    time.sleep(1)
    assert "CPU" not in ray.available_resources()
    assert len(list_actors()) == 2
    wg.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2

    with pytest.raises(RuntimeError):
        wg.execute(lambda: 1)


def test_worker_restart(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    with pytest.raises(RuntimeError):
        wg.start()
    # Avoid race condition.
    time.sleep(1)
    wg.shutdown(0)
    wg.start()
    wg.execute(lambda: 1)


def test_worker_with_gpu_ids(ray_start_2_cpus_and_gpus):
    num_gpus = 2
    wg = WorkerGroup(num_workers=2, resources_per_worker={"GPU": 1})
    assert len(wg.workers) == 2
    time.sleep(1)
    assert ray_constants.GPU not in ray.available_resources()
    wg.execute(lambda: 1)
    assert len(wg.workers) == 2
    for w in wg.workers:
        resource_ids = w.metadata.resource_ids
        gpu_ids = resource_ids[ray_constants.GPU]
        for gpu_id in gpu_ids:
            assert gpu_id in [str(i) for i in range(num_gpus)]
        assert len(resource_ids[ray_constants.NEURON_CORES]) == 0


def test_worker_with_neuron_core_accelerator_ids(
    ray_start_2_cpus_and_neuron_core_accelerator,
):
    num_nc = 2
    wg = WorkerGroup(
        num_workers=2, resources_per_worker={ray_constants.NEURON_CORES: 1}
    )
    assert len(wg.workers) == 2
    time.sleep(1)
    assert ray_constants.NEURON_CORES not in ray.available_resources()
    wg.execute(lambda: 1)
    assert len(wg.workers) == 2
    for w in wg.workers:
        resource_ids = w.metadata.resource_ids
        assert len(resource_ids[ray_constants.GPU]) == 0
        neuron_core_ids = resource_ids[ray_constants.NEURON_CORES]
        for neuron_core_id in neuron_core_ids:
            assert neuron_core_id in [str(i) for i in range(num_nc)]


def test_execute_async(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    futures = wg.execute_async(lambda: 1)
    assert len(futures) == 2
    outputs = ray.get(futures)
    assert all(o == 1 for o in outputs)


def test_execute(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    outputs = wg.execute(lambda: 1)
    assert len(outputs) == 2
    assert all(o == 1 for o in outputs)


def test_execute_args(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    outputs = wg.execute(lambda x: x, 1)
    assert len(outputs) == 2
    assert all(o == 1 for o in outputs)


def test_group_workers_by_node_id(ray_start_2_cpus):
    def create_worker_group(node_ids):
        wg = WorkerGroup(num_workers=2)
        wg.workers = [
            Worker(
                actor=None,
                metadata=WorkerMetadata(
                    node_id=node_id,
                    node_ip="dummy",
                    hostname="dummy",
                    resource_ids={},
                    pid=0,
                ),
            )
            for node_id in node_ids
        ]
        return wg

    wg = create_worker_group(["2", "3", "1", "4", "2", "1", "3", "3", "4", "2"])
    wg.sort_workers_by_node_id_and_gpu_id()
    expected = ["2", "2", "2", "3", "3", "3", "1", "1", "4", "4"]
    node_ids = [w.metadata.node_id for w in wg.workers]
    assert node_ids == expected, (
        "Workers should be grouped by Node ID "
        "and follow the same original order of IDs encountered (2, 3, 1, 4)."
    )

    wg = create_worker_group(["2", "3", "1", "4", "2", "1", "3", "3", "4", "2"])
    wg.sort_workers_by_node_id_and_gpu_id(_first_node_id="1")
    expected = ["1", "1", "2", "2", "2", "3", "3", "3", "4", "4"]
    node_ids = [w.metadata.node_id for w in wg.workers]
    assert (
        node_ids == expected
    ), "Workers should be grouped by Node ID, with the first ID being 1."


def test_sort_local_workers_by_gpu_id(ray_start_2_cpus):
    def create_worker_group(pids, node_ids, gpu_ids):
        wg = WorkerGroup(num_workers=2)
        wg.workers = [
            Worker(
                actor=None,
                metadata=WorkerMetadata(
                    node_id=node_id,
                    node_ip="dummy",
                    hostname="dummy",
                    resource_ids={"GPU": gpu_id.split() if gpu_id else []},
                    pid=pid,
                ),
            )
            for pid, node_id, gpu_id in zip(pids, node_ids, gpu_ids)
        ]
        return wg

    def setup_and_check_worker_group(pids, node_ids, gpu_ids, expected_local_ranks):
        """
        Create a worker group, group workers by Node ID,
        and check local ranks assignment.

        Args:
            pids: List of unique process IDs.
            node_ids: List of Node IDs corresponding to each PID.
            gpu_ids: List of GPU IDs or None for each PID.
            expected_local_ranks: Dictionary mapping PID to the
                expected local rank.
        """
        wg = create_worker_group(pids=pids, node_ids=node_ids, gpu_ids=gpu_ids)
        wg.sort_workers_by_node_id_and_gpu_id()

        # Build local ranks according to the logics in
        # `BackendExecutor._create_rank_world_size_mappings()`
        node_id_dict = defaultdict(int)
        local_ranks_map = defaultdict(int)
        for w in wg.workers:
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


def test_execute_single(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)

    def f():
        import os

        os.environ["TEST"] = "1"

    wg.execute_single(1, f)

    def check():
        import os

        return os.environ.get("TEST", "0")

    assert wg.execute(check) == ["0", "1"]


def test_bad_resources(ray_start_2_cpus):
    with pytest.raises(ValueError):
        WorkerGroup(num_workers=-1)

    with pytest.raises(ValueError):
        WorkerGroup(resources_per_worker={"CPU": -1})

    with pytest.raises(ValueError):
        WorkerGroup(resources_per_worker={"GPU": -1})

    with pytest.raises(ValueError):
        WorkerGroup(resources_per_worker={"memory": -1})


def test_placement_group(ray_start_2_cpus):
    """Tests that workers can be removed and added to a placement group."""
    num_workers = 2
    bundle = {"CPU": 1}
    bundles = [bundle.copy() for _ in range(num_workers)]
    placement_group = ray.util.placement_group(bundles)
    wg = WorkerGroup(num_workers=num_workers, placement_group=placement_group)
    wg.remove_workers([0])
    wg.add_workers(1)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
