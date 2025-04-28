import math
import os
import sys
import tempfile
import time
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators.neuron import NEURON_RT_VISIBLE_CORES_ENV_VAR
from ray import train
from ray.air._internal.util import StartTraceback

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa
from ray.train import DataConfig
from ray.train._internal.backend_executor import (
    BackendExecutor,
    InactiveWorkerGroupError,
    TrainBackendError,
    TrainingWorkerError,
)
from ray.train._internal.storage import StorageContext
from ray.train._internal.worker_group import WorkerGroup, WorkerMetadata
from ray.train.backend import Backend, BackendConfig
from ray.train.constants import (
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    ENABLE_SHARE_NEURON_CORES_ACCELERATOR_ENV,
    TRAIN_ENABLE_WORKER_SPREAD_ENV,
)
from ray.train.torch import TorchConfig
from ray.util.placement_group import get_current_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


def _start_training(backend_executor: BackendExecutor, fn):
    storage = StorageContext(
        storage_path=tempfile.mkdtemp(),
        experiment_dir_name="exp_name",
        trial_dir_name="trial_name",
    )
    backend_executor.start_training(
        train_func=fn,
        datasets={},
        metadata={},
        data_config=DataConfig(),
        storage=storage,
    )


def gen_execute_special(special_f):
    def execute_async_special(self, f):
        """Runs f on worker 0, special_f on other workers."""
        futures = [
            self.workers[0]
            .actor._RayTrainWorker__execute.options(name=f.__name__)
            .remote(f)
        ]
        for worker in self.workers[1:]:
            futures.append(
                worker.actor._RayTrainWorker__execute.options(
                    name=special_f.__name__
                ).remote(special_f)
            )
        return futures

    return execute_async_special


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass


original_add_workers = WorkerGroup.add_workers


def mock_add_workers(self, num_workers):
    original_add_workers(self, num_workers)
    for i, worker in enumerate(self.workers):
        metadata = WorkerMetadata(
            node_id=str(i % 2),
            node_ip=str(i % 2),
            hostname=0,
            resource_ids={"GPU": ["0"]},
            pid=0,
        )
        worker.metadata = metadata


def mock_add_workers_to_nodes_with_same_ip(self, num_workers):
    original_add_workers(self, num_workers)
    for i, worker in enumerate(self.workers):
        metadata = WorkerMetadata(
            node_id=str(i % 2),
            node_ip=0,
            hostname=0,
            resource_ids={"GPU": ["0"]},
            pid=0,
        )
        worker.metadata = metadata


def test_start(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    with pytest.raises(InactiveWorkerGroupError):
        _start_training(e, lambda: 1)
    e.start()
    assert len(e.worker_group) == 2


def test_initialization_hook(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)

    def init_hook():
        import os

        os.environ["TEST"] = "1"

    e.start(initialization_hook=init_hook)

    def check():
        import os

        return os.getenv("TEST", "0")

    _start_training(e, check)
    assert e.finish_training() == ["1", "1"]


def test_shutdown(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    assert len(e.worker_group) == 2
    e.shutdown()
    with pytest.raises(InactiveWorkerGroupError):
        _start_training(e, lambda: 1)


def test_train(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    _start_training(e, lambda: 1)
    assert e.finish_training() == [1, 1]


def test_local_ranks(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def train_func():
        return train.get_context().get_local_rank()

    _start_training(e, train_func)
    assert set(e.finish_training()) == {0, 1}


def test_local_ranks_with_same_ip_nodes(ray_2_node_2_cpu):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=4)
    e.start()

    def train_func():
        return train.get_context().get_local_rank()

    _start_training(e, train_func)
    assert list(e.finish_training()) == [0, 1, 0, 1]


def test_local_world_size(ray_2_node_2_cpu):
    config = TestConfig()
    with patch.object(WorkerGroup, "add_workers", mock_add_workers):
        e = BackendExecutor(config, num_workers=3)
        e.start()

        def train_func():
            return train.get_context().get_local_world_size()

        _start_training(e, train_func)
        assert list(e.finish_training()) == [2, 2, 1]


def test_local_world_size_with_same_ip_nodes(ray_2_node_2_cpu):
    config = TestConfig()
    with patch.object(
        WorkerGroup, "add_workers", mock_add_workers_to_nodes_with_same_ip
    ):
        e = BackendExecutor(config, num_workers=3)
        e.start()

        def train_func():
            return train.get_context().get_local_world_size()

        _start_training(e, train_func)
        assert list(e.finish_training()) == [2, 2, 1]


def test_node_ranks(ray_2_node_2_cpu):
    config = TestConfig()
    with patch.object(WorkerGroup, "add_workers", mock_add_workers):
        e = BackendExecutor(config, num_workers=3)
        e.start()

        def train_func():
            return train.get_context().get_node_rank()

        _start_training(e, train_func)
        assert list(e.finish_training()) == [0, 0, 1]


def test_node_ranks_with_same_ip_nodes(ray_2_node_2_cpu):
    config = TestConfig()
    with patch.object(
        WorkerGroup, "add_workers", mock_add_workers_to_nodes_with_same_ip
    ):
        e = BackendExecutor(config, num_workers=3)
        e.start()

        def train_func():
            return train.get_context().get_node_rank()

        _start_training(e, train_func)
        assert list(e.finish_training()) == [0, 0, 1]


def test_train_failure(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    with pytest.raises(StartTraceback) as exc:
        e.get_next_results()
    assert isinstance(exc.value.__cause__, TrainBackendError)

    with pytest.raises(StartTraceback) as exc:
        e.pause_reporting()
    assert isinstance(exc.value.__cause__, TrainBackendError)

    with pytest.raises(StartTraceback) as exc:
        e.finish_training()
    assert isinstance(exc.value.__cause__, TrainBackendError)

    _start_training(e, lambda: 1)

    with pytest.raises(StartTraceback) as exc:
        _start_training(e, lambda: 2)
    assert isinstance(exc.value.__cause__, TrainBackendError)

    assert e.finish_training() == [1, 1]


def test_single_worker_user_failure(ray_start_2_cpus):
    """Tests if training fails immediately if one worker raises an Exception
    while executing the user training code."""
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def single_worker_user_failure():
        if train.get_context().get_world_rank() == 0:
            raise RuntimeError
        else:
            time.sleep(1000000)

    _start_training(e, single_worker_user_failure)

    with pytest.raises(StartTraceback) as exc:
        e.get_next_results()
    assert isinstance(exc.value.__cause__, RuntimeError)


def test_single_worker_actor_failure(ray_start_2_cpus):
    """Tests is training fails immediately if one worker actor dies."""
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def single_worker_actor_failure():
        if train.get_context().get_world_rank() == 0:
            # Simulate actor failure
            os._exit(1)
        else:
            time.sleep(1000)

    _start_training(e, single_worker_actor_failure)

    with pytest.raises(TrainingWorkerError):
        e.get_next_results()


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="tensorflow is not supported in python 3.12+"
)
def test_tensorflow_start(ray_start_2_cpus):
    from ray.train.tensorflow import TensorflowConfig

    num_workers = 2
    tensorflow_config = TensorflowConfig()
    e = BackendExecutor(tensorflow_config, num_workers=num_workers)
    e.start()

    def get_tf_config():
        import json
        import os

        return json.loads(os.environ["TF_CONFIG"])

    _start_training(e, get_tf_config)
    results = e.finish_training()
    assert len(results) == num_workers

    workers = [result["cluster"]["worker"] for result in results]
    assert all(worker == workers[0] for worker in workers)

    indexes = [result["task"]["index"] for result in results]
    assert len(set(indexes)) == num_workers


@pytest.mark.parametrize("init_method", ["env", "tcp"])
def test_torch_start_shutdown(ray_start_2_cpus, init_method):
    torch_config = TorchConfig(backend="gloo", init_method=init_method)
    e = BackendExecutor(torch_config, num_workers=2)
    e.start()

    def check_process_group():
        import torch

        return (
            torch.distributed.is_initialized()
            and torch.distributed.get_world_size() == 2
        )

    _start_training(e, check_process_group)
    assert all(e.finish_training())

    e._backend.on_shutdown(e.worker_group, e._backend_config)

    _start_training(e, check_process_group)
    assert not any(e.finish_training())


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, [[0]]),
        (2, [[0, 1]] * 2),
        (3, [[0]] + [[0, 1]] * 2),
        (4, [[0, 1]] * 4),
    ],
)
def test_cuda_visible_devices(ray_2_node_2_gpu, worker_results):
    config = TestConfig()

    def get_resources():
        cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]
        # Sort the cuda visible devices to have exact match with expected result.
        sorted_devices = [
            int(device) for device in sorted(cuda_visible_devices.split(","))
        ]
        return sorted_devices

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"
    e = BackendExecutor(
        config, num_workers=num_workers, resources_per_worker={"GPU": 1}
    )
    e.start()
    _start_training(e, get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, [[0]]),
        (
            2,
            [[0]] * 2,
        ),
        (3, [[0, 1]] * 3),
        (4, [[0, 1]] * 4),
        (5, [[0]] + [[0, 1]] * 4),
        (6, [[0]] * 2 + [[0, 1]] * 4),
        (7, [[0, 1]] * 7),
        (8, [[0, 1]] * 8),
    ],
)
def test_cuda_visible_devices_fractional(ray_2_node_2_gpu, worker_results):
    config = TestConfig()

    if worker_results[0] != len(worker_results[1]):
        raise ValueError(
            "Invalid test parameter. Length of expected result should "
            "match number of workers."
        )

    def get_resources():
        cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]
        # Sort the cuda visible devices to have exact match with expected result.
        sorted_devices = [
            int(device) for device in sorted(cuda_visible_devices.split(","))
        ]
        return sorted_devices

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"
    e = BackendExecutor(
        config, num_workers=num_workers, resources_per_worker={"GPU": 0.5}
    )
    e.start()
    _start_training(e, get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, [[0, 1]]),
        (2, [[0, 1, 2, 3]] * 2),
        (3, [[0, 1]] + [[0, 1, 2, 3]] * 2),
        (4, [[0, 1, 2, 3]] * 4),
    ],
)
def test_cuda_visible_devices_multiple(ray_2_node_4_gpu, worker_results):
    config = TestConfig()

    def get_resources():
        cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]
        # Sort the cuda visible devices to have exact match with expected result.
        sorted_devices = [
            int(device) for device in sorted(cuda_visible_devices.split(","))
        ]
        return sorted_devices

    if worker_results[0] != len(worker_results[1]):
        raise ValueError(
            "Invalid test parameter. Length of expected result should "
            "match number of workers."
        )

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"
    e = BackendExecutor(
        config, num_workers=num_workers, resources_per_worker={"GPU": 2}
    )
    e.start()
    _start_training(e, get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, [[0]]),
        (2, [[0, 1]] * 2),
        (3, [[0]] + [[0, 1]] * 2),
        (4, [[0, 1]] * 4),
    ],
)
def test_neuron_core_accelerator_ids(ray_2_node_2_neuron_cores, worker_results):
    config = TestConfig()

    def get_resources():
        neuron_resource_ids = os.environ[NEURON_RT_VISIBLE_CORES_ENV_VAR]
        # Sort the runtime ids to have exact match with expected result.
        sorted_devices = [
            int(device) for device in sorted(neuron_resource_ids.split(","))
        ]
        return sorted_devices

    num_workers, expected_results = worker_results
    # sharing enabled by default
    os.environ.pop(ENABLE_SHARE_NEURON_CORES_ACCELERATOR_ENV, None)
    e = BackendExecutor(
        config,
        num_workers=num_workers,
        resources_per_worker={"neuron_cores": 1},
    )
    e.start()
    _start_training(e, get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, [[0]]),
        (2, [[0]] + [[1]]),
        (3, [[0]] * 2 + [[1]]),
        (4, [[0]] * 2 + [[1]] * 2),
    ],
)
def test_neuron_core_accelerator_ids_sharing_disabled(
    ray_2_node_2_neuron_cores, worker_results
):
    config = TestConfig()

    def get_resources():
        neuron_resource_ids = os.environ[NEURON_RT_VISIBLE_CORES_ENV_VAR]
        # Sort the runtime ids to have exact match with expected result.
        sorted_devices = [
            int(device) for device in sorted(neuron_resource_ids.split(","))
        ]
        return sorted_devices

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_NEURON_CORES_ACCELERATOR_ENV] = "0"
    e = BackendExecutor(
        config,
        num_workers=num_workers,
        resources_per_worker={"neuron_cores": 1},
    )
    e.start()
    _start_training(e, get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


def get_node_id_set():
    node_id_set = set()
    for actor_info in ray._private.state.actors().values():
        node_id = actor_info["Address"]["NodeID"]
        node_id_set.add(node_id)
    return node_id_set


@pytest.mark.parametrize("num_workers", [3, 4, 5])
def test_placement_group_pack(ray_4_node_4_cpu, num_workers):
    """Tests that workers are packed on nodes."""
    config = TestConfig()
    e = BackendExecutor(config, num_workers=num_workers)
    e.start()
    node_id_set = get_node_id_set()
    assert len(node_id_set) == math.ceil(num_workers / 4)


@pytest.mark.parametrize("num_workers", [3, 4, 5])
def test_placement_group_spread(ray_4_node_4_cpu, num_workers):
    """Tests that workers are spread across nodes."""
    os.environ[TRAIN_ENABLE_WORKER_SPREAD_ENV] = "1"
    config = TestConfig()
    e = BackendExecutor(config, num_workers=num_workers)
    e.start()
    node_id_set = get_node_id_set()
    assert len(node_id_set) == min(num_workers, 4)


@pytest.mark.parametrize("placement_group_capture_child_tasks", [True, False])
def test_placement_group_parent(ray_4_node_4_cpu, placement_group_capture_child_tasks):
    """Tests that parent placement group will be used."""
    num_workers = 2
    bundle = {"CPU": 1}
    bundles = [bundle.copy() for _ in range(num_workers + 1)]
    placement_group = ray.util.placement_group(bundles)

    def train_func():
        return get_current_placement_group().id

    @ray.remote
    def test():
        config = TestConfig()
        e = BackendExecutor(config, num_workers=2)
        e.start()
        _start_training(e, train_func)
        return e.finish_training()

    results_future = test.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group,
            placement_group_capture_child_tasks=placement_group_capture_child_tasks,
        ),
    ).remote()
    results = ray.get(results_future)
    for worker_result in results:
        if placement_group_capture_child_tasks:
            assert worker_result == placement_group.id
        else:
            assert worker_result != placement_group.id


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
