import math
import os
from unittest.mock import patch

import pytest

import ray
import ray.train as train
from ray.cluster_utils import Cluster
from ray.train.backend import (
    Backend,
    InactiveWorkerGroupError,
    TrainBackendError,
    TrainingWorkerError,
)
from ray.train.backend import BackendConfig, BackendExecutor
from ray.train.tensorflow import TensorflowConfig
from ray.train.torch import TorchConfig
from ray.train.constants import (
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    TRAIN_ENABLE_WORKER_SPREAD_ENV,
)
from ray.train.worker_group import WorkerGroup
from ray.util.placement_group import get_current_placement_group


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_4_node_4_cpu():
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=4)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_2_node_2_gpu():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=2, num_gpus=2)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_2_node_4_gpu():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=2, num_gpus=4)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def gen_execute_special(special_f):
    def execute_async_special(self, f):
        """Runs f on worker 0, special_f on other workers."""
        futures = [self.workers[0].actor._BaseWorkerMixin__execute.remote(f)]
        for worker in self.workers[1:]:
            futures.append(worker.actor._BaseWorkerMixin__execute.remote(special_f))
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


def test_start(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    with pytest.raises(InactiveWorkerGroupError):
        e.start_training(lambda: 1)
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

    e.start_training(check)
    assert e.finish_training() == ["1", "1"]


def test_shutdown(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    assert len(e.worker_group) == 2
    e.shutdown()
    with pytest.raises(InactiveWorkerGroupError):
        e.start_training(lambda: 1)


def test_train(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    e.start_training(lambda: 1)
    assert e.finish_training() == [1, 1]


def test_local_ranks(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def train_func():
        return train.local_rank()

    e.start_training(train_func)
    assert set(e.finish_training()) == {0, 1}


def test_train_failure(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    with pytest.raises(TrainBackendError):
        e.get_next_results()

    with pytest.raises(TrainBackendError):
        e.pause_reporting()

    with pytest.raises(TrainBackendError):
        e.finish_training()

    e.start_training(lambda: 1)

    with pytest.raises(TrainBackendError):
        e.start_training(lambda: 2)

    assert e.finish_training() == [1, 1]


def test_worker_failure(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def train_fail():
        ray.actor.exit_actor()

    new_execute_func = gen_execute_special(train_fail)
    with patch.object(WorkerGroup, "execute_async", new_execute_func):
        with pytest.raises(TrainingWorkerError):
            e.start_training(lambda: 1)
            e.finish_training()


def test_mismatch_checkpoint_report(ray_start_2_cpus):
    def train_func():
        if (train.world_rank()) == 0:
            train.save_checkpoint(epoch=0)
        else:
            train.report(iter=0)

    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    e.start_training(train_func)
    with pytest.raises(RuntimeError):
        e.get_next_results()


def test_tensorflow_start(ray_start_2_cpus):
    num_workers = 2
    tensorflow_config = TensorflowConfig()
    e = BackendExecutor(tensorflow_config, num_workers=num_workers)
    e.start()

    def get_tf_config():
        import json
        import os

        return json.loads(os.environ["TF_CONFIG"])

    e.start_training(get_tf_config)
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

    e.start_training(check_process_group)
    assert all(e.finish_training())

    e._backend.on_shutdown(e.worker_group, e._backend_config)

    e.start_training(check_process_group)
    assert not any(e.finish_training())


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, ["0"]),
        (2, ["0,1", "0,1"]),
        (3, ["0", "0,1", "0,1"]),
        (4, ["0,1", "0,1", "0,1", "0,1"]),
    ],
)
def test_cuda_visible_devices(ray_2_node_2_gpu, worker_results):
    config = TestConfig()

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"
    e = BackendExecutor(
        config, num_workers=num_workers, num_cpus_per_worker=0, num_gpus_per_worker=1
    )
    e.start()
    e.start_training(get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, ["0"]),
        (2, ["0", "0"]),
        (3, ["0,1", "0,1", "0,1"]),
        (4, ["0,1", "0,1", "0,1", "0,1"]),
        (5, ["0", "0,1", "0,1", "0,1", "0,1"]),
        (6, ["0", "0", "0,1", "0,1", "0,1", "0,1"]),
        (7, ["0,1", "0,1", "0,1", "0,1", "0,1", "0,1", "0,1"]),
        (8, ["0,1", "0,1", "0,1", "0,1", "0,1", "0,1", "0,1", "0,1"]),
    ],
)
def test_cuda_visible_devices_fractional(ray_2_node_2_gpu, worker_results):
    config = TestConfig()

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"
    e = BackendExecutor(
        config, num_workers=num_workers, num_cpus_per_worker=0, num_gpus_per_worker=0.5
    )
    e.start()
    e.start_training(get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [
        (1, ["0,1"]),
        (2, ["0,1,2,3", "0,1,2,3"]),
        (3, ["0,1", "0,1,2,3", "0,1,2,3"]),
        (4, ["0,1,2,3", "0,1,2,3", "0,1,2,3", "0,1,2,3"]),
    ],
)
def test_cuda_visible_devices_multiple(ray_2_node_4_gpu, worker_results):
    config = TestConfig()

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    num_workers, expected_results = worker_results

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"
    e = BackendExecutor(
        config, num_workers=num_workers, num_cpus_per_worker=0, num_gpus_per_worker=2
    )
    e.start()
    e.start_training(get_resources)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


def get_node_id_set():
    node_id_set = set()
    for actor_info in ray.state.actors().values():
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
        e.start_training(train_func)
        return e.finish_training()

    results_future = test.options(
        placement_group=placement_group,
        placement_group_capture_child_tasks=placement_group_capture_child_tasks,
    ).remote()
    results = ray.get(results_future)
    for worker_result in results:
        if placement_group_capture_child_tasks:
            assert worker_result == placement_group.id
        else:
            assert worker_result != placement_group.id


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
