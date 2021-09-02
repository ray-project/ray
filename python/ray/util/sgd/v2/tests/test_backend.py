import os

import pytest
from unittest.mock import patch

import ray
from ray.cluster_utils import Cluster
from ray.util.sgd import v2 as sgd
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor
from ray.util.sgd.v2.backends.tensorflow import TensorflowConfig
from ray.util.sgd.v2.worker_group import WorkerGroup
from ray.util.sgd.v2.backends.torch import TorchConfig

from ray.util.sgd.v2.backends.backend import Backend, \
    InactiveWorkerGroupError, SGDBackendError, TrainingWorkerError


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


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
        futures = [self.workers[0].execute.remote(f)]
        for worker in self.workers[1:]:
            futures.append(worker.execute.remote(special_f))
        return futures

    return execute_async_special


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TestConfig):
        pass


def test_start(ray_start_2_cpus, tmp_path):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    with pytest.raises(InactiveWorkerGroupError):
        e.start_training(lambda: 1, run_dir=tmp_path)
    e.start()
    assert len(e.worker_group) == 2


def test_initialization_hook(ray_start_2_cpus, tmp_path):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)

    def init_hook():
        import os
        os.environ["TEST"] = "1"

    e.start(initialization_hook=init_hook)

    def check():
        import os
        return os.getenv("TEST", "0")

    e.start_training(check, run_dir=tmp_path)
    assert e.finish_training() == ["1", "1"]


def test_shutdown(ray_start_2_cpus, tmp_path):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    assert len(e.worker_group) == 2
    e.shutdown()
    with pytest.raises(InactiveWorkerGroupError):
        e.start_training(lambda: 1, run_dir=tmp_path)


def test_train(ray_start_2_cpus, tmp_path):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    e.start_training(lambda: 1, run_dir=tmp_path)
    assert e.finish_training() == [1, 1]


def test_train_failure(ray_start_2_cpus, tmp_path):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    with pytest.raises(SGDBackendError):
        e.fetch_next_result()

    with pytest.raises(SGDBackendError):
        e.finish_training()

    e.start_training(lambda: 1, run_dir=tmp_path)

    with pytest.raises(SGDBackendError):
        e.start_training(lambda: 2, run_dir=tmp_path)

    assert e.finish_training() == [1, 1]


def test_worker_failure(ray_start_2_cpus, tmp_path):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def train_fail():
        ray.actor.exit_actor()

    new_execute_func = gen_execute_special(train_fail)
    with patch.object(WorkerGroup, "execute_async", new_execute_func):
        with pytest.raises(TrainingWorkerError):
            e.start_training(lambda: 1, run_dir=tmp_path)
            e.finish_training()


def test_no_exhaust(ray_start_2_cpus, tmp_path):
    """Tests if training can finish even if queue is not exhausted."""

    def train():
        for _ in range(2):
            sgd.report(loss=1)
        return 2

    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    e.start_training(train, run_dir=tmp_path)
    output = e.finish_training()

    assert output == [2, 2]


def test_checkpoint(ray_start_2_cpus, tmp_path):
    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)

    config = TestConfig()
    e = BackendExecutor(config, num_workers=1)
    e.start()

    e.start_training(train, run_dir=tmp_path)
    e.finish_training()

    assert e.latest_checkpoint is not None
    assert e.latest_checkpoint["epoch"] == 1


def test_persisted_checkpoint(ray_start_2_cpus, tmp_path):
    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)

    config = TestConfig()
    e = BackendExecutor(config)
    e.start()
    e.start_training(train, run_dir=tmp_path)
    e.finish_training()

    assert e.latest_checkpoint_id == 2
    assert e.latest_checkpoint is not None
    assert e.latest_checkpoint["epoch"] == 1
    assert e.latest_checkpoint_path is not None

    assert os.path.exists(e.latest_checkpoint_path)

    def validate():
        checkpoint = sgd.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint["epoch"] == 1

    e2 = BackendExecutor(config)
    e2.start()
    e2.start_training(
        validate, checkpoint=e.latest_checkpoint_path, run_dir=tmp_path)
    e2.finish_training()


def test_persisted_checkpoint_id(ray_start_2_cpus, tmp_path):
    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)

    config = TestConfig()
    e = BackendExecutor(config)
    e.start()
    e.start_training(train, run_dir=tmp_path, latest_checkpoint_id=100)
    e.finish_training()

    assert e.latest_checkpoint_id == 102
    assert e.latest_checkpoint is not None
    assert e.latest_checkpoint["epoch"] == 1
    assert e.latest_checkpoint_path is not None

    assert os.path.exists(e.latest_checkpoint_path)


def test_mismatch_checkpoint_report(ray_start_2_cpus, tmp_path):
    def train():
        if (sgd.world_rank()) == 0:
            sgd.save_checkpoint(epoch=0)
        else:
            sgd.report(iter=0)

    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    e.start_training(train, run_dir=tmp_path)
    with pytest.raises(RuntimeError):
        e.finish_training()


def test_tensorflow_start(ray_start_2_cpus, tmp_path):
    num_workers = 2
    tensorflow_config = TensorflowConfig()
    e = BackendExecutor(tensorflow_config, num_workers=num_workers)
    e.start()

    def get_tf_config():
        import json
        import os
        return json.loads(os.environ["TF_CONFIG"])

    e.start_training(get_tf_config, run_dir=tmp_path)
    results = e.finish_training()
    assert len(results) == num_workers

    workers = [result["cluster"]["worker"] for result in results]
    assert all(worker == workers[0] for worker in workers)

    indexes = [result["task"]["index"] for result in results]
    assert len(set(indexes)) == num_workers


@pytest.mark.parametrize("init_method", ["env", "tcp"])
def test_torch_start_shutdown(ray_start_2_cpus, init_method, tmp_path):
    torch_config = TorchConfig(backend="gloo", init_method=init_method)
    e = BackendExecutor(torch_config, num_workers=2)
    e.start()

    def check_process_group():
        import torch
        return torch.distributed.is_initialized(
        ) and torch.distributed.get_world_size() == 2

    e.start_training(check_process_group, run_dir=tmp_path)
    assert all(e.finish_training())

    e._backend.on_shutdown(e.worker_group, e._backend_config)

    e.start_training(check_process_group, run_dir=tmp_path)
    assert not any(e.finish_training())


@pytest.mark.parametrize("worker_results", [(1, ["0"]), (2, ["0,1", "0,1"]),
                                            (3, ["0", "0,1", "0,1"]),
                                            (4, ["0,1", "0,1", "0,1", "0,1"])])
def test_cuda_visible_devices(ray_2_node_2_gpu, worker_results, tmp_path):
    config = TestConfig()

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    num_workers, expected_results = worker_results

    e = BackendExecutor(
        config,
        num_workers=num_workers,
        num_cpus_per_worker=0,
        num_gpus_per_worker=1)
    e.start()
    e.start_training(get_resources, tmp_path)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize(
    "worker_results",
    [(1, ["0"]), (2, ["0", "0"]), (3, ["0,1", "0,1", "0,1"]),
     (4, ["0,1", "0,1", "0,1", "0,1"]), (5, ["0", "0,1", "0,1", "0,1", "0,1"]),
     (6, ["0", "0", "0,1", "0,1", "0,1", "0,1"]),
     (7, ["0,1", "0,1", "0,1", "0,1", "0,1", "0,1", "0,1"]),
     (8, ["0,1", "0,1", "0,1", "0,1", "0,1", "0,1", "0,1", "0,1"])])
def test_cuda_visible_devices_fractional(ray_2_node_2_gpu, worker_results,
                                         tmp_path):
    config = TestConfig()

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    num_workers, expected_results = worker_results

    e = BackendExecutor(
        config,
        num_workers=num_workers,
        num_cpus_per_worker=0,
        num_gpus_per_worker=0.5)
    e.start()
    e.start_training(get_resources, tmp_path)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


@pytest.mark.parametrize("worker_results",
                         [(1, ["0,1"]), (2, ["0,1,2,3", "0,1,2,3"]),
                          (3, ["0,1", "0,1,2,3", "0,1,2,3"]),
                          (4, ["0,1,2,3", "0,1,2,3", "0,1,2,3", "0,1,2,3"])])
def test_cuda_visible_devices_multiple(ray_2_node_4_gpu, worker_results,
                                       tmp_path):
    config = TestConfig()

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    num_workers, expected_results = worker_results

    e = BackendExecutor(
        config,
        num_workers=num_workers,
        num_cpus_per_worker=0,
        num_gpus_per_worker=2)
    e.start()
    e.start_training(get_resources, run_dir=tmp_path)
    results = e.finish_training()
    results.sort()
    assert results == expected_results


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
