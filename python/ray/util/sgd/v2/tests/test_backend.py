import pytest
from unittest.mock import patch

import ray
from ray.util.sgd import v2 as sgd
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor
from ray.util.sgd.v2.backends.tensorflow import TensorflowConfig
from ray.util.sgd.v2.worker_group import WorkerGroup
from ray.util.sgd.v2.backends.torch import TorchConfig

from ray.util.sgd.v2.backends.backend import BackendInterface, \
    InactiveWorkerGroupError, SGDBackendError


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def gen_execute_special(special_f):
    def execute_async_special(self, f):
        """Runs f on worker 0, special_f on worker 1."""
        assert len(self.workers) == 2
        return [
            self.workers[0].execute.remote(f),
            self.workers[0].execute.remote(special_f)
        ]

    return execute_async_special


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(BackendInterface):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TestConfig):
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


def test_train_failure(ray_start_2_cpus):
    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    with pytest.raises(SGDBackendError):
        e.fetch_next_result()

    with pytest.raises(SGDBackendError):
        e.finish_training()

    e.start_training(lambda: 1)

    with pytest.raises(SGDBackendError):
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
        with pytest.raises(RuntimeError):
            e.start_training(lambda: 1)
            e.finish_training()


def test_no_exhaust(ray_start_2_cpus):
    """Tests if training can finish even if queue is not exhausted."""

    def train():
        for _ in range(2):
            sgd.report(loss=1)
        return 2

    config = TestConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    e.start_training(train)
    output = e.finish_training()

    assert output == [2, 2]


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
        return torch.distributed.is_initialized(
        ) and torch.distributed.get_world_size() == 2

    e.start_training(check_process_group)
    assert all(e.finish_training())

    e._backend.on_shutdown(e.worker_group, e._backend_config)

    e.start_training(check_process_group)
    assert not any(e.finish_training())


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
