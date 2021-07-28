import pytest
from unittest.mock import patch

import ray
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor
from ray.util.sgd.v2.worker_group import WorkerGroup
from ray.util.sgd.v2.backends.torch import TorchConfig, TorchExecutor


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


def test_start(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    with pytest.raises(RuntimeError):
        e.run(lambda: 1)
    e.start()
    assert len(e.worker_group) == 2


def test_shutdown(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    assert len(e.worker_group) == 2
    e.shutdown()
    with pytest.raises(RuntimeError):
        e.run(lambda: 1)


def test_execute(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    assert e.run(lambda: 1) == [1, 1]


def test_execute_worker_failure(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def train_fail():
        ray.actor.exit_actor()

    new_execute_func = gen_execute_special(train_fail)
    with patch.object(WorkerGroup, "execute_async", new_execute_func):
        with pytest.raises(RuntimeError):
            e.run(lambda: 1)


@pytest.mark.parametrize("init_method", ["env", "tcp"])
def test_torch_start_shutdown(ray_start_2_cpus, init_method):
    torch_config = TorchConfig(init_method=init_method)
    e = TorchExecutor(torch_config, num_workers=2)

    def check_process_group():
        import torch
        return torch.distributed.is_initialized(
        ) and torch.distributed.get_world_size() == 2

    assert all(e.run(check_process_group))

    e._backend.on_shutdown(e.worker_group, e._backend_config)

    assert not any(e.run(check_process_group))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
