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
        next(e.execute(lambda: 1))
    e.start()
    assert len(e.worker_group) == 2


def test_shutdown(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()
    assert len(e.worker_group) == 2
    e.shutdown()
    with pytest.raises(RuntimeError):
        next(e.execute(lambda: 1))


def test_execute(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    for _ in e.execute(lambda: 1):
        continue

    assert e.return_values == [1, 1]


def test_execute_worker_failure(ray_start_2_cpus):
    config = BackendConfig()
    e = BackendExecutor(config, num_workers=2)
    e.start()

    def train_fail():
        ray.actor.exit_actor()

    new_execute_func = gen_execute_special(train_fail)
    with patch.object(WorkerGroup, "execute_async", new_execute_func):
        with pytest.raises(RuntimeError):
            for _ in e.execute(lambda: 1):
                continue


def execute_with_results(train_fn):
    @ray.remote
    class Queue:
        def __init__(self):
            self.vals = [1, 2, 3, 4, 5, 6]

        def size(self):
            return len(self.vals)

        def pop(self):
            return self.vals.pop(0)

    q = Queue.remote()

    def fetch_next():
        if ray.get(q.size.remote()) > 0:
            return ray.get(q.pop.remote())
        else:
            return None

    def fetch_all():
        r = []
        n = fetch_next()
        while n is not None:
            r.append(n)
            n = fetch_next()
        return r

    ray.util.sgd.v2.backends.backend.fetch_next = fetch_next
    ray.util.sgd.v2.backends.backend.fetch_all = fetch_all

    config = BackendConfig()
    e = BackendExecutor(config, num_workers=1)
    e.start()

    generator = e.execute(train_fn)

    result = next(generator)
    assert result == [1]

    result = next(generator)
    assert result == [2]

    result = next(generator)
    assert result == [3]

    for _ in generator:
        continue

    assert e.return_values == ["x"]


def test_execute_with_results(ray_start_2_cpus):
    execute_with_results(lambda: "x")


def test_execute_slow_worker(ray_start_2_cpus):
    def slow_train():
        import time
        time.sleep(5)
        return "x"

    execute_with_results(slow_train)


@pytest.mark.parametrize("init_method", ["env", "tcp"])
def test_torch_start(ray_start_2_cpus, init_method):
    torch_config = TorchConfig(init_method=init_method)
    e = TorchExecutor(torch_config, num_workers=2)

    def train():
        import torch
        return torch.dist.get_world_size() == 2

    e.start()
    for _ in e.execute(train):
        continue

    assert all(e.return_values)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
