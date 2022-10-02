import pytest
import ray
from ray.tune import register_trainable


def _one_iter(config):
    return 1


def _inf_iter(config):
    while True:
        yield 1


def _failing(config):
    raise RuntimeError("Failing")


@pytest.fixture
def ray_start_local():
    address_info = ray.init(
        local_mode=True, num_cpus=4, num_gpus=2, include_dashboard=False
    )
    register_trainable("_one_iter", _one_iter)
    register_trainable("_inf_iter", _inf_iter)
    register_trainable("_failing", _failing)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    register_trainable("_one_iter", _one_iter)
    register_trainable("_inf_iter", _inf_iter)
    register_trainable("_failing", _failing)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
