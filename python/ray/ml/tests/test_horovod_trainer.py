import pytest

import ray
from ray.ml.train.integrations.horovod import HorovodTrainer
from ray.train.examples.horovod.horovod_example import train_func as hvd_train_func
from ray.train.horovod import HorovodConfig


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2])
def test_horovod(ray_start_4_cpus, num_workers):
    def train_func(config):
        result = hvd_train_func(config)
        assert len(result) == epochs
        assert result[-1] < result[0]

    num_workers = num_workers
    epochs = 2
    scaling_config = {"num_workers": num_workers}
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 64, "num_epochs": epochs}
    trainer = HorovodTrainer(
        train_loop_per_worker=train_func,
        backend_config=HorovodConfig(),
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    trainer.fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
