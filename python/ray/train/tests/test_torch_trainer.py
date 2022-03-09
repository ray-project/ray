import pytest

import ray
from ray.train.integrations.torch import TorchTrainer
from ray.train.examples.train_linear_example import train_func as linear_train_func


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestTorchTrainer:
    @pytest.mark.parametrize("num_workers", [1, 2])
    def test_torch_linear(self, ray_start_4_cpus, num_workers):
        def train_func(config):
            result = linear_train_func(config)
            assert len(result) == epochs
            assert result[-1]["loss"] < result[0]["loss"]

        num_workers = num_workers
        epochs = 3
        scaling_config = {"num_workers": num_workers}
        config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
        trainer = TorchTrainer(
            train_func=train_func,
            train_func_config=config,
            scaling_config=scaling_config,
        )
        trainer.fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
