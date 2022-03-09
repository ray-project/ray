import pytest

import ray
from ray import train
from ray.train.integrations.torch import TorchTrainer
from ray.train.examples.train_linear_example import train_func as linear_train_func

from ray.ml.integrations.torch import TorchPredictor
from ray.ml.constants import MODEL_KEY


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

    def test_torch_predict(self, ray_start_4_cpus):
        import torch

        def train_func():
            model = torch.nn.Linear(1, 1)
            train.save_checkpoint({MODEL_KEY: model})

        scaling_config = {"num_workers": 2}
        trainer = TorchTrainer(train_func=train_func, scaling_config=scaling_config)
        result = trainer.fit()

        predictor = TorchPredictor.from_checkpoint(result.checkpoint)

        predict_dataset = ray.data.range(3)
        predictions = predict_dataset.map_batches(
            lambda batch: predictor.predict(batch, dtype=torch.float),
            batch_format="pandas",
        )
        assert predictions.count() == 3

    def test_torch_predict_state_dict(self, ray_start_4_cpus):
        import torch

        def train_func():
            model = torch.nn.Linear(1, 1).state_dict()
            train.save_checkpoint({MODEL_KEY: model})

        scaling_config = {"num_workers": 2}
        trainer = TorchTrainer(train_func=train_func, scaling_config=scaling_config)
        result = trainer.fit()

        # If loading from a state dict, a model definition must be passed in.
        with pytest.raises(RuntimeError):
            TorchPredictor.from_checkpoint(result.checkpoint)

        predictor = TorchPredictor.from_checkpoint(
            result.checkpoint, model_definition=torch.nn.Linear(1, 1)
        )

        predict_dataset = ray.data.range(3)
        predictions = predict_dataset.map_batches(
            lambda batch: predictor.predict(batch, dtype=torch.float),
            batch_format="pandas",
        )
        assert predictions.count() == 3


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
