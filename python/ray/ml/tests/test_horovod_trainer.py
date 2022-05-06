import pytest
import torch
import torch.nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import transforms

import ray
from ray.ml.examples.horovod.horovod_pytorch_example import (
    train_func as hvd_train_func,
    Net,
)
from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.train.integrations.horovod import HorovodTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def run_image_prediction(model: torch.nn.Module, images: torch.Tensor) -> torch.Tensor:
    model.eval()
    with torch.no_grad():
        return torch.exp(model(images)).argmax(dim=1)


def test_horovod(ray_start_4_cpus):
    def train_func(config):
        result = hvd_train_func(config)
        assert len(result) == epochs
        assert result[-1] < result[0]

    num_workers = 1
    epochs = 10
    scaling_config = {"num_workers": num_workers}
    config = {"num_epochs": epochs, "save_model_as_dict": False}
    trainer = HorovodTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    result = trainer.fit()
    predictor = TorchPredictor.from_checkpoint(result.checkpoint)

    # Find some test data to run on.
    test_set = datasets.MNIST(
        "./data",
        train=False,
        download=True,
        transform=transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        ),
    )

    test_dataloader = DataLoader(test_set, batch_size=10)
    test_dataloader_iter = iter(test_dataloader)
    images, labels = next(
        test_dataloader_iter
    )  # only running a batch inference of 10 images
    predicted_labels = run_image_prediction(predictor.model, images)
    assert torch.equal(predicted_labels, labels)


def test_horovod_state_dict(ray_start_4_cpus):
    def train_func(config):
        result = hvd_train_func(config)
        assert len(result) == epochs
        assert result[-1] < result[0]

    num_workers = 2
    epochs = 10
    scaling_config = {"num_workers": num_workers}
    config = {"num_epochs": epochs, "save_model_as_dict": True}
    trainer = HorovodTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    result = trainer.fit()
    predictor = TorchPredictor.from_checkpoint(result.checkpoint, model=Net())

    # Find some test data to run on.
    test_set = datasets.MNIST(
        "./data",
        train=False,
        download=True,
        transform=transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        ),
    )

    test_dataloader = DataLoader(test_set, batch_size=10)
    test_dataloader_iter = iter(test_dataloader)
    images, labels = next(
        test_dataloader_iter
    )  # only running a batch inference of 10 images
    predicted_labels = run_image_prediction(predictor.model, images)
    assert torch.equal(predicted_labels, labels)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
