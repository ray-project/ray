import pytest
import torch

from ray.train.torch import TorchCheckpoint


def test_from_model_and_get_model():
    model = torch.nn.Linear(1, 1)
    checkpoint = TorchCheckpoint.from_model(model)
    assert str(checkpoint.get_model().state_dict()) == str(model.state_dict())


def test_from_model_kwargs():
    model = torch.nn.Linear(1, 1)
    checkpoint = TorchCheckpoint.from_model(model, epoch=0)
    assert checkpoint.to_dict()["epoch"] == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
