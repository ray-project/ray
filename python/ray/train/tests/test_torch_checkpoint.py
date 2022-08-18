import pytest
import torch

from ray.train.torch import TorchCheckpoint


def test_from_state_dict():
    model = torch.nn.Linear(1, 1)
    expected_state_dict = model.state_dict()
    checkpoint = TorchCheckpoint.from_state_dict(expected_state_dict)
    actual_state_dict = checkpoint.get_model(torch.nn.Linear(1, 1)).state_dict()
    assert actual_state_dict == expected_state_dict


def test_from_model_value_error():
    class StubModel(torch.nn.Module):
        __module__ = "__main__"

        def forward(x):
            return x

    model = StubModel()
    with pytest.raises(ValueError):
        TorchCheckpoint.from_model(model)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
