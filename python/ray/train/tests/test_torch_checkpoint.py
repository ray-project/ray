import torch

from ray.train.torch import TorchCheckpoint


def test_from_state_dict():
    model = torch.nn.Linear(1, 1)
    expected_state_dict = model.state_dict()
    checkpoint = TorchCheckpoint.from_state_dict(expected_state_dict)
    actual_state_dict = checkpoint.get_model(torch.nn.Linear(1, 1)).state_dict()
    assert actual_state_dict == expected_state_dict


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
