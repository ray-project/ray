import torch

from ray.train.torch import TorchCheckpoint


def assert_equal_torch_models(model1, model2):
    # Check equality by comparing their `state_dict`
    model1_state = model1.state_dict()
    model2_state = model2.state_dict()
    assert len(model1_state.keys()) == len(model2_state.keys())
    for key in model1_state:
        assert key in model2_state
        assert torch.equal(model1_state[key], model2_state[key])


def test_from_model():
    model = torch.nn.Linear(1, 1)
    checkpoint = TorchCheckpoint.from_model(model)
    assert_equal_torch_models(checkpoint.get_model(), model)

    with checkpoint.as_directory() as path:
        checkpoint = TorchCheckpoint.from_directory(path)
        checkpoint_model = checkpoint.get_model()

    assert_equal_torch_models(checkpoint_model, model)


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
