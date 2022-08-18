import pytest
from ray.air.checkpoint import Checkpoint
from ray.train.torch.torch_checkpoint import TorchCheckpoint
import torch
import torch.nn as nn


@pytest.fixture
def simple_linear_model():
    return nn.Linear(1, 1)


def assert_equal_torch_models(model1, model2):
    # Check equality by comparing their `state_dict`
    model1_state = model1.state_dict()
    model2_state = model2.state_dict()
    assert len(model1_state.keys()) == len(model2_state.keys())
    for key in model1_state:
        assert key in model2_state
        assert torch.equal(model1_state[key], model2_state[key])


def test_torch_checkpoint(simple_linear_model):
    checkpoint = TorchCheckpoint.from_model(simple_linear_model)
    path = checkpoint.to_directory()

    checkpoint = Checkpoint.from_directory(path)
    checkpoint = TorchCheckpoint.from_checkpoint(checkpoint)
    model = checkpoint.get_model()

    assert_equal_torch_models(model, simple_linear_model)

    # TODO: check if preprocessor checkpointing works (?)
