import unittest
import os
import tempfile
import torch
from torch import nn

from ray.rllib.models.temp_spec_classes import TensorDict, SpecDict, ModelConfig
from ray.rllib.models.torch.model import TorchRecurrentModel, TorchModel

B, T = 6, 8


class SimpleRecurrentModel(TorchRecurrentModel):
    input_spec = SpecDict({"in": "b t h"}, h=2)
    output_spec = SpecDict({"out": "b t h"}, h=3)
    prev_state_spec = SpecDict({"in": "b h"}, h=4)
    next_state_spec = SpecDict({"out": "b h"}, h=5)

    def _unroll(self, input, prev_state):
        assert torch.all(input["in"] == torch.arange(B * T * 2).reshape(B, T, 2))
        assert torch.all(prev_state["in"] == torch.arange(B * 4).reshape(B, 4))
        assert "bork" not in input.keys()
        assert "bork" not in prev_state.keys()

        return (
            TensorDict({"out": torch.arange(B * T * 3).reshape(B, T, 3)}),
            TensorDict({"out": torch.arange(B * 5).reshape(B, 5)}),
        )


class SimpleModel(TorchModel):
    input_spec = SpecDict({"in": "b t h"}, h=2)
    output_spec = SpecDict({"out": "b t h"}, h=3)

    def _forward(self, input):
        assert torch.all(input["in"] == torch.arange(B * T * 2).reshape(B, T, 2))
        return TensorDict({"out": torch.arange(B * T * 3).reshape(B, T, 3)})


class IOTorchModel(SimpleModel):
    def __init__(self, value):
        super().__init__(config=ModelConfig())
        self.weights = nn.Parameter(torch.tensor([value]))

    def _forward(self, input):
        pass


class TestTorchIO(unittest.TestCase):
    def test_save_load(self):
        """Test saving/restoring model weights"""
        with tempfile.TemporaryDirectory("test_torch_model.cpt") as d:
            path = os.path.join(d, "bork")
            m = IOTorchModel(value=1.0)
            m.save(path)
            lo = IOTorchModel(value=2.0)
            lo.load(path)
            self.assertTrue(torch.all(m.weights == lo.weights))


class TestTorchRecurrentModel(unittest.TestCase):
    def test_init(self):
        SimpleRecurrentModel(config=ModelConfig())

    def test_unroll_and_filter(self):
        """Ensures unused inputs are filtered out before _unroll and that
        outputs are correct."""
        inputs = TensorDict(
            {
                "in": torch.arange(B * T * 2).reshape(B, T, 2),
                "bork": torch.arange(5 * 4).reshape(5, 4),
            }
        )
        states = TensorDict(
            {
                "in": torch.arange(B * 4).reshape(B, 4),
                "bork": torch.arange(5 * 4).reshape(5, 4),
            }
        )
        outputs, out_states = SimpleRecurrentModel(ModelConfig()).unroll(inputs, states)
        desired = TensorDict({"out": torch.arange(B * T * 3).reshape(B, T, 3)})
        desired_states = TensorDict({"out": torch.arange(B * 5).reshape(B, 5)})

        for k in outputs.flatten().keys() | desired.flatten().keys():
            self.assertTrue(torch.all(outputs[k] == desired[k]))
        for k in out_states.flatten().keys() | desired_states.flatten().keys():
            self.assertTrue(torch.all(out_states[k] == desired_states[k]))


class TestTorchModel(unittest.TestCase):
    def test_init(self):
        SimpleModel(config=ModelConfig())

    def test_unroll_and_filter(self):
        """Ensures unused inputs are filtered out before _unroll and that
        outputs are correct."""
        inputs = TensorDict(
            {
                "in": torch.arange(B * T * 2).reshape(B, T, 2),
                "bork": torch.arange(5 * 4).reshape(5, 4),
            }
        )
        outputs, _ = SimpleModel(ModelConfig()).unroll(inputs, TensorDict())
        desired = TensorDict({"out": torch.arange(B * T * 3).reshape(B, T, 3)})

        for k in outputs.flatten().keys() | desired.flatten().keys():
            self.assertTrue(torch.all(outputs[k] == desired[k]))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
