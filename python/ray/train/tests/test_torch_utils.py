import pytest
import torch

from ray.air._internal.torch_utils import (
    contains_tensor,
    load_torch_model,
)

torch_module = torch.nn.Linear(1, 1)


class TestLoadTorchModel:
    def test_load_module(self):
        assert load_torch_model(torch_module) == torch_module

    def test_load_state_dict(self):
        state_dict = torch_module.state_dict()
        model_definition = torch.nn.Linear(1, 1)
        assert model_definition.state_dict() != state_dict

        assert load_torch_model(state_dict, model_definition).state_dict() == state_dict

    def test_load_state_dict_fail(self):
        with pytest.raises(ValueError):
            # model_definition is required to load state dict.
            load_torch_model(torch_module.state_dict())


def test_contains_tensor():
    t = torch.tensor([0])
    assert contains_tensor(t)
    assert contains_tensor([1, 2, 3, t, 5, 6])
    assert contains_tensor([1, 2, 3, {"dict": t}, 5, 6])
    assert contains_tensor({"outer": [1, 2, 3, {"dict": t}, 5, 6]})
    assert contains_tensor({t: [1, 2, 3, {"dict": 2}, 5, 6]})
    assert not contains_tensor([4, 5, 6])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
