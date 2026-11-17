import pytest
import torch

from ray.air._internal.torch_utils import (
    consume_prefix_in_state_dict_if_present_not_in_place,
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

    def test_load_state_dict_empty_container_module(self):
        empty_seq = torch.nn.Sequential()
        loaded = load_torch_model(empty_seq.state_dict(), empty_seq)
        assert loaded is empty_seq

    def test_load_state_dict_fail(self):
        with pytest.raises(ValueError):
            # model_definition is required to load state dict.
            load_torch_model(torch_module.state_dict())


def test_consume_prefix_in_state_dict_if_present_not_in_place():
    original_metadata = {
        "": {"version": 1},
        "module.module": {"version": 2},
        "module": {"version": 1},
        "module.layer1": {"version": 1},
        "module.layer2": {"version": 1},
        "other.layer": {"version": 1},
    }
    state_dict = {
        "module.module.layer1.weight": torch.tensor([10.0]),
        "module.layer1.weight": torch.tensor([1.0]),
        "module.layer2.weight": torch.tensor([2.0]),
        "other.layer.weight": torch.tensor([3.0]),
        "_metadata": original_metadata.copy(),
    }

    result = consume_prefix_in_state_dict_if_present_not_in_place(state_dict, "module.")
    assert torch.equal(result["layer1.weight"], torch.tensor([1.0]))
    assert torch.equal(result["module.layer1.weight"], torch.tensor([10.0]))
    assert "layer2.weight" in result
    assert "other.layer.weight" in result
    assert result["_metadata"] == {
        "": {"version": 1},
        "module": {"version": 2},
        "layer1": {"version": 1},
        "layer2": {"version": 1},
        "other.layer": {"version": 1},
    }
    # Original state_dict and its _metadata must not be mutated in place.
    assert "module.layer1.weight" in state_dict
    assert state_dict["_metadata"] == original_metadata

    # When no tensor key has the prefix, caller's state_dict and _metadata
    # still must not be mutated in place.
    no_match_metadata = {"": {"version": 1}, "other": {"version": 1}}
    no_match_state_dict = {
        "other.weight": torch.tensor([1.0]),
        "_metadata": no_match_metadata,
    }
    no_match_result = consume_prefix_in_state_dict_if_present_not_in_place(
        no_match_state_dict, "module."
    )
    assert no_match_state_dict["_metadata"] is no_match_metadata
    assert no_match_result["_metadata"] == no_match_metadata
    assert no_match_result["_metadata"] is not no_match_metadata


def test_contains_tensor():
    t = torch.tensor([0])
    assert contains_tensor(t)
    assert contains_tensor([1, 2, 3, t, 5, 6])
    assert contains_tensor([1, 2, 3, {"dict": t}, 5, 6])
    assert contains_tensor({"outer": [1, 2, 3, {"dict": t}, 5, 6]})
    assert contains_tensor({t: [1, 2, 3, {"dict": 2}, 5, 6]})
    assert contains_tensor(torch_module)
    assert contains_tensor({"model": torch_module})
    assert contains_tensor([1, 2, torch_module])
    assert not contains_tensor([4, 5, 6])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
