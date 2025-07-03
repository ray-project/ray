import sys

import pytest
import torch

from ray.air._internal.torch_utils import move_tensors_to_device


def test_move_tensors_to_device():
    """Test that move_tensors_to_device moves and concatenates tensors correctly."""
    device = torch.device("cpu")

    # torch.Tensor
    batch = torch.ones(1)
    t = move_tensors_to_device(batch, device)
    assert torch.equal(t, torch.ones(1))

    # Tuple[torch.Tensor]
    batch = (torch.ones(1), torch.ones(1))
    t1, t2 = move_tensors_to_device(batch, device)
    assert torch.equal(t1, torch.ones(1))
    assert torch.equal(t2, torch.ones(1))

    # List[torch.Tensor]
    batch = [torch.ones(1), torch.ones(1)]
    t1, t2 = move_tensors_to_device(batch, device)
    assert torch.equal(t1, torch.ones(1))
    assert torch.equal(t2, torch.ones(1))

    # List/Tuple[List/Tuple[torch.Tensor]]
    batch = [(torch.ones(1), torch.ones(1)), [torch.ones(1)]]
    t1, t2 = move_tensors_to_device(batch, device)
    assert torch.equal(t1, torch.ones(2))
    assert torch.equal(t2, torch.ones(1))

    # Dict[str, torch.Tensor]
    batch = {"a": torch.ones(1), "b": torch.ones(1)}
    out = move_tensors_to_device(batch, device)
    assert torch.equal(out["a"], torch.ones(1))
    assert torch.equal(out["b"], torch.ones(1))

    # Dict[str, List/Tuple[torch.Tensor]]
    batch = {"a": [torch.ones(1)], "b": (torch.ones(1), torch.ones(1))}
    out = move_tensors_to_device(batch, device)
    assert torch.equal(out["a"], torch.ones(1))
    assert torch.equal(out["b"], torch.ones(2))


def test_move_invalid_batch_type():
    """Test that move_tensors_to_device raises an error for invalid batch types."""
    device = torch.device("cpu")
    with pytest.raises(ValueError, match="Invalid input type"):
        move_tensors_to_device("invalid", device)

    with pytest.raises(ValueError, match="Invalid input type: list[int | list[int]]*"):
        move_tensors_to_device([1, 2, [3]], device)

    with pytest.raises(
        ValueError, match="Invalid input type: list[Tensor | list[Tensor]]*"
    ):
        move_tensors_to_device([torch.ones(1), [torch.ones(1)]], device)

    with pytest.raises(
        ValueError, match="Invalid input type: dict[str, Tensor | list[Tensor]]*"
    ):
        move_tensors_to_device({"a": torch.ones(1), "b": [torch.ones(1)]}, device)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
