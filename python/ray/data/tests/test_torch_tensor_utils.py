import sys

import pytest
import torch

from ray.air._internal.torch_utils import (
    concat_tensors_to_device,
    move_tensors_to_device,
)


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


def test_concat_tensors_to_device_no_copy():
    """Test concat_tensors_to_device when copy is not needed.

    This tests the optimization path where there is only one tensor
    and its device already matches the target device, so the tensor
    is returned directly without copying.
    """
    # Test case 1: Single tensor with device=None (no copy needed)
    tensor = torch.tensor([1.0, 2.0, 3.0])
    result = concat_tensors_to_device([tensor], device=None)
    # Should return the same tensor object, not a copy
    assert result is tensor
    assert torch.equal(result, tensor)

    # Test case 2: Single tensor on CPU with device="cpu" (no copy needed)
    tensor = torch.tensor([4.0, 5.0, 6.0], device="cpu")
    result = concat_tensors_to_device([tensor], device="cpu")
    # Should return the same tensor object, not a copy
    assert result is tensor
    assert torch.equal(result, tensor)

    # Test case 3: Single tensor on CPU with device=torch.device("cpu") (no copy needed)
    tensor = torch.tensor([7.0, 8.0, 9.0], device="cpu")
    result = concat_tensors_to_device([tensor], device=torch.device("cpu"))
    # Should return the same tensor object, not a copy
    assert result is tensor
    assert torch.equal(result, tensor)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
