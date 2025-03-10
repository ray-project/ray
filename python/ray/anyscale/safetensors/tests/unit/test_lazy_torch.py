import sys

import pytest

from ray.anyscale.safetensors._private.util import (
    bytes_to_gigabytes,
    get_current_torch_device,
)


def test_torch_imported_lazily():
    # Importing anytensor and using unrelated functionality should not import torch.
    assert bytes_to_gigabytes(1024**3) == 1
    assert "torch" not in sys.modules

    # Calling a torch-related method should import it.
    device = get_current_torch_device()
    assert "torch" in sys.modules

    # Check that the torch module functions as usual.
    import torch

    assert device == torch.device("cpu")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
