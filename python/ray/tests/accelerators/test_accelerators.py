import os
import sys
import pytest

from ray.util import accelerators
from ray.util.annotations import RayDeprecationWarning


def test_accelerators():
    assert accelerators.NVIDIA_TESLA_K80 == "K80"
    assert accelerators.NVIDIA_A100 == "A100"
    with pytest.raises(
        AttributeError,
        match="module 'ray.util.accelerators' has no attribute 'NVIDIA_INVALID'",
    ):
        accelerators.NVIDIA_INVALID
    with pytest.warns(RayDeprecationWarning):
        assert accelerators.NVIDIA_TESLA_A100 == "A100"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
