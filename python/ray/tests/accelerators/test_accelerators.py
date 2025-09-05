import sys

import pytest

from ray.util import accelerators
from ray.util.annotations import RayDeprecationWarning


def test_accelerators():
    assert accelerators.NVIDIA_TESLA_K80 == "K80"
    assert accelerators.NVIDIA_A100 == "A100"
    assert accelerators.APPLE_M1 == "M1"
    assert accelerators.APPLE_M1_PRO == "M1-Pro"
    assert accelerators.APPLE_M2 == "M2"
    assert accelerators.APPLE_M2_MAX == "M2-Max"
    assert accelerators.APPLE_M3 == "M3"
    assert accelerators.APPLE_SILICON == "Apple-Silicon"
    with pytest.raises(
        AttributeError,
        match="module 'ray.util.accelerators' has no attribute 'NVIDIA_INVALID'",
    ):
        _ = accelerators.NVIDIA_INVALID
    with pytest.warns(RayDeprecationWarning):
        assert accelerators.NVIDIA_TESLA_A100 == "A100"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
