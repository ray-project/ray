import sys

import pytest

from ray._private.accelerators.intel_gpu import _accelerator_type_from_device_name
from ray.util.accelerators import (
    INTEL_ARC_PRO_B50,
    INTEL_ARC_PRO_B60,
    INTEL_ARC_PRO_B65,
    INTEL_ARC_PRO_B70,
    INTEL_MAX_1100,
    INTEL_MAX_1550,
)


@pytest.mark.parametrize(
    "device_name,expected",
    [
        ("Intel(R) Arc(TM) Pro B50 Graphics", INTEL_ARC_PRO_B50),
        ("Intel(R) Arc(TM) Pro B60 Graphics", INTEL_ARC_PRO_B60),
        ("Intel(R) Arc(TM) Pro B65 Graphics", INTEL_ARC_PRO_B65),
        ("Intel(R) Arc(TM) Pro B70 Graphics", INTEL_ARC_PRO_B70),
        # Tolerates a missing (TM), case differences and no trailing "Graphics".
        ("Intel(R) Arc Pro B60 Graphics", INTEL_ARC_PRO_B60),
        ("Intel(R) ARC(tm) PRO b50 Graphics", INTEL_ARC_PRO_B50),
        ("Intel(R) Arc(TM) Pro B60", INTEL_ARC_PRO_B60),
        # Data Center GPU Max names are unchanged.
        ("Intel(R) Data Center GPU Max 1550", INTEL_MAX_1550),
        ("Intel(R) Data Center GPU Max 1100", INTEL_MAX_1100),
    ],
)
def test_accelerator_type_from_device_name(device_name, expected):
    assert _accelerator_type_from_device_name(device_name) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
