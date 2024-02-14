import os
import sys
import pytest

from ray.util import accelerators
from ray.util.annotations import RayDeprecationWarning


def test_sanity_check():
    


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
