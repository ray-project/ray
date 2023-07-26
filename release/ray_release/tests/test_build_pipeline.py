import sys

import pytest

from ray_release.scripts.build_pipeline import _get_rerun_cmd


def test_get_rerun_cmd():
    cmd = " ".join(_get_rerun_cmd("collection", True))
    assert "--test-collection-file collection --run-jailed-tests" in cmd


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
