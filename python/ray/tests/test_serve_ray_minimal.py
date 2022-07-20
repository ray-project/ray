"""
Test that Serve gives good error message when no deps are installed.
This tests exist in the ray/tests/ directory instead of ray/serve/tests because
it needs not to have dependencies on serve's conftest.py.
"""

import os
import sys

import pytest


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.",
)
def test_error_msg():
    with pytest.raises(ModuleNotFoundError, match=r'.*install "ray\[serve\]".*'):
        from ray import serve

        serve.start()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
