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
    sys.exit(pytest.main(["-sv", __file__]))
