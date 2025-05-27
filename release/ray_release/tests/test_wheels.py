import os
import sys
import pytest

from ray_release.util import url_exists


@pytest.fixture
def remove_buildkite_env():
    for key in os.environ:
        if key.startswith("BUILDKITE"):
            os.environ.pop(key)


def test_url_exist():
    assert url_exists("https://github.com/")
    assert not url_exists("invalid://somewhere")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
