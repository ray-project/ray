import sys

import pytest

from ray._common.network_utils import is_localhost


def test_is_localhost():
    assert is_localhost("localhost")
    assert is_localhost("127.0.0.1")
    assert is_localhost("::1")
    assert not is_localhost("8.8.8.8")
    assert not is_localhost("2001:db8::1")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
