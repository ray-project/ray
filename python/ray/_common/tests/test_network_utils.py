import sys

import pytest

from ray._common.network_utils import get_all_interfaces_ip, is_localhost


def test_is_localhost():
    assert is_localhost("localhost")
    assert is_localhost("127.0.0.1")
    assert is_localhost("::1")
    assert not is_localhost("8.8.8.8")
    assert not is_localhost("2001:db8::1")


def test_get_all_interfaces_ip_for_host():
    assert get_all_interfaces_ip() in ("0.0.0.0", "::")
    assert get_all_interfaces_ip("192.0.2.1") == "0.0.0.0"
    assert get_all_interfaces_ip("2001:db8::1") == "::"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
