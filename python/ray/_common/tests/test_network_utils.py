import pytest
import sys

from ray._common.network_utils import parse_address, build_address, is_localhost


def test_is_localhost():
    assert is_localhost("localhost")
    assert is_localhost("127.0.0.1")
    assert is_localhost("::1")
    assert not is_localhost("8.8.8.8")
    assert not is_localhost("2001:db8::1")


@pytest.mark.parametrize(
    "host,port,expected",
    [
        # IPv4
        ("192.168.1.1", 8080, "192.168.1.1:8080"),
        ("192.168.1.1", "8080", "192.168.1.1:8080"),
        # IPv6
        ("::1", 8080, "[::1]:8080"),
        ("::1", "8080", "[::1]:8080"),
        ("2001:db8::1", 8080, "[2001:db8::1]:8080"),
        ("2001:db8::1", "8080", "[2001:db8::1]:8080"),
        # Hostname
        ("localhost", 9000, "localhost:9000"),
        ("localhost", "9000", "localhost:9000"),
    ],
)
def test_build_address(host, port, expected):
    """Test cases for build_address function, matching C++ tests exactly."""
    result = build_address(host, port)
    assert result == expected


@pytest.mark.parametrize(
    "address,expected",
    [
        # IPv4
        ("192.168.1.1:8080", ("192.168.1.1", "8080")),
        # IPv6:loopback address
        ("[::1]:8080", ("::1", "8080")),
        # IPv6
        ("[2001:db8::1]:8080", ("2001:db8::1", "8080")),
        # Hostname:Port
        ("localhost:9000", ("localhost", "9000")),
    ],
)
def test_parse_valid_addresses(address, expected):
    """Test cases for parse_address function, matching C++ tests exactly."""
    result = parse_address(address)
    assert result == expected


@pytest.mark.parametrize(
    "address",
    [
        # bare IP or hostname
        # should return None when no port is found
        "::1",
        "2001:db8::1",
        "192.168.1.1",
        "localhost",
    ],
)
def test_parse_bare_addresses(address):
    """Test parsing bare addresses returns None."""
    result = parse_address(address)
    assert result is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
