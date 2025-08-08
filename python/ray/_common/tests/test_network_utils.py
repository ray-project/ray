import pytest
import sys

from ray._common.network_utils import parse_address, build_address


class TestBuildAddress:
    """Test cases for build_address function, matching C++ tests exactly."""

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
    def test_build_address(self, host, port, expected):
        """Test building address strings from host and port."""
        result = build_address(host, port)
        assert result == expected


class TestParseAddress:
    """Test cases for parse_address function, matching C++ tests exactly."""

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
    def test_parse_valid_addresses(self, address, expected):
        """Test parsing valid addresses."""
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
    def test_parse_bare_addresses(self, address):
        """Test parsing bare addresses returns None."""
        result = parse_address(address)
        assert result is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
