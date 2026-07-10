import ipaddress

import pytest

from ray.util import get_node_ip_address


def test_hostname_routing(monkeypatch):
    # Test that get_node_ip_address returns a hostname (not an IP address) when
    # RAY_NODE_USE_HOSTNAME=1. Hostname resolution is delegated to the C++
    # GetNodeIpAddressFromPerspective (boost::asio::ip::host_name()), which can
    # return a different string than Python's socket.gethostname() on some
    # platforms (e.g. FQDN vs short name). So instead of asserting exact string
    # equality with socket.gethostname() -- which is flaky across platforms --
    # we assert that the result is a non-empty string that is not an IP address.
    monkeypatch.setenv("RAY_NODE_USE_HOSTNAME", "1")
    hostname = get_node_ip_address()
    assert hostname
    with pytest.raises(ValueError):
        ipaddress.ip_address(hostname)


def test_default_routing(monkeypatch):
    # Test that get_node_ip_address returns an IP address (not hostname) by default
    monkeypatch.delenv("RAY_NODE_USE_HOSTNAME", raising=False)
    ip_address = get_node_ip_address()

    # Strict IP validation (v4 or v6); raises ValueError if not a valid IP.
    ipaddress.ip_address(ip_address)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
