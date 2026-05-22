import os
import socket
import pytest
import ray
from ray.util import get_node_ip_address


def test_hostname_routing(monkeypatch):
    # Test that get_node_ip_address returns hostname when RAY_NODE_USE_HOSTNAME=1
    monkeypatch.setenv("RAY_NODE_USE_HOSTNAME", "1")
    hostname = socket.gethostname()
    assert get_node_ip_address() == hostname


def test_default_routing(monkeypatch):
    # Test that get_node_ip_address returns an IP address (not hostname) by default
    # (assuming the hostname is not an IP address itself)
    monkeypatch.delenv("RAY_NODE_USE_HOSTNAME", raising=False)
    ip_address = get_node_ip_address()
    hostname = socket.gethostname()

    # Simple check: ip_address should look like an IP address, not the hostname
    # This might fail if the hostname IS an IP address, but usually it's not.
    assert ip_address != hostname
    # Basic IP validation (v4 or v6)
    assert "." in ip_address or ":" in ip_address


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
