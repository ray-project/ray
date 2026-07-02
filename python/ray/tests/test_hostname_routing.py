import socket
import pytest
from ray.util import get_node_ip_address


def test_hostname_routing(monkeypatch):
    # Test that get_node_ip_address returns hostname when RAY_NODE_USE_HOSTNAME=1
    monkeypatch.setenv("RAY_NODE_USE_HOSTNAME", "1")
    monkeypatch.setattr(socket, "gethostname", lambda: "my-mock-hostname")
    assert get_node_ip_address() == "my-mock-hostname"


def test_default_routing(monkeypatch):
    # Test that get_node_ip_address returns an IP address (not hostname) by default
    monkeypatch.delenv("RAY_NODE_USE_HOSTNAME", raising=False)
    monkeypatch.setattr(socket, "gethostname", lambda: "my-mock-hostname")
    ip_address = get_node_ip_address()

    # Simple check: ip_address should look like an IP address, not the mocked hostname
    assert ip_address != "my-mock-hostname"
    # Basic IP validation (v4 or v6)
    assert "." in ip_address or ":" in ip_address


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
