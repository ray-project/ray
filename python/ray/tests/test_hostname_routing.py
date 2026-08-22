import ipaddress
import sys

import pytest

from ray._private import services


def test_hostname_routing_delegates_to_node_address_from_perspective(monkeypatch):
    calls = []
    localhost_calls = []

    def fake_node_ip_address_from_perspective(address=None):
        calls.append(address)
        return "test-hostname"

    def fake_get_localhost_ip():
        localhost_calls.append(None)
        return "127.0.0.1"

    monkeypatch.setattr(services.ray_constants, "ENABLE_RAY_CLUSTER", False)
    monkeypatch.setattr(services, "get_localhost_ip", fake_get_localhost_ip)
    monkeypatch.setattr(
        services,
        "node_ip_address_from_perspective",
        fake_node_ip_address_from_perspective,
    )
    monkeypatch.setenv("RAY_NODE_USE_HOSTNAME", "1")

    assert services.get_node_ip_address("1.2.3.4:6379") == "test-hostname"
    assert calls == ["1.2.3.4:6379"]
    assert localhost_calls == []


def test_default_routing(monkeypatch):
    # Test that get_node_ip_address returns an IP address (not hostname) by default
    monkeypatch.delenv("RAY_NODE_USE_HOSTNAME", raising=False)
    ip_address = services.get_node_ip_address()

    # Strict IP validation (v4 or v6); raises ValueError if not a valid IP.
    ipaddress.ip_address(ip_address)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
