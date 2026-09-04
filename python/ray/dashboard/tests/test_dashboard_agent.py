import sys
from unittest.mock import Mock

import pytest

from ray.dashboard import agent


def test_build_grpc_address_uses_ipv6_node_address(monkeypatch):
    get_all_interfaces_ip = Mock(return_value="::")
    monkeypatch.setattr(agent, "get_all_interfaces_ip", get_all_interfaces_ip)

    assert agent._build_grpc_address("2001:db8::1", 12345) == "[::]:12345"
    get_all_interfaces_ip.assert_called_once_with("2001:db8::1")


@pytest.mark.parametrize(
    ("node_ip_address", "expected_address"),
    [
        ("127.0.0.1", "127.0.0.1:12345"),
        ("::1", "[::1]:12345"),
    ],
)
def test_build_grpc_address_preserves_literal_loopback(
    node_ip_address, expected_address
):
    assert agent._build_grpc_address(node_ip_address, 12345) == expected_address


def test_build_grpc_address_resolves_localhost(monkeypatch):
    get_localhost_ip = Mock(return_value="::1")
    monkeypatch.setattr(agent, "get_localhost_ip", get_localhost_ip)

    assert agent._build_grpc_address("localhost", 12345) == "[::1]:12345"
    get_localhost_ip.assert_called_once_with()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
