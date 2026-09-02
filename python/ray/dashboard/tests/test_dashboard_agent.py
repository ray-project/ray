from unittest.mock import Mock

from ray.dashboard import agent


def test_build_grpc_address_uses_ipv6_node_address(monkeypatch):
    get_all_interfaces_ip = Mock(return_value="::")
    monkeypatch.setattr(agent, "get_all_interfaces_ip", get_all_interfaces_ip)

    assert agent._build_grpc_address("2001:db8::1", 12345) == "[::]:12345"
    get_all_interfaces_ip.assert_called_once_with("2001:db8::1")
