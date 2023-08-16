import pytest
import sys
from unittest.mock import patch, MagicMock

from ray.serve._private.http_proxy import GenericProxy, HTTPProxy


@patch("ray.get_runtime_context", MagicMock())
@patch("ray.get_actor", MagicMock())
class TestHTTPProxy:
    def create_http_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=MagicMock(),
        )

    def test_subclass_from_generic_proxy(self):
        http_proxy = self.create_http_proxy()
        assert isinstance(http_proxy, GenericProxy)

    def test_protocol(self):
        http_proxy = self.create_http_proxy()
        assert http_proxy.protocol == "HTTP"

    def test_success_status_code(self):
        http_proxy = self.create_http_proxy()
        assert http_proxy.success_status_code == "200"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
