import pytest
from unittest.mock import patch, Mock

from ray.serve._private.http_proxy import GenericProxy, gRPCProxy, HTTPProxy


@patch("ray.get_runtime_context", Mock())
class TestHTTPProxy:
    def create_http_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
        )

    def test_subclass_from_generic_proxy(self):
        http_proxy = self.create_http_proxy()
        assert isinstance(http_proxy, GenericProxy)

    def test_proxy_name(self):
        http_proxy = self.create_http_proxy()
        assert http_proxy.proxy_name == "HTTP"

    def test_success_status_code(self):
        http_proxy = self.create_http_proxy()
        assert http_proxy.success_status_code == "200"




if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
