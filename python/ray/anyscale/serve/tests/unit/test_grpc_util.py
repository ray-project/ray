import grpc
import pytest

from ray.serve._private.default_impl import add_grpc_address


class FakeGrpcServer:  # noqa: F811
    def __init__(self):
        self.address = None
        self.certs = None

    def add_insecure_port(self, address: str):
        self.address = address

    def add_secure_port(self, address: str, server_credentials: grpc.ServerCredentials):
        self.address = address
        self.certs = server_credentials


def test_add_grpc_address_with_secure_port(monkeypatch):
    """Test `add_grpc_address` adds the address to the gRPC server on the secured port.

    When the environment variable `RAY_USE_TLS` is set to true, the gRPC server should
    add the address on the secured port.
    """
    monkeypatch.setenv("RAY_SERVE_GRPC_SERVER_USE_SECURE_PORT", "1")
    fake_grpc_server = FakeGrpcServer()
    grpc_address = "fake_address:50051"
    assert fake_grpc_server.address is None
    assert fake_grpc_server.certs is None
    add_grpc_address(fake_grpc_server, grpc_address)
    assert fake_grpc_server.address == grpc_address
    assert fake_grpc_server.certs is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
