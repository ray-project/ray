import sys

import grpc
import grpc.aio._server  # noqa: F401  # ensure Server class is loaded
import pytest

from ray.serve._private.grpc_util import gRPCGenericServer


def _noop_handler_factory(**kwargs):
    return None


def _get_free_port() -> int:
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_so_reuseport_channel_arg_is_integer(monkeypatch):
    """`grpc.so_reuseport` must be passed to gRPC as an integer.

    gRPC parses channel args by type and ignores a string value, so passing
    "0" (str) silently leaves SO_REUSEPORT on and lets two servers bind the
    same port. See #66815.
    """
    captured_options = {}
    original_init = grpc.aio._server.Server.__init__

    def fake_init(self, *args, **kwargs):
        captured_options["options"] = kwargs.get("options")
        return original_init(self, *args, **kwargs)

    monkeypatch.setattr(grpc.aio._server.Server, "__init__", fake_init)

    gRPCGenericServer(
        _noop_handler_factory,
        extra_options=[("grpc.so_reuseport", int(False))],
    )

    options = dict(captured_options["options"])
    assert options["grpc.so_reuseport"] == 0
    assert isinstance(options["grpc.so_reuseport"], int)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="SO_REUSEPORT semantics differ on Windows.",
)
def test_port_conflict_fails_with_reuseport_disabled():
    """With reuseport disabled, binding an in-use port must fail."""
    port = _get_free_port()
    address = f"127.0.0.1:{port}"

    server1 = gRPCGenericServer(
        _noop_handler_factory,
        extra_options=[("grpc.so_reuseport", 0)],
    )
    assert server1.add_insecure_port(address) == port

    server2 = gRPCGenericServer(
        _noop_handler_factory,
        extra_options=[("grpc.so_reuseport", 0)],
    )
    assert server2.add_insecure_port(address) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
