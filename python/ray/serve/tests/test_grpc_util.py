import asyncio
import logging
import sys

import grpc
import pytest

import grpc.aio._server  # noqa: F401  # ensure the Server class is loaded

from ray.serve._private.grpc_util import gRPCGenericServer

logger = logging.getLogger(__name__)


def _noop_handler_factory(**kwargs):
    return None


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


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="SO_REUSEPORT semantics differ on Windows.",
)
async def test_port_conflict_fails_with_reuseport_disabled():
    """With reuseport disabled, binding an in-use port must fail.

    The first server binds to an ephemeral port (port 0) and the second binds
    to that exact port, so the test is deterministic and does not race with
    other processes over a pre-picked port.
    """
    server1 = gRPCGenericServer(
        _noop_handler_factory,
        extra_options=[("grpc.so_reuseport", 0)],
    )
    server2 = gRPCGenericServer(
        _noop_handler_factory,
        extra_options=[("grpc.so_reuseport", 0)],
    )
    try:
        port = server1.add_insecure_port("127.0.0.1:0")
        assert port != 0

        # Binding the same explicit port must be refused. Newer grpcio raises
        # RuntimeError from add_insecure_port for a failed explicit-port bind;
        # older versions return 0.
        refused = False
        try:
            bound = server2.add_insecure_port(f"127.0.0.1:{port}")
            refused = bound == 0
        except RuntimeError:
            refused = True
        assert refused, "second bind must be refused with reuseport disabled"
    finally:
        results = await asyncio.gather(
            server1.stop(grace=0), server2.stop(grace=0), return_exceptions=True
        )
        for result in results:
            if isinstance(result, Exception):
                logger.debug("Ignoring error while stopping test server: %s", result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
