"""End-to-end tests of the gRPC facade driven by the real Modal client SDK.

``test_grpc_facade.py`` speaks the wire contract through the vendored stubs;
these tests run an unmodified ``modal`` client against the facade instead, on
the same fake resolver and runtime as the REST tests, so a mismatch between
the vendored contract and the SDK shows up here. They also cover importing
the facade and the SDK in one process. Skipped when ``modal`` or ``grpclib``
is not installed (neither is in the default CI image).
"""

from __future__ import annotations

import asyncio
import sys
import threading
from typing import Any, Iterator, Tuple

import pytest

from ray.experimental.sandbox.http.tests.conftest import (
    FakeExecResult,
    FakeResolver,
    FakeSandboxRuntime,
)

try:
    import modal
    from grpclib.server import Server
    from modal.exception import InvalidError, SandboxFilesystemNotFoundError

    from ray.experimental.sandbox.http.grpc_facade import build_servicers

    _HAVE_DEPS = True
except ImportError:
    _HAVE_DEPS = False

pytestmark = pytest.mark.skipif(
    not _HAVE_DEPS,
    reason="modal and grpclib are not installed (optional; absent in CI)",
)

_next_port = iter(range(50961, 50991))
_IMAGE = "python:3.12-slim"


class _FacadeThread:
    """The facade on its own event loop thread, as in a real deployment.

    The Modal SDK runs its own event loop and reaches the facade over
    loopback, which also satisfies its rule that a plain-http command router
    must be on localhost.
    """

    def __init__(self, resolver: FakeResolver, port: int) -> None:
        self._resolver = resolver
        self._port = port
        self._loop = asyncio.new_event_loop()
        self._server: Any = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started = threading.Event()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        servicers = build_servicers(
            handle_resolver=self._resolver,
            advertise_url=f"http://127.0.0.1:{self._port}",
        )
        self._server = Server(servicers)
        self._loop.run_until_complete(self._server.start("127.0.0.1", self._port))
        self._started.set()
        self._loop.run_forever()

    def __enter__(self) -> "_FacadeThread":
        self._thread.start()
        assert self._started.wait(10), "facade did not start"
        return self

    def __exit__(self, *exc_info: Any) -> None:
        async def _stop() -> None:
            self._server.close()
            await self._server.wait_closed()

        asyncio.run_coroutine_threadsafe(_stop(), self._loop).result(10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10)


@pytest.fixture
def facade(monkeypatch) -> Iterator[Tuple[FakeResolver, Any]]:
    port = next(_next_port)
    resolver = FakeResolver()
    # The SDK reads its server URL from the environment when a client opens.
    monkeypatch.setenv("MODAL_SERVER_URL", f"http://127.0.0.1:{port}")
    with _FacadeThread(resolver, port):
        client = modal.Client.from_credentials("ak-test", "as-test")
        yield resolver, client


def _create(client: Any, **kwargs: Any) -> Any:
    app = modal.App.lookup("facade-sdk-test", create_if_missing=True, client=client)
    return modal.Sandbox.create(
        app=app,
        image=modal.Image.from_registry(_IMAGE),
        timeout=600,
        client=client,
        **kwargs,
    )


def _host(resolver: FakeResolver, sandbox: Any) -> Any:
    return resolver.handles[sandbox.object_id].host


def test_create_exec_and_terminate(facade) -> None:
    resolver, client = facade
    runtime = FakeSandboxRuntime()
    runtime.exec_results = [
        FakeExecResult(stdout="hello\n"),
        FakeExecResult(exit_code=3, stderr="boom\n"),
    ]
    resolver.next_runtime = runtime

    sandbox = _create(client, cpu=0.5, memory=512)
    process = sandbox.exec("echo", "hello")
    assert process.wait() == 0
    assert process.stdout.read() == "hello\n"
    process = sandbox.exec("sh", "-c", "exit 3")
    assert process.wait() == 3
    assert process.stderr.read() == "boom\n"

    assert [call["command"] for call in runtime.exec_calls] == [
        ["echo", "hello"],
        ["sh", "-c", "exit 3"],
    ]
    assert runtime.create_calls[0]["image"] == _IMAGE
    (options,) = resolver.create_options
    assert options["num_cpus"] == 0.5
    assert options["memory"] == 512 * 1024 * 1024

    sandbox.terminate()
    assert runtime.deleted == [runtime.instance_id]
    assert sandbox.poll() is not None


def test_filesystem_roundtrip(facade) -> None:
    resolver, client = facade
    runtime = FakeSandboxRuntime()
    runtime.readable_files["/work/in.txt"] = b"from the sandbox"
    resolver.next_runtime = runtime

    sandbox = _create(client)
    sandbox.filesystem.write_bytes(b"\x00payload\xff", "/work/data.bin")
    assert runtime.written_files["/work/data.bin"] == b"\x00payload\xff"
    assert sandbox.filesystem.read_bytes("/work/in.txt") == b"from the sandbox"
    with pytest.raises(SandboxFilesystemNotFoundError):
        sandbox.filesystem.read_bytes("/work/missing.txt")
    sandbox.terminate()


def test_network_policies_map_onto_the_runtime(facade) -> None:
    resolver, client = facade

    blocked = _create(client, block_network=True)
    open_egress = _create(client)
    assert _host(resolver, blocked)._spec["network"] == "none"
    assert _host(resolver, open_egress)._spec["network"] == "public"
    # Refused rather than granted wider than the client asked for.
    with pytest.raises(InvalidError, match="allowlist"):
        _create(client, outbound_cidr_allowlist=["1.1.1.1/32"])
    with pytest.raises(InvalidError, match="allowlist"):
        _create(client, outbound_domain_allowlist=["pypi.org"])
    blocked.terminate()
    open_egress.terminate()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
