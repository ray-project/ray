"""Tests of the gRPC facade served by Ray Serve (``grpc_app``).

The unit tests check what Serve's proxy needs from the facade: one service
per registration, and an ingress method per RPC that takes the call's
context and reports failures through it. The end-to-end tests run the
application on a local Serve instance, with the sandbox runtime faked inside
the Serve replica, and drive it through Serve's gRPC proxy with the
unmodified client SDK. They also need Ray Serve and ``modal``, which the
default CI image lacks.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import pickle
import socket
import sys
import urllib.error
import urllib.request
from collections import namedtuple
from typing import Any, Iterator, List, Optional, Tuple
from unittest import mock

import grpc
import pytest

from ray.experimental.sandbox.http import grpc_app, grpc_facade
from ray.experimental.sandbox.http._proto import (
    sandbox_control_pb2 as api_pb2,
    sandbox_exec_pb2 as sr_pb2,
)
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.experimental.sandbox.http.tests.conftest import (
    FakeResolver,
    LoopbackResolver,
)

_TOKEN = "grpc-app-test-token"
# The token as the client SDK presents it on the control plane (its token
# secret) and on the command router (the handed-out jwt as a bearer token).
_SECRET = [("x-modal-token-secret", _TOKEN)]
_BEARER = [("authorization", f"Bearer {_TOKEN}")]
_CallDetails = namedtuple("_CallDetails", ["method", "invocation_metadata"])


class _CaptureServer:
    """Records what a servicer function registers, as Serve's proxy does."""

    def __init__(self) -> None:
        self.calls: List[Tuple[Any, ...]] = []

    def add_generic_rpc_handlers(self, handlers: Tuple[Any, ...]) -> None:
        self.calls.append(handlers)


class _Context:
    """The parts of Serve's gRPC context that the facade uses: no abort()."""

    def __init__(self, metadata: List[Tuple[str, str]] = ()) -> None:
        self._metadata = list(metadata)
        self.code: Optional[Any] = None
        self.details: Optional[str] = None

    def invocation_metadata(self) -> List[Tuple[str, str]]:
        return self._metadata

    def set_code(self, code: Any) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _rpcs() -> Iterator[Tuple[str, str]]:
    """Each RPC of the facade's services: its wire path and name."""
    for module, service in ((api_pb2, "ModalClient"), (sr_pb2, "TaskCommandRouter")):
        descriptor = module.DESCRIPTOR.services_by_name[service]
        wire_service = descriptor.full_name.removeprefix("ray_sandbox_facade.")
        for method in descriptor.methods:
            yield f"/{wire_service}/{method.name}", method.name


def _ingress(monkeypatch: Any, advertise_url: Optional[str] = "http://x") -> Any:
    monkeypatch.setenv("RAY_SANDBOX_API_TOKEN", _TOKEN)
    return grpc_app._FacadeIngress(SandboxAPISettings(), advertise_url, FakeResolver)


def test_serve_routes_every_rpc_to_the_ingress() -> None:
    """Each RPC reaches the ingress method named after it, which takes the
    call's context and streams its replies exactly when the RPC does."""
    server = _CaptureServer()
    grpc_facade.add_servicers_to_server(mock.Mock(), server)
    # One service per call: Serve's proxy reads only the first handler.
    assert [len(handlers) for handlers in server.calls] == [1, 1]
    generic = [handlers[0] for handlers in server.calls]
    for path, name in _rpcs():
        found = [h.service(_CallDetails(path, ())) for h in generic]
        (handler,) = [h for h in found if h is not None]
        method = getattr(grpc_app._FacadeIngress, name)
        # Serve passes a call's context only to a parameter of this name.
        assert "grpc_context" in inspect.signature(method).parameters, path
        assert inspect.isasyncgenfunction(method) == handler.response_streaming, path


def test_ingress_requires_the_token(monkeypatch) -> None:
    ingress = _ingress(monkeypatch)
    request = api_pb2.AppGetOrCreateRequest(app_name="grpc-app-test")

    async def scenario() -> None:
        for metadata in ([], [("x-modal-token-secret", "wrong")]):
            context = _Context(metadata)
            with pytest.raises(grpc_facade._RpcError):
                await ingress.AppGetOrCreate(request, context)
            assert context.code == grpc.StatusCode.UNAUTHENTICATED
            assert context.details == "invalid or missing API token"
        for metadata in (_SECRET, _BEARER):
            context = _Context(metadata)
            reply = await ingress.AppGetOrCreate(request, context)
            assert context.code is None
            assert reply.app_id

    asyncio.run(scenario())


def test_streaming_errors_carry_their_status(monkeypatch) -> None:
    """A failed call sets its status on the context and raises: Serve ends a
    streaming call whose generator returns as OK."""
    ingress = _ingress(monkeypatch)
    request = sr_pb2.TaskExecStdioReadRequest(
        exec_id="ex-missing",
        file_descriptor=sr_pb2.TASK_EXEC_STDIO_FILE_DESCRIPTOR_STDOUT,
    )

    async def scenario() -> None:
        for metadata, code in (
            ([], grpc.StatusCode.UNAUTHENTICATED),
            (_BEARER, grpc.StatusCode.NOT_FOUND),
        ):
            context = _Context(metadata)
            with pytest.raises(grpc_facade._RpcError):
                async for _ in ingress.TaskExecStdioRead(request, context):
                    pass
            assert context.code == code

    asyncio.run(scenario())


def test_rpc_errors_survive_pickling() -> None:
    """Serve sends a failed call's exception from its replica to its proxy."""
    error = pickle.loads(
        pickle.dumps(grpc_facade._RpcError(grpc.StatusCode.NOT_FOUND, "gone"))
    )
    assert (error.code, error.message, str(error)) == (
        grpc.StatusCode.NOT_FOUND,
        "gone",
        "gone",
    )


def test_router_access_defaults_to_the_dialed_host(monkeypatch) -> None:
    ingress = _ingress(monkeypatch, advertise_url=None)
    context = _Context(_SECRET + [("x-modal-host", "facade.example.com")])
    access = asyncio.run(
        ingress.TaskGetCommandRouterAccess(
            api_pb2.TaskGetCommandRouterAccessRequest(), context
        )
    )
    assert context.code is None
    assert (access.url, access.jwt) == ("https://facade.example.com", _TOKEN)


def test_build_app_requires_a_token() -> None:
    with pytest.raises(ValueError, match="RAY_SANDBOX_API_TOKEN"):
        grpc_app.build_app({})


def test_ingress_refuses_to_start_without_a_token(monkeypatch) -> None:
    """The replica checks its own environment, which can lack the token that
    build_app found where the application was built."""
    monkeypatch.delenv("RAY_SANDBOX_API_TOKEN", raising=False)
    resolver_factory = mock.Mock()
    with pytest.raises(ValueError, match="RAY_SANDBOX_API_TOKEN"):
        grpc_app._FacadeIngress(SandboxAPISettings(), "http://x", resolver_factory)
    resolver_factory.assert_not_called()


def test_build_app_runs_one_replica(monkeypatch) -> None:
    monkeypatch.setenv("RAY_SANDBOX_API_TOKEN", _TOKEN)
    with pytest.raises(ValueError, match="one replica"):
        grpc_app.build_app({"num_replicas": 2})


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def serve_facade() -> Iterator[Tuple[str, int]]:
    """The application on a local Serve instance; yields its gRPC URL and
    HTTP port."""
    serve = pytest.importorskip("ray.serve")
    if not hasattr(serve, "run"):
        pytest.skip("ray.serve is present but not fully installed")
    pytest.importorskip("modal")

    import ray
    from ray.serve.config import gRPCOptions

    grpc_port, http_port = _free_port(), _free_port()
    previous_token = os.environ.get("RAY_SANDBOX_API_TOKEN")
    # Set before Ray starts, so the Serve replica inherits it.
    os.environ["RAY_SANDBOX_API_TOKEN"] = _TOKEN
    ray.init()
    try:
        serve.start(
            grpc_options=gRPCOptions(
                port=grpc_port,
                grpc_servicer_functions=[
                    "ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server"
                ],
            ),
            http_options={"host": "127.0.0.1", "port": http_port},
        )
        url = f"http://127.0.0.1:{grpc_port}"
        # The application build_app builds, with the sandbox runtime faked in
        # the replica. Loopback, because the SDK accepts a plaintext command
        # router only on localhost.
        serve.run(
            grpc_app._bind(SandboxAPISettings(), url, LoopbackResolver),
            name="sandbox-facade",
            route_prefix="/",
        )
        yield url, http_port
    finally:
        serve.shutdown()
        ray.shutdown()
        # Restored so the token does not leak into later test modules.
        if previous_token is None:
            os.environ.pop("RAY_SANDBOX_API_TOKEN", None)
        else:
            os.environ["RAY_SANDBOX_API_TOKEN"] = previous_token


def _client(url: str, secret: str, monkeypatch: Any) -> Any:
    import modal

    # The SDK reads its server URL from the environment when a client opens.
    monkeypatch.setenv("MODAL_SERVER_URL", url)
    # The facade serves the SDK's original sandbox API, which modal 1.6
    # stopped using by default.
    monkeypatch.setenv("MODAL_SANDBOX_V2", "0")
    return modal.Client.from_credentials("ak-test", secret)


def test_the_sdk_runs_sandboxes_through_serve(serve_facade, monkeypatch) -> None:
    import modal

    url, _ = serve_facade
    client = _client(url, _TOKEN, monkeypatch)
    app = modal.App.lookup("grpc-app-test", create_if_missing=True, client=client)
    sandbox = modal.Sandbox.create(
        app=app,
        image=modal.Image.from_registry("python:3.12-slim"),
        timeout=600,
        client=client,
    )
    process = sandbox.exec("echo", "hello", "serve")
    assert process.wait() == 0
    assert process.stdout.read() == "hello serve\n"
    assert sandbox.exec("sh", "-c", "exit 3").wait() == 3
    # Several messages of the client stream that carries a file write.
    payload = bytes(range(256)) * 4096
    sandbox.filesystem.write_bytes(payload, "/work/data.bin")
    assert sandbox.filesystem.read_bytes("/work/data.bin") == payload
    sandbox.terminate()
    assert sandbox.poll() is not None


def test_serve_rejects_a_wrong_token(serve_facade, monkeypatch) -> None:
    import modal
    from modal.exception import AuthError

    url, _ = serve_facade
    client = _client(url, "wrong", monkeypatch)
    with pytest.raises(AuthError):
        modal.App.lookup("grpc-app-test", create_if_missing=True, client=client)


def test_http_requests_get_not_found(serve_facade) -> None:
    _, http_port = serve_facade
    with pytest.raises(urllib.error.HTTPError) as info:
        urllib.request.urlopen(f"http://127.0.0.1:{http_port}/", timeout=30)
    assert info.value.code == 404


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
