"""Ray Serve application for the gRPC facade.

Serves the gRPC facade (``grpc_facade``) through Ray Serve's gRPC proxy, so a
Serve application, such as an Anyscale service, exposes the same API without
running the facade's own server. A Serve config names two entry points from
this module:

* ``add_servicers_to_server`` in ``grpc_options.grpc_servicer_functions``,
  which registers the client SDK's two services under their wire names.
* ``build_app`` as the application's ``import_path``.

Serve's proxy keeps only the method tables it is given and calls the ingress
method named after each RPC. The tables come from the same generated stubs
that the facade's own server routes with, and each ingress method runs the
facade's handler from the handler table that server dispatches through, so
the API token check applies unchanged. The facade keeps its exec table in
memory, so the deployment runs exactly one replica.

Requires ``grpclib`` and ``ray[serve]``.
"""

import asyncio
import os
from typing import Any, AsyncIterator, Callable, Dict, Iterator, Optional, Tuple

import grpc

from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import PublicAPI

try:
    from grpclib import GRPCError

    from ray.experimental.sandbox.http._proto.sandbox_control_grpc import (
        ModalClientBase,
    )
    from ray.experimental.sandbox.http._proto.sandbox_exec_grpc import (
        TaskCommandRouterBase,
    )
except ImportError as exc:  # pragma: no cover - exercised only without grpclib
    raise ImportError(
        "The Ray Sandbox gRPC facade requires the `grpclib` package: "
        "pip install grpclib"
    ) from exc

_DEPLOYMENT_NAME = "RaySandboxGrpcFacade"
# Long polls (SandboxWait, TaskExecWait) and output streams each hold a slot
# for as long as they wait, so admit far more than Serve's default.
_MAX_ONGOING_REQUESTS = 10_000
_HANDLER_KINDS = {
    (False, False): grpc.unary_unary_rpc_method_handler,
    (False, True): grpc.unary_stream_rpc_method_handler,
    (True, False): grpc.stream_unary_rpc_method_handler,
    (True, True): grpc.stream_stream_rpc_method_handler,
}
_END = object()


class _Unset:
    """Stands in for the handlers when only a route table is read."""

    def __getattr__(self, name: str) -> None:
        return None


def _routes() -> Iterator[Tuple[str, Any]]:
    """Each RPC's wire path and its entry in the facade's generated tables.

    Only the cardinality and the message types of each entry are used; its
    handler slot is empty.
    """
    for base in (ModalClientBase, TaskCommandRouterBase):
        yield from base.__mapping__(_Unset()).items()


@PublicAPI(stability="alpha")
def add_servicers_to_server(servicer: Any, server: Any) -> None:
    """Register the facade's gRPC services with a server.

    For ``grpc_options.grpc_servicer_functions`` in a Ray Serve config. The
    client SDK's control-plane and command-router services are registered
    under their wire names. Serve's proxy keeps their method tables and
    routes each call to the method of the same name on the application's
    ingress, which ``build_app`` builds.

    Args:
        servicer: The object whose methods handle the RPCs. Serve passes a
            placeholder and substitutes its own handlers.
        server: The ``grpc`` server to register the services with.
    """
    tables: Dict[str, Dict[str, Any]] = {}
    for path, route in _routes():
        _, service, method = path.split("/")
        kind = (route.cardinality.client_streaming, route.cardinality.server_streaming)
        tables.setdefault(service, {})[method] = _HANDLER_KINDS[kind](
            getattr(servicer, method),
            request_deserializer=route.request_type.FromString,
            response_serializer=route.reply_type.SerializeToString,
        )
    for service, table in tables.items():
        # One service per call: Serve's proxy reads only the first handler
        # of each call.
        server.add_generic_rpc_handlers(
            (grpc.method_handlers_generic_handler(service, table),)
        )


class _Stream:
    """The parts of a grpclib server stream that the facade's handlers use."""

    def __init__(self, requests: AsyncIterator[Any], metadata: Dict[str, Any]) -> None:
        self._requests = requests
        self.metadata = metadata
        self.responses: "asyncio.Queue[Any]" = asyncio.Queue()

    async def recv_message(self) -> Any:
        try:
            return await self._requests.__anext__()
        except StopAsyncIteration:
            return None

    async def send_message(self, message: Any) -> None:
        self.responses.put_nowait(message)


async def _single(request: Any) -> AsyncIterator[Any]:
    yield request


def _call_stream(request: Any, client_streaming: bool, grpc_context: Any) -> _Stream:
    # Serve passes a client-streaming call's messages as an async iterator.
    requests = request if client_streaming else _single(request)
    metadata: Dict[str, Any] = {}
    for key, value in grpc_context.invocation_metadata():
        # The first value of a repeated key, as grpclib reports it.
        metadata.setdefault(key, value)
    return _Stream(requests, metadata)


def _set_status(grpc_context: Any, error: GRPCError) -> None:
    grpc_context.set_code(grpc.StatusCode[error.status.name])
    grpc_context.set_details(error.message or error.status.name)


def _unary_method(name: str, client_streaming: bool, reply_type: Any) -> Callable:
    async def call(self, request: Any, grpc_context: Any) -> Any:
        stream = _call_stream(request, client_streaming, grpc_context)
        try:
            await self._handlers[name](stream)
        except GRPCError as error:
            # Serve sends the status set on the context with the reply.
            _set_status(grpc_context, error)
            return reply_type()
        if stream.responses.empty():
            return reply_type()
        return stream.responses.get_nowait()

    call.__name__ = name
    return call


def _streaming_method(name: str, client_streaming: bool) -> Callable:
    async def call(self, request: Any, grpc_context: Any) -> AsyncIterator[Any]:
        stream = _call_stream(request, client_streaming, grpc_context)

        async def run() -> None:
            try:
                await self._handlers[name](stream)
            finally:
                stream.responses.put_nowait(_END)

        task = asyncio.ensure_future(run())
        try:
            while (message := await stream.responses.get()) is not _END:
                yield message
            await task
        except GRPCError as error:
            # A streaming call's status reaches the client only with an
            # exception: a generator that returns ends the call as OK.
            _set_status(grpc_context, error)
            raise
        finally:
            task.cancel()

    call.__name__ = name
    return call


def _with_rpc_methods(cls: type) -> type:
    """Give ``cls`` one method per RPC, named after it, as Serve dispatches."""
    for path, route in _routes():
        name = path.rsplit("/", 1)[1]
        cardinality = route.cardinality
        if cardinality.server_streaming:
            method = _streaming_method(name, cardinality.client_streaming)
        else:
            method = _unary_method(name, cardinality.client_streaming, route.reply_type)
        setattr(cls, name, method)
    return cls


@_with_rpc_methods
class _FacadeIngress:
    """Serve ingress whose methods run the facade's RPC handlers."""

    def __init__(
        self,
        settings: SandboxAPISettings,
        advertise_url: Optional[str],
        handle_resolver_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        from ray.experimental.sandbox.http.grpc_facade import build_servicers

        # The facade reads the token from this process's environment, which
        # can lack what build_app found where the application was built,
        # such as when `serve run` builds it outside the cluster.
        _require_token_env(settings)
        # Built in the replica: the resolver holds locks and actor handles,
        # which can't travel with the application.
        resolver = handle_resolver_factory() if handle_resolver_factory else None
        servicers = build_servicers(
            settings, handle_resolver=resolver, advertise_url=advertise_url
        )
        # The tables grpclib's server dispatches through, so every call
        # passes the same token check here.
        self._handlers = {
            path.rsplit("/", 1)[1]: route.func
            for servicer in servicers
            for path, route in servicer.__mapping__().items()
        }

    async def __call__(self, request: Any) -> Any:
        """Answer HTTP requests: the application serves only gRPC."""
        from starlette.responses import PlainTextResponse

        return PlainTextResponse("This application serves gRPC only.", status_code=404)


def _require_token_env(settings: SandboxAPISettings) -> None:
    """Refuse to serve without the API token in this process's environment.

    The facade serves every call unchecked without one, and Serve's proxies
    listen on every node's address, which sandboxes with network access can
    reach.
    """
    if not os.environ.get(settings.token_env_var):
        raise ValueError(
            f"set {settings.token_env_var} to the API token that clients must "
            "present: Serve's proxies listen on every node's address, which "
            "sandboxes with network access can reach"
        )


def _bind(
    settings: SandboxAPISettings,
    advertise_url: Optional[str],
    handle_resolver_factory: Optional[Callable[[], Any]] = None,
) -> Any:
    from ray import serve

    deployment = serve.deployment(
        _FacadeIngress,
        name=_DEPLOYMENT_NAME,
        num_replicas=1,
        max_ongoing_requests=_MAX_ONGOING_REQUESTS,
    )
    return deployment.bind(settings, advertise_url, handle_resolver_factory)


@PublicAPI(stability="alpha")
def build_app(args: Optional[Dict[str, Any]] = None) -> Any:
    """Ray Serve application builder for the gRPC facade.

    Use it as the ``import_path`` of an application whose Serve config also
    lists ``add_servicers_to_server`` in
    ``grpc_options.grpc_servicer_functions``.

    The facade requires the API token from the environment variable named
    by ``token_env_var`` (default ``RAY_SANDBOX_API_TOKEN``). Building the
    application fails without it, and so does starting its replica, which
    reads the variable from its own environment: Serve's proxies listen on
    every node's address, which sandboxes with network access can reach.

    Args:
        args: Builder arguments. ``advertise_url`` sets the command-router
            URL handed to clients (default: ``https://`` plus the host each
            client dialed). Every other key is a :class:`SandboxAPISettings`
            field.

    Returns:
        The facade's ingress deployment, bound and ready to run.
    """
    args = dict(args or {})
    advertise_url = args.pop("advertise_url", None)
    settings = SandboxAPISettings(**args)
    if settings.num_replicas != 1:
        raise ValueError(
            "the gRPC facade keeps its exec table in memory and runs as one "
            "replica; remove num_replicas"
        )
    _require_token_env(settings)
    return _bind(settings, advertise_url)
