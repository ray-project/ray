"""gRPC facade over Ray Sandbox for third-party sandbox clients.

Implements the subset of a sandbox client SDK's control-plane
(``ModalClient``) and command-router (``TaskCommandRouter``) services that
its Sandbox API uses, backed by the same detached ``SandboxHost`` actors as
the REST API in ``app.py``. An unmodified client pointed at this server can
create sandboxes, run commands, and use its filesystem API against a Ray
cluster. The wire contract is vendored under ``_proto/``.

The facade keeps no registry: object ids carry their payload (``im-`` wraps
an image ref, ``st-`` a secret's env dict) and the sandbox id doubles as the
client task id. Each exec lives on its sandbox's host under the client's
exec id, so the facade holds no state and can run as several processes.

When the environment variable named by ``SandboxAPISettings.token_env_var``
(default ``RAY_SANDBOX_API_TOKEN``) is set, every RPC must present that
token, as the client's token secret or as ``authorization: Bearer <token>``,
the same token the REST app checks. Without one, ``main`` serves only
loopback addresses unless ``--allow-unauthenticated`` is passed: sandboxes
with network access can reach any address their node can, so a tokenless
facade on a network address would also serve the code running inside them.

``RaySandboxFacade`` is a standard ``grpc`` servicer of both services, so
it runs on the facade's own ``grpc.aio`` server (``python -m
ray.experimental.sandbox.http.grpc_facade``, which needs only
``ray[default]``) or as a Ray Serve gRPC deployment (``grpc_app``).
"""

import argparse
import asyncio
import base64
import functools
import hashlib
import hmac
import inspect
import ipaddress
import json
import logging
import os
import re
import shlex
import threading
import uuid
from typing import Any, AsyncIterator, Callable, Dict, List, NoReturn, Optional, Set

import grpc
from grpc import StatusCode

from ray.experimental.sandbox.http._proto import (
    sandbox_control_pb2 as api_pb2,
    sandbox_exec_pb2 as sr_pb2,
)
from ray.experimental.sandbox.http._proto.sandbox_control_pb2_grpc import (
    ModalClientServicer,
    add_ModalClientServicer_to_server,
)
from ray.experimental.sandbox.http._proto.sandbox_exec_pb2_grpc import (
    TaskCommandRouterServicer,
    add_TaskCommandRouterServicer_to_server,
)
from ray.experimental.sandbox.http.host import HostSettings, SandboxSpec
from ray.experimental.sandbox.http.resolver import (
    SANDBOX_ID_PREFIX,
    HostedSandboxHandle,
    RayActorHandleResolver,
    _is_actor_gone,
    _is_actor_unavailable,
    _is_unschedulable,
)
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)

# Helper binary the client SDK execs for its filesystem API. Sandbox images
# do not contain it; execs of this argv are emulated, never run.
_FS_TOOLS_PATH = "/__modal/.bin/modal-sandbox-fs-tools"

# Exit codes for exec jobs that did not produce one of their own.
_EXIT_TIMEOUT = 124
_EXIT_ERROR = 126

_LONG_POLL_SECONDS = 15.0
_WAIT_MAX_SECONDS = 55.0
# How long a non-blocking poll (SandboxWait with timeout=0) may wait on an
# actor that is not answering before reporting "still running".
_POLL_GRACE_SECONDS = 0.5
_SCHEDULING_GRACE_SECONDS = 10.0
_PULL_TIMEOUT_SECONDS = 1800.0
_START_TIMEOUT_SECONDS = 120.0
_READ_BOUND_SECONDS = 600.0
_WRITE_BOUND_SECONDS = 120.0
# How long SandboxTerminate waits for the host's terminate() to report back.
_TERMINATE_WAIT_SECONDS = 30.0
_STDIO_CHUNK_BYTES = 256 * 1024
# Command-router credential handed out when no token is configured.
_ROUTER_JWT_WITHOUT_TOKEN = "ray-sandbox-facade"
# A host a client may name as the one it dialed: a DNS name or an IPv4
# address, so that it forms a URL by itself.
_DIALED_HOST = re.compile(r"[A-Za-z0-9.-]{1,253}")


class _RpcError(Exception):
    """Ends the RPC being served with a gRPC status; see ``_rpc``."""

    def __init__(self, code: StatusCode, message: str) -> None:
        # Both in args, so the error survives pickling: Ray Serve sends a
        # failed call's exception from its replica to its proxy.
        super().__init__(code, message)
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return self.message


def _new_sandbox_id(key: Optional[str] = None) -> str:
    """Mint a sandbox id in the client's V1 shape: ``sb-`` plus 22 base62 chars.

    The SDK routes ids of any other shape to its V2 backend, which needs
    auth-token RPCs the facade does not serve. Hex is a base62 subset.
    """
    suffix = hashlib.sha256(key.encode()).hexdigest() if key else uuid.uuid4().hex
    return f"{SANDBOX_ID_PREFIX}{suffix[:22]}"


def _encode_id(prefix: str, payload: Any) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return prefix + base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_id(prefix: str, value: str) -> Any:
    if not value.startswith(prefix):
        raise _RpcError(StatusCode.INVALID_ARGUMENT, f"malformed id: {value!r}")
    data = value[len(prefix) :]
    data += "=" * (-len(data) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(data))
    except (ValueError, TypeError):
        raise _RpcError(StatusCode.INVALID_ARGUMENT, f"malformed id: {value!r}")


def _image_ref_from_dockerfile(image: Any) -> str:
    """Extract the registry ref from an Image proto.

    Anything beyond a single FROM plus metadata-only commands asks for a
    server-side image build, which Ray Sandbox does not do.
    """
    ref = None
    for line in image.dockerfile_commands:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split(None, 1)
        keyword = parts[0].upper()
        if keyword == "FROM":
            if len(parts) < 2:
                raise _RpcError(
                    StatusCode.INVALID_ARGUMENT, "FROM needs an image reference"
                )
            if ref is not None:
                raise _RpcError(
                    StatusCode.INVALID_ARGUMENT,
                    "multi-stage image builds are not supported by the "
                    "Ray Sandbox gRPC facade",
                )
            ref = parts[1].strip()
        elif keyword not in ("ENTRYPOINT", "CMD", "ENV", "WORKDIR", "LABEL"):
            raise _RpcError(
                StatusCode.INVALID_ARGUMENT,
                f"image build step {stripped!r} is not supported: the "
                "Ray Sandbox gRPC facade only runs prebuilt registry images "
                "(a single FROM line)",
            )
    if ref is None:
        raise _RpcError(
            StatusCode.INVALID_ARGUMENT, "image definition contains no FROM line"
        )
    return ref


def _env_from_secret_ids(secret_ids: Any) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for secret_id in secret_ids:
        env.update(_decode_id("st-", secret_id))
    return env


def _fs_error(error_kind: str, message: str) -> bytes:
    return json.dumps({"error_kind": error_kind, "message": message}).encode()


class _FacadeState:
    """State shared between the control-plane and exec-plane servicers."""

    def __init__(
        self,
        resolver: Any,
        settings: SandboxAPISettings,
        advertise_url: Optional[str],
        token: Optional[str] = None,
    ) -> None:
        self.resolver = resolver
        self.settings = settings
        # None: derive each client's router URL; see _router_url_for.
        self.advertise_url = advertise_url
        # Required on every RPC when set; see _rpc.
        self.token = token

    # Resolver calls can wait on a GCS round trip (an actor create, a cache
    # miss), so they run in worker threads, never on the event loop that
    # serves every client.
    async def lookup(self, sandbox_id: str) -> Optional[Any]:
        cached = getattr(self.resolver, "cached", None)
        if cached is not None:
            handle = cached(sandbox_id)
            if handle is not None:
                return handle
        return await asyncio.to_thread(self.resolver.get, sandbox_id)

    async def require_handle(self, sandbox_id: str) -> Any:
        handle = await self.lookup(sandbox_id)
        if handle is None:
            raise _RpcError(StatusCode.NOT_FOUND, f"sandbox {sandbox_id!r} not found")
        return handle

    async def kill(self, sandbox_id: str, handle: Any) -> None:
        self.resolver.forget(sandbox_id)
        await asyncio.to_thread(self.resolver.kill, handle)


def _actor_error(exc: Exception) -> Optional[_RpcError]:
    """The gRPC status a failed SandboxHost call maps to, or None to re-raise.

    Unreachable actors map to UNAVAILABLE, which the client SDK retries; a
    dead actor maps to NOT_FOUND, the SDK's "task shut down" signal.
    """
    if _is_actor_unavailable(exc):
        return _RpcError(
            StatusCode.UNAVAILABLE, "sandbox actor is temporarily unavailable"
        )
    if _is_actor_gone(exc):
        return _RpcError(StatusCode.NOT_FOUND, "sandbox is gone; its actor has died")
    if _is_unschedulable(exc):
        return _RpcError(
            StatusCode.FAILED_PRECONDITION,
            f"sandbox cannot be scheduled: {str(exc)[:300]}",
        )
    return None


async def _bounded(
    awaitable: Any, extra_wait: float = 0.0, grace: Optional[float] = None
) -> Any:
    """Await a SandboxHost call, mapping actor failures to gRPC statuses.

    A call to an actor that is not scheduled yet (the cluster may still be
    scaling) blocks indefinitely, so every call is capped at its own
    long-poll budget plus a grace period (``grace`` overrides the default
    for calls that must return promptly); past it the client gets
    UNAVAILABLE and retries.
    """
    if grace is None:
        grace = _SCHEDULING_GRACE_SECONDS
    try:
        return await asyncio.wait_for(awaitable, timeout=extra_wait + grace)
    except asyncio.TimeoutError:
        raise _RpcError(
            StatusCode.UNAVAILABLE,
            "sandbox actor is not reachable; the cluster may still be scaling",
        )
    except Exception as exc:
        mapped = _actor_error(exc)
        if mapped is not None:
            raise mapped
        raise


async def _unbounded(awaitable: Any) -> Any:
    """Await a SandboxHost call to completion, however long the actor takes.

    For calls with a side effect the facade must not repeat, such as
    starting a command: a bound here would leave it unsure whether the call
    ran. Callers bound the client's wait separately and let this continue.
    """
    try:
        return await awaitable
    except Exception as exc:
        mapped = _actor_error(exc)
        if mapped is not None:
            raise mapped
        raise


async def _is_alive(handle: Any) -> bool:
    """False only when the actor has died; an unscheduled actor counts as alive."""
    try:
        await _bounded(handle.describe.remote())
    except _RpcError as exc:
        return exc.code != StatusCode.NOT_FOUND
    return True


def _exec_exit_code(info: Dict[str, Any]) -> int:
    if info["status"] == "completed":
        return info["exit_code"] if info["exit_code"] is not None else 0
    if info["status"] == "timeout":
        return _EXIT_TIMEOUT
    return _EXIT_ERROR


def _exec_stream(info: Dict[str, Any], want_stdout: bool) -> bytes:
    if want_stdout:
        return (info.get("stdout") or "").encode("utf-8", errors="replace")
    stderr = (info.get("stderr") or "").encode("utf-8", errors="replace")
    # Spawn and timeout failures surface on stderr, where clients look.
    if info["status"] in ("error", "timeout") and info.get("error"):
        stderr += f"\n[ray-sandbox] {info['error']}".lstrip("\n").encode(
            "utf-8", errors="replace"
        )
    return stderr


def _job_exit_code(info: Dict[str, Any]) -> int:
    """A finished host exec's exit code, for a command or a file operation."""
    if info.get("kind", "command") == "command":
        return _exec_exit_code(info)
    code = info.get("exit_code")
    return code if code is not None else _EXIT_ERROR


def _job_stream(info: Dict[str, Any], want_stdout: bool) -> bytes:
    """A finished host exec's stdout or stderr as the client SDK reads it.

    File operations report failures as the fs-tools JSON error on stderr.
    """
    if info.get("kind", "command") == "command":
        return _exec_stream(info, want_stdout)
    if want_stdout:
        return info.get("content") or b""
    if info["status"] == "completed":
        return b""
    if info.get("failure_code") == "file_not_found":
        return _fs_error("NotFound", "path does not exist")
    return _fs_error("Other", info.get("error") or "file operation failed")


def _check_found(result: Dict[str, Any]) -> None:
    """Raise NOT_FOUND for a sandbox its node host doesn't have.

    A hosted sandbox's id resolves while its node's host lives, so an id
    never created, or one already released, reaches the host; the status
    matches a sandbox actor that no longer exists.
    """
    if result.get("error_code") == "sandbox_not_found":
        raise _RpcError(StatusCode.NOT_FOUND, result["message"])


def _check_stdin(
    result: Dict[str, Any], settings: SandboxAPISettings
) -> Dict[str, Any]:
    """Map a host stdin call's error to its gRPC status; return the result."""
    code = result.get("error_code")
    if code is None:
        return result
    if code == "stdin_unsupported":
        raise _RpcError(
            StatusCode.UNIMPLEMENTED,
            "exec stdin is not supported by the Ray Sandbox gRPC facade",
        )
    if code == "offset_mismatch":
        raise _RpcError(StatusCode.FAILED_PRECONDITION, "stdin offset mismatch")
    if code == "too_large":
        raise _RpcError(
            StatusCode.RESOURCE_EXHAUSTED,
            f"file writes are capped at {settings.max_file_bytes} bytes "
            "(max_file_bytes)",
        )
    raise _RpcError(StatusCode.NOT_FOUND, result.get("message", "exec not found"))


def _sandbox_spec(
    settings: SandboxAPISettings,
    image: str,
    network: str,
    env: Optional[Dict[str, str]] = None,
    workdir: Optional[str] = None,
    ttl_seconds: Optional[int] = None,
    cpu_limit: Optional[float] = None,
    memory_limit_mb: Optional[int] = None,
    labels: Optional[Dict[str, str]] = None,
) -> SandboxSpec:
    """The host spec for a facade create (and for the warm pool's templates)."""
    return {
        "image": image,
        "env": dict(env or {}),
        "workdir": workdir,
        "ttl_seconds": ttl_seconds,
        "network": network,
        "dns": None,
        "shell": "/bin/bash",
        "rootless": True,
        "readonly": False,
        "capabilities": list(settings.default_capabilities),
        "cpu_limit": cpu_limit,
        "memory_limit_mb": memory_limit_mb,
        "image_pull_timeout_seconds": _PULL_TIMEOUT_SECONDS,
        "start_timeout_seconds": _START_TIMEOUT_SECONDS,
        "labels": dict(labels or {}),
    }


def _terminated() -> Any:
    return api_pb2.GenericResult(status=api_pb2.GenericResult.GENERIC_STATUS_TERMINATED)


def _router_url_for(metadata: Any) -> str:
    """The command-router URL for a client of a facade with none configured.

    The client SDK names the host it dialed in ``x-modal-host`` on every
    control-plane call (gRPC servers don't see the ``:authority`` it also
    sends). Behind a TLS endpoint on the default port, such as an ingress in
    front of Ray Serve, ``https://`` plus that host reaches this same server.
    A client can only point its own router calls elsewhere with it.
    """
    host = metadata.get("x-modal-host") if metadata is not None else None
    if not isinstance(host, str) or not _DIALED_HOST.fullmatch(host):
        raise _RpcError(
            StatusCode.FAILED_PRECONDITION,
            "the facade has no command-router URL for this client: configure "
            "the facade's advertise_url, or use a client that sends the host "
            "it dialed (x-modal-host)",
        )
    return f"https://{host}"


def _carries_token(metadata: Any, token: str) -> bool:
    """True when a call's metadata presents ``token``.

    The client SDK sends its token secret on control-plane calls and
    ``authorization: Bearer <jwt>`` on command-router calls, where the jwt
    is the one ``TaskGetCommandRouterAccess`` handed out: the token itself.
    Either header may carry it, and the bearer form must match exactly, as
    in the REST app. A header that is missing or not a string counts as
    absent.
    """

    def presented(key: str) -> bytes:
        value = metadata.get(key) if metadata is not None else None
        return value.encode("utf-8") if isinstance(value, str) else b""

    expected = token.encode("utf-8")
    secret_ok = hmac.compare_digest(presented("x-modal-token-secret"), expected)
    bearer_ok = hmac.compare_digest(presented("authorization"), b"Bearer " + expected)
    return secret_ok or bearer_ok


def _metadata(grpc_context: Any) -> Dict[str, Any]:
    """A call's metadata, with the first value of each key."""
    metadata: Dict[str, Any] = {}
    for key, value in grpc_context.invocation_metadata() or ():
        metadata.setdefault(key, value)
    return metadata


def _authenticate(token: Optional[str], grpc_context: Any, rpc: str) -> None:
    """Reject a call that does not present ``token``, when there is one."""
    if token is not None and not _carries_token(_metadata(grpc_context), token):
        # No header values in the log: a client set up for another server
        # may present real credentials of its own.
        logger.debug("Rejected an unauthenticated call to %s", rpc)
        raise _RpcError(StatusCode.UNAUTHENTICATED, "invalid or missing API token")


async def _abort(grpc_context: Any, error: _RpcError) -> NoReturn:
    """End the call being served with ``error``'s status."""
    abort = getattr(grpc_context, "abort", None)
    if abort is not None:
        # grpc.aio: abort() raises, ending the call with this status.
        await abort(error.code, error.message)
    # Ray Serve's context has no abort(): a call that raises ends with the
    # status set on the context.
    grpc_context.set_code(error.code)
    grpc_context.set_details(error.message)
    raise error


def _rpc(handler: Callable) -> Callable:
    """Serve a servicer method as an RPC, on ``grpc.aio`` or Ray Serve.

    Both call the method named after the RPC with the request (an async
    iterator of requests for a client-streaming RPC) and the call's context,
    which Ray Serve passes only to a parameter named ``grpc_context``. The
    call must present the facade's token, when it has one, before the
    handler runs, so a rejected call never reaches a sandbox or the cluster.
    An ``_RpcError`` from the handler ends the call with its status.
    """
    if inspect.isasyncgenfunction(handler):

        @functools.wraps(handler)
        async def streaming(
            self: Any, request: Any, grpc_context: Any
        ) -> AsyncIterator[Any]:
            try:
                _authenticate(self._state.token, grpc_context, handler.__name__)
                async for reply in handler(self, request, grpc_context):
                    yield reply
            except _RpcError as error:
                await _abort(grpc_context, error)

        return streaming

    @functools.wraps(handler)
    async def unary(self: Any, request: Any, grpc_context: Any) -> Any:
        try:
            _authenticate(self._state.token, grpc_context, handler.__name__)
            return await handler(self, request, grpc_context)
        except _RpcError as error:
            await _abort(grpc_context, error)

    return unary


class _ControlServicer(ModalClientServicer):
    """Control-plane RPCs: apps, images, secrets, sandbox lifecycle."""

    _state: _FacadeState

    @_rpc
    async def ClientHello(self, request: Any, grpc_context: Any) -> Any:
        return api_pb2.ClientHelloResponse()

    @_rpc
    async def AppGetOrCreate(self, request: Any, grpc_context: Any) -> Any:
        return api_pb2.AppGetOrCreateResponse(
            app_id=_encode_id("ap-", request.app_name or "default")
        )

    @_rpc
    async def EnvironmentGetOrCreate(self, request: Any, grpc_context: Any) -> Any:
        name = request.deployment_name or "main"
        return api_pb2.EnvironmentGetOrCreateResponse(
            environment_id=_encode_id("en-", name),
            metadata=api_pb2.EnvironmentMetadata(
                name=name,
                # From 2025.06 the SDK mounts its own dependencies at
                # runtime, so a registry image reduces to a bare FROM.
                settings=api_pb2.EnvironmentSettings(image_builder_version="2025.06"),
            ),
        )

    @_rpc
    async def SecretGetOrCreate(self, request: Any, grpc_context: Any) -> Any:
        anonymous_types = (
            api_pb2.OBJECT_CREATION_TYPE_ANONYMOUS_OWNED_BY_APP,
            api_pb2.OBJECT_CREATION_TYPE_EPHEMERAL,
        )
        if request.object_creation_type not in anonymous_types:
            raise _RpcError(
                StatusCode.UNIMPLEMENTED,
                "only anonymous or ephemeral secrets (an inline dict) are "
                "supported by the Ray Sandbox gRPC facade",
            )
        return api_pb2.SecretGetOrCreateResponse(
            secret_id=_encode_id("st-", dict(request.env_dict))
        )

    @_rpc
    async def ImageGetOrCreate(self, request: Any, grpc_context: Any) -> Any:
        ref = _image_ref_from_dockerfile(request.image)
        return api_pb2.ImageGetOrCreateResponse(
            image_id=_encode_id("im-", ref),
            metadata=api_pb2.ImageMetadata(
                image_builder_version=request.builder_version
            ),
        )

    @_rpc
    async def ImageJoinStreaming(
        self, request: Any, grpc_context: Any
    ) -> AsyncIterator[Any]:
        # Images are pulled at sandbox boot, so the "build" is already done.
        yield api_pb2.ImageJoinStreamingResponse(
            result=api_pb2.GenericResult(
                status=api_pb2.GenericResult.GENERIC_STATUS_SUCCESS
            )
        )

    @_rpc
    async def SandboxCreate(self, request: Any, grpc_context: Any) -> Any:
        sandbox_id = await self._create_sandbox(request)
        return api_pb2.SandboxCreateResponse(
            sandbox_id=sandbox_id,
            metadata=api_pb2.SandboxHandleMetadata(app_id=request.app_id),
        )

    SandboxCreateV2 = SandboxCreate

    async def _create_sandbox(self, request: Any) -> str:
        state = self._state
        settings = state.settings
        definition = request.definition

        if definition.name:
            # Names are unique per app while the sandbox lives, so a named
            # create is idempotent: retries converge on the existing actor.
            sandbox_id = _new_sandbox_id(f"{request.app_id}/{definition.name}")
            existing = await state.lookup(sandbox_id)
            if existing is not None:
                if await _is_alive(existing):
                    # boot() is idempotent: sending it again covers a first
                    # create whose boot call never reached the actor.
                    existing.boot.remote()
                    return sandbox_id
                # The previous actor died (node loss, OOM); recreate it.
                await state.kill(sandbox_id, existing)
        else:
            sandbox_id = _new_sandbox_id()

        network_type = definition.network_access.network_access_type
        if network_type == api_pb2.NetworkAccess.ALLOWLIST:
            # Refused, not widened: a sandbox created with open egress in
            # place of the allowlist the client asked for would run with
            # fewer restrictions than the client believes it has.
            raise _RpcError(
                StatusCode.INVALID_ARGUMENT,
                "network allowlists (cidr_allowlist) are not supported by the "
                "Ray Sandbox gRPC facade; use block_network=True for no "
                "network, or omit both for open egress",
            )
        network = "none" if network_type == api_pb2.NetworkAccess.BLOCKED else "public"

        resources = definition.resources
        cpu_request = resources.milli_cpu / 1000.0 if resources.milli_cpu else None
        cpu_limit = (
            resources.milli_cpu_max / 1000.0 if resources.milli_cpu_max else None
        )
        memory_request_mb = resources.memory_mb or None
        memory_limit_mb = resources.memory_mb_max or None

        num_cpus = cpu_request or cpu_limit or settings.default_actor_num_cpus
        actor_options: Dict[str, Any] = {"num_cpus": num_cpus}
        request_mb = memory_request_mb or memory_limit_mb
        if request_mb is not None:
            actor_options["memory"] = request_mb * 1024 * 1024

        ttl = settings.max_ttl_seconds
        if definition.timeout_secs:
            ttl = min(definition.timeout_secs, ttl)

        # entrypoint_args are ignored: SandboxHost keeps the sandbox alive.
        spec = _sandbox_spec(
            settings,
            image=_decode_id("im-", definition.image_id),
            network=network,
            env=_env_from_secret_ids(definition.secret_ids),
            workdir=definition.workdir or None,
            ttl_seconds=ttl,
            cpu_limit=cpu_limit,
            memory_limit_mb=memory_limit_mb,
            labels={tag.tag_name: tag.tag_value for tag in request.tags},
        )
        host_settings: HostSettings = {
            "max_output_bytes": settings.max_output_bytes,
            "max_exec_history": settings.max_exec_history,
            "max_file_bytes": settings.max_file_bytes,
        }
        ctor_kwargs = {
            "sandbox_id": sandbox_id,
            "spec": spec,
            "settings": host_settings,
        }
        # Named sandboxes are get-or-create so retries converge; a fresh
        # random id needs no lookup.
        get_if_exists = bool(definition.name)
        acreate = getattr(state.resolver, "acreate", None)
        if acreate is not None:
            try:
                handle = await acreate(
                    sandbox_id, actor_options, ctor_kwargs, get_if_exists=get_if_exists
                )
            except asyncio.TimeoutError:
                raise _RpcError(
                    StatusCode.RESOURCE_EXHAUSTED,
                    "no node has room for the sandbox's resources yet",
                )
            if isinstance(handle, HostedSandboxHandle):
                # A hosted sandbox's id names its node, so the resolver picks it.
                sandbox_id = handle.sandbox_id
        else:
            handle = await asyncio.to_thread(
                state.resolver.create,
                sandbox_id,
                actor_options,
                ctor_kwargs,
                get_if_exists=get_if_exists,
            )
        if not isinstance(handle, HostedSandboxHandle):
            # A node host starts the boot when it adds the sandbox.
            handle.boot.remote()
        logger.info(
            "Created sandbox %s (image=%s, network=%s)",
            sandbox_id,
            spec["image"],
            network,
        )
        return sandbox_id

    @_rpc
    async def SandboxGetTaskId(self, request: Any, grpc_context: Any) -> Any:
        handle = await self._state.require_handle(request.sandbox_id)
        try:
            info = await _bounded(
                handle.describe.remote(wait_seconds=1.0), extra_wait=1.0
            )
        except _RpcError as exc:
            if exc.code != StatusCode.UNAVAILABLE:
                raise
            # An unscheduled actor looks like a booting one to the client:
            # an empty task id keeps the SDK polling.
            return api_pb2.SandboxGetTaskIdResponse(task_id="")
        _check_found(info)
        status = info["status"]
        if status in ("error", "terminated"):
            raise _RpcError(
                StatusCode.FAILED_PRECONDITION,
                f"sandbox {request.sandbox_id} is {status}: "
                f"{info.get('error') or 'no longer running'}",
            )
        # The sandbox id doubles as the task id once running; an empty task
        # id while pulling or starting makes the SDK poll.
        task_id = request.sandbox_id if status == "running" else ""
        return api_pb2.SandboxGetTaskIdResponse(task_id=task_id)

    SandboxGetTaskIdV2 = SandboxGetTaskId

    @_rpc
    async def SandboxWait(self, request: Any, grpc_context: Any) -> Any:
        handle = await self._state.lookup(request.sandbox_id)
        loop = asyncio.get_running_loop()
        # timeout=0 is the SDK's non-blocking poll(); wait() loops with 10s.
        # A poll must come back at once even while the sandbox boots or its
        # actor is still being scheduled, so it neither long-polls the
        # describe nor waits out the scheduling grace.
        polling = request.timeout == 0
        describe_wait = 0.0 if polling else 1.0
        deadline = loop.time() + min(request.timeout, _WAIT_MAX_SECONDS)
        result = None
        while True:
            if handle is None:
                result = _terminated()
                break
            try:
                info = await _bounded(
                    handle.describe.remote(wait_seconds=describe_wait),
                    extra_wait=describe_wait,
                    grace=_POLL_GRACE_SECONDS if polling else None,
                )
            except _RpcError as exc:
                if exc.code == StatusCode.NOT_FOUND:
                    result = _terminated()
                    break
                info = None  # Unreachable actor: still pending.
            if info is not None and info["status"] == "terminated":
                result = _terminated()
                break
            if info is not None and info["status"] == "error":
                result = api_pb2.GenericResult(
                    status=api_pb2.GenericResult.GENERIC_STATUS_FAILURE,
                    exception=info.get("error") or "sandbox failed",
                )
                break
            if loop.time() >= deadline:
                break
            await asyncio.sleep(1.0)
        response = api_pb2.SandboxWaitResponse()
        if result is not None:
            response.result.CopyFrom(result)
        return response

    @_rpc
    async def SandboxTerminate(self, request: Any, grpc_context: Any) -> Any:
        state = self._state
        handle = await state.lookup(request.sandbox_id)
        if handle is not None:
            try:
                await _bounded(
                    handle.terminate.remote(), extra_wait=_TERMINATE_WAIT_SECONDS
                )
            except _RpcError as exc:
                if exc.code == StatusCode.UNAVAILABLE:
                    # Still being scheduled, or mid-way through a slow
                    # teardown (a container that was starting is waited for
                    # and deleted first): the queued terminate() finishes the
                    # job and the host exits on its own, as in the REST
                    # DELETE. Killing it now could orphan the container.
                    logger.info("Sandbox %s is terminating", request.sandbox_id)
                    return api_pb2.SandboxTerminateResponse()
                logger.debug("terminate(%s): %s", request.sandbox_id, exc)
            except Exception as exc:
                logger.debug("terminate(%s): %s", request.sandbox_id, exc)
            # terminate() only deletes the sandbox; killing the actor
            # releases its cluster reservation (as in the REST DELETE).
            await state.kill(request.sandbox_id, handle)
            logger.info("Terminated sandbox %s", request.sandbox_id)
        return api_pb2.SandboxTerminateResponse()

    SandboxTerminateV2 = SandboxTerminate

    @_rpc
    async def TaskGetCommandRouterAccess(self, request: Any, grpc_context: Any) -> Any:
        url = self._state.advertise_url or _router_url_for(_metadata(grpc_context))
        return api_pb2.TaskGetCommandRouterAccessResponse(
            url=url,
            # The SDK presents this as "Bearer <jwt>" on every
            # command-router call, so with a token configured it is the
            # token itself, which this caller has just presented; that
            # holds only while the facade has one shared credential.
            # Not a parseable JWT (the docs ask for an opaque token), so
            # the SDK applies no client-side expiry and only refreshes on
            # UNAUTHENTICATED.
            jwt=self._state.token or _ROUTER_JWT_WITHOUT_TOKEN,
        )


class _RouterServicer(TaskCommandRouterServicer):
    """Exec-plane RPCs: start, stdio, stdin, poll, and wait.

    The client SDK reaches this service at the URL handed out by
    ``TaskGetCommandRouterAccess``, which here is the same server. Every
    request names its sandbox (``task_id``), and the sandbox's host keeps
    each exec under the client's exec id, so the facade holds no exec state
    and any of its replicas can serve any call.
    """

    _state: _FacadeState
    _starts: Set["asyncio.Task[Any]"]

    async def _handle(self, task_id: str) -> Any:
        if not task_id:
            raise _RpcError(StatusCode.INVALID_ARGUMENT, "request names no task_id")
        return await self._state.require_handle(task_id)

    @_rpc
    async def TaskExecStart(self, request: Any, grpc_context: Any) -> Any:
        handle = await self._handle(request.task_id)
        logger.debug(
            "exec %s on %s: %s",
            request.exec_id,
            request.task_id,
            list(request.command_args)[:2],
        )
        # The start runs to completion as its own task even if the client
        # stops waiting (UNAVAILABLE below, a dropped connection): the host
        # joins a retried start with the same exec id to this one, so the
        # command never runs twice.
        start = asyncio.ensure_future(self._start(handle, request))
        self._starts.add(start)
        start.add_done_callback(self._start_done)
        try:
            started = await asyncio.wait_for(
                asyncio.shield(start), timeout=_SCHEDULING_GRACE_SECONDS
            )
        except asyncio.TimeoutError:
            raise _RpcError(
                StatusCode.UNAVAILABLE,
                "sandbox actor is not reachable; the cluster may still be scaling",
            )
        _check_found(started)
        if started.get("error_code"):
            raise _RpcError(
                StatusCode.FAILED_PRECONDITION,
                started.get("message", "sandbox is not running"),
            )
        return sr_pb2.TaskExecStartResponse()

    def _start_done(self, start: "asyncio.Task") -> None:
        self._starts.discard(start)
        # Retrieved here, since a client that stopped waiting (UNAVAILABLE
        # above) never awaits it; its retry starts afresh or joins the host's.
        if not start.cancelled() and start.exception() is not None:
            logger.debug("An exec start failed: %r", start.exception())

    async def _start(self, handle: Any, request: Any) -> Dict[str, Any]:
        command = list(request.command_args)
        if command and command[0] == _FS_TOOLS_PATH:
            return await self._start_fs_op(handle, request.exec_id, command)
        env = dict(request.env)
        env.update(_env_from_secret_ids(request.secret_ids))
        return await _unbounded(
            handle.start_exec.remote(
                command,
                cwd=request.workdir or None,
                env=env or None,
                timeout_seconds=request.timeout_secs or None,
                exec_key=request.exec_id,
            )
        )

    async def _start_fs_op(
        self, handle: Any, exec_key: str, command: List[str]
    ) -> Dict[str, Any]:
        """Start one emulated filesystem-tools invocation as a host exec."""
        try:
            op = json.loads(command[1]) if len(command) > 1 else {}
        except ValueError:
            op = {}
        if not isinstance(op, dict) or len(op) != 1:
            raise _RpcError(
                StatusCode.INVALID_ARGUMENT,
                f"unrecognized fs-tools command: {command[1:]}",
            )
        ((name, payload),) = op.items()
        if not isinstance(payload, dict):
            raise _RpcError(
                StatusCode.INVALID_ARGUMENT,
                f"unrecognized fs-tools payload for {name}: {payload!r}",
            )
        path = payload.get("path", "")
        if name == "WriteFile":
            # Content arrives over stdin; the host writes it at stdin EOF.
            return await _unbounded(handle.fs_write_open.remote(exec_key, path))
        if name == "ReadFile":
            return await _bounded(
                handle.fs_read.remote(exec_key, path), extra_wait=_READ_BOUND_SECONDS
            )
        if name == "ListFiles":
            # A shell probe that reports only the typed errors (NotFound,
            # NotDirectory) the SDK's existence and directory checks need;
            # the entry list itself is empty.
            quoted = shlex.quote(path)
            not_found = shlex.quote(
                _fs_error("NotFound", "path does not exist").decode()
            )
            not_dir = shlex.quote(
                _fs_error("NotDirectory", "path is not a directory").decode()
            )
            probe = (
                f"if [ ! -e {quoted} ]; then printf %s {not_found} >&2; exit 1; "
                f"elif [ ! -d {quoted} ]; then printf %s {not_dir} >&2; exit 1; "
                f"else printf '[]'; fi"
            )
            return await _unbounded(
                handle.start_exec.remote(["/bin/sh", "-c", probe], exec_key=exec_key)
            )
        raise _RpcError(
            StatusCode.UNIMPLEMENTED,
            f"fs-tools operation {name!r} is not supported by the "
            "Ray Sandbox gRPC facade",
        )

    async def _job(
        self, task_id: str, exec_id: str, wait_seconds: float = 0.0
    ) -> Dict[str, Any]:
        handle = await self._handle(task_id)
        info = await _bounded(
            handle.get_exec_by_key.remote(exec_id, wait_seconds=wait_seconds),
            extra_wait=wait_seconds,
        )
        if info.get("error_code"):
            raise _RpcError(StatusCode.NOT_FOUND, info.get("message", "exec not found"))
        return info

    async def _finished_job(self, task_id: str, exec_id: str) -> Dict[str, Any]:
        while True:
            info = await self._job(task_id, exec_id, _LONG_POLL_SECONDS)
            if info["status"] != "running":
                return info

    @_rpc
    async def TaskExecStdioRead(
        self, request: Any, grpc_context: Any
    ) -> AsyncIterator[Any]:
        info = await self._finished_job(request.task_id, request.exec_id)
        want_stdout = (
            request.file_descriptor == sr_pb2.TASK_EXEC_STDIO_FILE_DESCRIPTOR_STDOUT
        )
        data = _job_stream(info, want_stdout)[request.offset :]
        for start in range(0, len(data), _STDIO_CHUNK_BYTES):
            yield sr_pb2.TaskExecStdioReadResponse(
                data=data[start : start + _STDIO_CHUNK_BYTES]
            )

    @_rpc
    async def TaskExecStdinWrite(self, request: Any, grpc_context: Any) -> Any:
        handle = await self._handle(request.task_id)
        _check_stdin(
            await _bounded(
                handle.stdin_write.remote(
                    request.exec_id, request.data, request.offset, request.eof
                ),
                extra_wait=_WRITE_BOUND_SECONDS,
            ),
            self._state.settings,
        )
        return sr_pb2.TaskExecStdinWriteResponse()

    @_rpc
    async def TaskExecStdinWriteStream(
        self, requests: AsyncIterator[Any], grpc_context: Any
    ) -> Any:
        request = await anext(requests, None)
        if request is None or request.WhichOneof("payload") != "start":
            raise _RpcError(
                StatusCode.INVALID_ARGUMENT, "first stdin stream message must be start"
            )
        start = request.start
        handle = await self._handle(start.task_id)
        status = _check_stdin(
            await _bounded(handle.stdin_status.remote(start.exec_id)),
            self._state.settings,
        )
        if start.offset != status["num_bytes_written"]:
            raise _RpcError(StatusCode.FAILED_PRECONDITION, "stdin offset mismatch")
        offset = start.offset
        async for request in requests:
            which = request.WhichOneof("payload")
            if which not in ("data", "end"):
                raise _RpcError(
                    StatusCode.INVALID_ARGUMENT,
                    "stdin stream message must contain data",
                )
            data = request.data if which == "data" else b""
            status = _check_stdin(
                await _bounded(
                    handle.stdin_write.remote(
                        start.exec_id, data, offset, which == "end"
                    ),
                    extra_wait=_WRITE_BOUND_SECONDS,
                ),
                self._state.settings,
            )
            offset = status["num_bytes_written"]
            if which == "end":
                break
        return sr_pb2.TaskExecStdinWriteStreamResponse()

    @_rpc
    async def TaskExecStdinStatus(self, request: Any, grpc_context: Any) -> Any:
        handle = await self._handle(request.task_id)
        status = _check_stdin(
            await _bounded(handle.stdin_status.remote(request.exec_id)),
            self._state.settings,
        )
        return sr_pb2.TaskExecStdinStatusResponse(
            num_bytes_written=status["num_bytes_written"], closed=status["closed"]
        )

    @_rpc
    async def TaskExecPoll(self, request: Any, grpc_context: Any) -> Any:
        info = await self._job(request.task_id, request.exec_id)
        response = sr_pb2.TaskExecPollResponse()
        if info["status"] != "running":
            response.code = _job_exit_code(info)
        return response

    @_rpc
    async def TaskExecWait(self, request: Any, grpc_context: Any) -> Any:
        info = await self._finished_job(request.task_id, request.exec_id)
        return sr_pb2.TaskExecWaitResponse(code=_job_exit_code(info))

    @_rpc
    async def TaskSetNetworkAccess(self, request: Any, grpc_context: Any) -> Any:
        # Refused, not acknowledged: a client that believes it changed the
        # sandbox's network policy must not keep running under the old one.
        raise _RpcError(
            StatusCode.UNIMPLEMENTED,
            "TaskSetNetworkAccess is not supported by the Ray Sandbox gRPC "
            "facade: network policy is fixed when the sandbox is created",
        )


def _prestart(resolver: Any) -> None:
    try:
        logger.info("Started %d sandbox node hosts", resolver.prestart())
    except Exception as exc:
        logger.warning("Failed to prestart sandbox node hosts: %s", exc)


@DeveloperAPI
class RaySandboxFacade(_ControlServicer, _RouterServicer):
    """The facade's gRPC servicer: both services, sharing one state.

    Serve it with ``serve`` (``grpc.aio``) or deploy it with Ray Serve
    (``grpc_app``); ``add_servicers_to_server`` registers its services with
    either server. When the environment variable named by
    ``settings.token_env_var`` is set, every RPC must present that token.

    Args:
        settings: Server settings; defaults are production-safe.
        handle_resolver: Test seam, same surface as in ``create_app``.
        advertise_url: Command-router URL handed to clients; must route
            back to this same server. When None, each client gets
            ``https://`` plus the host it dialed, which suits a facade
            behind a TLS endpoint on the default port.
    """

    def __init__(
        self,
        settings: Optional[SandboxAPISettings] = None,
        *,
        handle_resolver: Optional[Any] = None,
        advertise_url: Optional[str] = None,
    ) -> None:
        settings = settings or SandboxAPISettings()
        resolver = handle_resolver or RayActorHandleResolver(settings)
        if settings.warm_pool and hasattr(resolver, "warm_templates"):
            # Booted from the same spec a matching create would get.
            resolver.warm_templates = [
                {
                    "spec": _sandbox_spec(
                        settings,
                        image=profile["image"],
                        network=profile.get("network", "none"),
                    ),
                    "size": int(profile.get("size", 0)),
                    # Optional: reserve each pool's CPU (size x cpu) while it
                    # fills.
                    "reserve": (
                        {"CPU": float(profile["cpu"]) * int(profile.get("size", 0))}
                        if profile.get("cpu")
                        else None
                    ),
                }
                for profile in settings.warm_pool
            ]
        if settings.host_mode == "node" and hasattr(resolver, "prestart"):
            # Node hosts start in the background, so the first sandbox on
            # each node doesn't wait for one.
            threading.Thread(target=_prestart, args=(resolver,), daemon=True).start()
        token = os.environ.get(settings.token_env_var) or None
        self._state = _FacadeState(resolver, settings, advertise_url, token)
        # Exec starts that outlive their call (see TaskExecStart); the loop
        # keeps only weak references to tasks.
        self._starts = set()


@PublicAPI(stability="alpha")
def add_servicers_to_server(servicer: Any, server: Any) -> None:
    """Register the facade's gRPC services with a server.

    Registers the client SDK's control-plane and command-router services
    under their wire names. To deploy the facade with Ray Serve, list this
    function in ``grpc_options.grpc_servicer_functions`` (see ``grpc_app``).

    Args:
        servicer: The object whose methods handle the RPCs, such as a
            ``RaySandboxFacade``. Ray Serve passes a placeholder and routes
            each call to its application's ingress.
        server: The ``grpc`` server to register the services with.
    """
    add_ModalClientServicer_to_server(servicer, server)
    add_TaskCommandRouterServicer_to_server(servicer, server)


_SERVER_OPTIONS = [
    # The client SDK can send a file write as one message, past gRPC's 4 MiB
    # default; the facade caps files itself (max_file_bytes).
    ("grpc.max_receive_message_length", -1),
    ("grpc.max_send_message_length", -1),
    # Fail to start, rather than share the port, when another process
    # listens on it.
    ("grpc.so_reuseport", 0),
]


async def serve(host: str, port: int, facade: RaySandboxFacade) -> None:
    """Serve ``facade`` on ``host:port`` until the server stops."""
    server = grpc.aio.server(options=_SERVER_OPTIONS)
    add_servicers_to_server(facade, server)
    address = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    server.add_insecure_port(address)
    await server.start()
    logger.info("Ray Sandbox gRPC facade listening on %s", address)
    await server.wait_for_termination()


def _is_loopback(host: str) -> bool:
    """True for ``localhost`` or a literal loopback address."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        # A hostname, or "" (every interface): not provably loopback.
        return False


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument(
        "--advertise-url",
        default=None,
        help="Command-router URL handed to clients (default http://HOST:PORT)",
    )
    parser.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        help=(
            "Serve a non-loopback --host without a token. Only for a facade "
            "that nothing but an authenticating proxy can reach."
        ),
    )
    args = parser.parse_args(argv)
    advertise = args.advertise_url or f"http://{args.host}:{args.port}"

    settings = SandboxAPISettings()
    authenticated = bool(os.environ.get(settings.token_env_var))
    exposed = not authenticated and not _is_loopback(args.host)
    if exposed and not args.allow_unauthenticated:
        # Refused before connecting to the cluster.
        parser.error(
            f"refusing to serve {args.host!r} without authentication: set "
            f"{settings.token_env_var}, bind a loopback address, or pass "
            "--allow-unauthenticated if only an authenticating proxy can "
            "reach the facade"
        )

    logging.basicConfig(level=logging.INFO)
    if authenticated:
        logger.info("Requiring the API token from %s", settings.token_env_var)
    elif exposed:
        logger.warning(
            "Serving %r without authentication (--allow-unauthenticated)", args.host
        )
    import ray

    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), ignore_reinit_error=True)
    facade = RaySandboxFacade(settings, advertise_url=advertise)
    asyncio.run(serve(args.host, args.port, facade))


if __name__ == "__main__":
    main()
