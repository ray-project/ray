"""Wire-compatible gRPC facade over Ray Sandbox.

Implements the subset of a sandbox client's control plane (``ModalClient``)
and exec plane (``TaskCommandRouter``) gRPC services that the client SDK uses
for its Sandbox API, backed by the same detached ``SandboxHost`` actors as the
REST API in ``app.py``. An unmodified client pointed at this server can create
sandboxes, exec commands, and use the filesystem API against a Ray cluster.

The wire contract (service names, method names, message field numbers) is
vendored under ``_proto/`` as a minimal, self-contained protobuf/grpclib stub
set, so the facade needs no third-party client package to build or test.

Design notes:

- The facade is stateless where possible: object ids carry their own payload
  (``im-<b64 docker ref>`` for images, ``st-<b64 env json>`` for secrets, and
  the sandbox id doubles as the client task id), so no registry survives a
  restart and any state a request needs travels inside the request.
- Exec state is the one exception: the router keeps an in-process table
  mapping client exec ids to SandboxHost exec jobs and to in-flight filesystem
  operations. Run a single facade process per cluster.
- The client's ``sandbox.filesystem`` API is implemented client-side on top of
  ``exec`` of a helper binary (``/__modal/.bin/modal-sandbox-fs-tools``).
  Sandbox images do not contain that binary; the router recognizes its argv
  and emulates the WriteFile / ReadFile / ListFiles commands with SandboxHost
  file operations instead of running anything. ListFiles reports an empty
  entry list — enough for existence and is-directory probes, which is all the
  SDK's typed errors need.

Requires ``grpclib`` (the async gRPC server framework the facade is built on);
run with ``python -m ray.experimental.sandbox.http.grpc_facade``.
"""

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import shlex
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ray.experimental.sandbox.http.app import (
    SANDBOX_ID_PREFIX,
    RayActorHandleResolver,
)
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import DeveloperAPI

try:
    from grpclib import GRPCError, Status
    from grpclib.server import Server

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

from ray.experimental.sandbox.http._proto import (
    sandbox_control_pb2 as api_pb2,
    sandbox_exec_pb2 as sr_pb2,
)

logger = logging.getLogger(__name__)

# Path of the helper binary the client SDK execs for its filesystem API; execs
# of this argv are emulated, never run. This is the SDK's own wire constant.
_FS_TOOLS_PATH = "/__modal/.bin/modal-sandbox-fs-tools"

# Exit codes for exec jobs that did not produce one of their own.
_EXIT_TIMEOUT = 124
_EXIT_ERROR = 126

_LONG_POLL_SECONDS = 15.0

# Filesystem writes are flushed to the sandbox in bounded slices so one huge
# upload cannot outrun the per-call scheduling bound.
_FS_WRITE_CHUNK_BYTES = 4 * 1024 * 1024
_FS_WRITE_BOUND_SECONDS = 120.0


def _new_sandbox_id(token: Optional[str] = None) -> str:
    """Mint a sandbox id in the client's V1 id shape: ``sb-`` + 22 base62 chars.

    The SDK routes ids of any other shape to its V2 backend, which requires
    auth-token RPCs this facade does not serve. Hex is a base62 subset.
    """
    if token is not None:
        suffix = hashlib.sha256(token.encode("utf-8")).hexdigest()[:22]
    else:
        suffix = uuid.uuid4().hex[:22]
    return f"{SANDBOX_ID_PREFIX}{suffix}"


def _encode_id(prefix: str, payload: Any) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return prefix + base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_id(prefix: str, value: str) -> Any:
    if not value.startswith(prefix):
        raise GRPCError(Status.INVALID_ARGUMENT, f"malformed id: {value!r}")
    data = value[len(prefix) :]
    data += "=" * (-len(data) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(data))
    except (ValueError, TypeError):
        raise GRPCError(Status.INVALID_ARGUMENT, f"malformed id: {value!r}")


def _fill_unimplemented(cls: type) -> type:
    """Give every unimplemented RPC of a grpclib service base a clear error."""

    for name in sorted(getattr(cls, "__abstractmethods__", ())):

        async def _unimplemented(self, stream: Any, _name: str = name) -> None:
            await stream.recv_message()
            raise GRPCError(
                Status.UNIMPLEMENTED,
                f"{_name} is not supported by the Ray Sandbox gRPC facade",
            )

        setattr(cls, name, _unimplemented)
    cls.__abstractmethods__ = frozenset()
    return cls


def _image_ref_from_proto(image: Any) -> str:
    """Extract the registry ref from a registry Image proto.

    Anything beyond a single FROM (plus harmless metadata commands) means the
    client asked for a server-side image build, which Ray Sandbox does not do.
    """
    ref = None
    for line in image.dockerfile_commands:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        keyword = stripped.split(None, 1)[0].upper()
        if keyword == "FROM":
            if ref is not None:
                raise GRPCError(
                    Status.INVALID_ARGUMENT,
                    "multi-stage image builds are not supported by the "
                    "Ray Sandbox gRPC facade",
                )
            ref = stripped.split(None, 1)[1].strip()
        elif keyword in ("ENTRYPOINT", "CMD", "ENV", "WORKDIR", "LABEL"):
            # Metadata-only commands; sandbox creation overrides them anyway.
            continue
        else:
            raise GRPCError(
                Status.INVALID_ARGUMENT,
                f"image build step {stripped!r} is not supported: the "
                "Ray Sandbox gRPC facade only runs prebuilt registry images "
                "(a single FROM line)",
            )
    if ref is None:
        raise GRPCError(
            Status.INVALID_ARGUMENT, "image definition contains no FROM line"
        )
    return ref


def _env_from_secret_ids(secret_ids: Any) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for secret_id in secret_ids:
        env.update(_decode_id("st-", secret_id))
    return env


def _sanitize_stderr(message: str) -> bytes:
    return message.encode("utf-8", errors="replace")


def _fs_error(error_kind: str, message: str) -> bytes:
    return json.dumps({"error_kind": error_kind, "message": message}).encode()


@dataclass
class _ExecRecord:
    """One client exec id and how the facade services it."""

    kind: str  # "host" | "fs"
    handle: Any
    host_exec_id: Optional[str] = None
    # Filesystem ops (kind == "fs") resolve to buffered results.
    fs_path: Optional[str] = None
    fs_write: bool = False
    stdin_chunks: List[bytes] = field(default_factory=list)
    stdin_bytes: int = 0
    stdin_closed: bool = False
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    exit_code: int = 0
    stdout: bytes = b""
    stderr: bytes = b""

    def finish(self, exit_code: int, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.finished.set()


class _FacadeState:
    """State shared between the control-plane and exec-plane servicers."""

    def __init__(
        self, resolver: Any, settings: SandboxAPISettings, advertise_url: str
    ) -> None:
        self.resolver = resolver
        self.settings = settings
        self.advertise_url = advertise_url
        self.execs: Dict[str, _ExecRecord] = {}

    def require_handle(self, sandbox_id: str) -> Any:
        handle = self.resolver.get(sandbox_id)
        if handle is None:
            raise GRPCError(Status.NOT_FOUND, f"sandbox {sandbox_id!r} not found")
        return handle

    def require_exec(self, exec_id: str) -> _ExecRecord:
        record = self.execs.get(exec_id)
        if record is None:
            raise GRPCError(Status.NOT_FOUND, f"exec {exec_id!r} not found")
        return record


# How long past its own long-poll budget an actor call may take before the
# actor is considered unreachable (unscheduled on a scaling cluster, or on a
# lost node). Mirrors the REST app's _bounded_call.
_SCHEDULING_GRACE_SECONDS = 10.0


async def _bounded(awaitable: Any, extra_wait: float = 0.0) -> Any:
    """Await a SandboxHost call, but never block behind actor scheduling.

    A named detached actor exists the moment it is created, but on a cluster
    that is still scaling it may not be *scheduled* for a long time — and a
    call to an unscheduled actor blocks indefinitely. Bounding every call
    keeps that pressure visible on the wire (UNAVAILABLE, which the client
    SDK retries) instead of hanging the client silently.

    A dead actor (OOM, lost node) maps to NOT_FOUND: that is the client SDK's
    "task shut down" signal, the one status its retry loops stop on.
    """
    try:
        return await asyncio.wait_for(
            awaitable, timeout=extra_wait + _SCHEDULING_GRACE_SECONDS
        )
    except asyncio.TimeoutError:
        raise GRPCError(
            Status.UNAVAILABLE,
            "sandbox actor is not reachable (cluster may still be scaling)",
        )
    except Exception as exc:
        try:
            from ray.exceptions import RayActorError
        except ImportError:
            raise
        if isinstance(exc, RayActorError):
            raise GRPCError(Status.NOT_FOUND, "sandbox is gone (its actor has died)")
        raise


async def _await_host_exec(record: _ExecRecord) -> Dict[str, Any]:
    """Long-poll a SandboxHost exec job until it reaches a terminal state."""
    polls = 0
    while True:
        info = await _bounded(
            record.handle.get_exec.remote(
                record.host_exec_id, wait_seconds=_LONG_POLL_SECONDS
            ),
            extra_wait=_LONG_POLL_SECONDS,
        )
        if info.get("error_code"):
            raise GRPCError(Status.NOT_FOUND, info.get("message", "exec lost"))
        if info["status"] != "running":
            logger.info(
                "exec %s finished: status=%s exit=%s",
                record.host_exec_id,
                info["status"],
                info.get("exit_code"),
            )
            return info
        polls += 1
        if polls % 20 == 0:  # roughly every 5 minutes
            logger.info(
                "exec %s still running after ~%ds",
                record.host_exec_id,
                int(polls * _LONG_POLL_SECONDS),
            )


def _host_exit_code(info: Dict[str, Any]) -> int:
    if info["status"] == "completed":
        return info["exit_code"] if info["exit_code"] is not None else 0
    if info["status"] == "timeout":
        return _EXIT_TIMEOUT
    return _EXIT_ERROR


def _host_stream(info: Dict[str, Any], want_stdout: bool) -> bytes:
    if want_stdout:
        return (info.get("stdout") or "").encode("utf-8", errors="replace")
    stderr = (info.get("stderr") or "").encode("utf-8", errors="replace")
    # Surface spawn/timeout failures on stderr, where clients look for them.
    if info["status"] in ("error", "timeout") and info.get("error"):
        stderr += _sanitize_stderr(f"\n[ray-sandbox] {info['error']}".lstrip("\n"))
    return stderr


@_fill_unimplemented
@DeveloperAPI
class RaySandboxControlServicer(ModalClientBase):
    """Control-plane RPCs: apps, images, secrets, sandbox lifecycle."""

    def __init__(self, state: _FacadeState) -> None:
        self._state = state

    async def ClientHello(self, stream: Any) -> None:
        await stream.recv_message()
        await stream.send_message(api_pb2.ClientHelloResponse())

    async def AppGetOrCreate(self, stream: Any) -> None:
        request = await stream.recv_message()
        await stream.send_message(
            api_pb2.AppGetOrCreateResponse(
                app_id=_encode_id("ap-", request.app_name or "default")
            )
        )

    async def EnvironmentGetOrCreate(self, stream: Any) -> None:
        request = await stream.recv_message()
        name = request.deployment_name or "main"
        await stream.send_message(
            api_pb2.EnvironmentGetOrCreateResponse(
                environment_id=_encode_id("en-", name),
                metadata=api_pb2.EnvironmentMetadata(
                    name=name,
                    # 2025.06+ mounts the client's deps at runtime instead
                    # of baking them in, so a registry image reduces to a
                    # bare FROM — the only image shape the facade runs.
                    settings=api_pb2.EnvironmentSettings(
                        image_builder_version="2025.06"
                    ),
                ),
            )
        )

    async def SecretGetOrCreate(self, stream: Any) -> None:
        request = await stream.recv_message()
        anonymous_types = (
            api_pb2.OBJECT_CREATION_TYPE_ANONYMOUS_OWNED_BY_APP,
            api_pb2.OBJECT_CREATION_TYPE_EPHEMERAL,
        )
        if request.object_creation_type not in anonymous_types:
            raise GRPCError(
                Status.UNIMPLEMENTED,
                "only anonymous/ephemeral secrets (an inline dict) are "
                "supported by the Ray Sandbox gRPC facade",
            )
        secret_id = _encode_id("st-", dict(request.env_dict))
        await stream.send_message(
            api_pb2.SecretGetOrCreateResponse(secret_id=secret_id)
        )

    async def ImageGetOrCreate(self, stream: Any) -> None:
        request = await stream.recv_message()
        ref = _image_ref_from_proto(request.image)
        await stream.send_message(
            api_pb2.ImageGetOrCreateResponse(
                image_id=_encode_id("im-", ref),
                metadata=api_pb2.ImageMetadata(
                    image_builder_version=request.builder_version
                ),
            )
        )

    async def ImageJoinStreaming(self, stream: Any) -> None:
        # The image is pulled at sandbox boot, not at image build time, so
        # the "build" completes instantly.
        await stream.recv_message()
        await stream.send_message(
            api_pb2.ImageJoinStreamingResponse(
                result=api_pb2.GenericResult(
                    status=api_pb2.GenericResult.GENERIC_STATUS_SUCCESS
                )
            )
        )

    async def SandboxCreate(self, stream: Any) -> None:
        request = await stream.recv_message()
        info = self._create_sandbox(request)
        await stream.send_message(
            api_pb2.SandboxCreateResponse(
                sandbox_id=info["sandbox_id"],
                metadata=api_pb2.SandboxHandleMetadata(app_id=request.app_id),
            )
        )

    async def SandboxCreateV2(self, stream: Any) -> None:
        await self.SandboxCreate(stream)

    def _create_sandbox(self, request: Any) -> Dict[str, Any]:
        state = self._state
        settings = state.settings
        definition = request.definition

        image = _image_ref_from_proto_id(definition.image_id)
        env = _env_from_secret_ids(definition.secret_ids)
        labels = {tag.tag_name: tag.tag_value for tag in request.tags}

        network_type = definition.network_access.network_access_type
        if network_type == api_pb2.NetworkAccess.BLOCKED:
            network = "none"
        else:
            if network_type == api_pb2.NetworkAccess.ALLOWLIST:
                logger.warning(
                    "Network allowlists are not enforced by the Ray Sandbox "
                    "gRPC facade; granting open egress instead."
                )
            network = "public"

        resources = definition.resources
        cpu_request = resources.milli_cpu / 1000.0 if resources.milli_cpu else None
        cpu_limit = (
            resources.milli_cpu_max / 1000.0 if resources.milli_cpu_max else None
        )
        memory_request_mb = resources.memory_mb or None
        memory_limit_mb = resources.memory_mb_max or None

        ttl = (
            min(definition.timeout_secs, settings.max_ttl_seconds)
            if definition.timeout_secs
            else settings.max_ttl_seconds
        )

        # Sandbox names are unique per app while alive; deriving the id
        # from the name makes create retries converge on one actor
        # (get_if_exists on the named actor makes the race atomic).
        sandbox_id = _new_sandbox_id(definition.name or None)

        if list(definition.entrypoint_args) not in (
            [],
            ["sh", "-c", "sleep infinity"],
            ["sleep", "infinity"],
        ):
            logger.debug(
                "Ignoring sandbox entrypoint %s: Ray Sandbox keeps sandboxes "
                "alive with its own keepalive.",
                list(definition.entrypoint_args),
            )

        actor_options: Dict[str, Any] = {
            "num_cpus": (
                cpu_request
                if cpu_request is not None
                else (
                    cpu_limit
                    if cpu_limit is not None
                    else settings.default_actor_num_cpus
                )
            ),
        }
        request_mb = (
            memory_request_mb if memory_request_mb is not None else memory_limit_mb
        )
        if request_mb is not None:
            actor_options["memory"] = request_mb * 1024 * 1024

        spec = {
            "image": image,
            "env": env,
            "workdir": definition.workdir or None,
            "ttl_seconds": ttl,
            "network": network,
            "dns": None,
            "shell": "/bin/bash",
            "rootless": True,
            "readonly": False,
            "capabilities": list(settings.default_capabilities),
            "cpu_limit": cpu_limit,
            "memory_limit_mb": memory_limit_mb,
            "image_pull_timeout_seconds": _pull_timeout_seconds(),
            "start_timeout_seconds": 120.0,
            "labels": labels,
        }
        host_settings = {
            "max_output_bytes": settings.max_output_bytes,
            "max_exec_history": settings.max_exec_history,
            "auto_install_runsc": settings.auto_install_runsc,
        }
        handle = state.resolver.create(
            sandbox_id,
            actor_options,
            {"sandbox_id": sandbox_id, "spec": spec, "settings": host_settings},
        )
        handle.boot.remote()
        logger.info(
            "Created sandbox %s (image=%s, network=%s)", sandbox_id, image, network
        )
        return {"sandbox_id": sandbox_id}

    async def SandboxGetTaskId(self, stream: Any) -> None:
        request = await stream.recv_message()
        handle = self._state.require_handle(request.sandbox_id)
        try:
            info = await _bounded(
                handle.describe.remote(wait_seconds=1.0), extra_wait=1.0
            )
        except GRPCError as exc:
            if exc.status != Status.UNAVAILABLE:
                raise
            # An unscheduled actor is indistinguishable from a booting one to
            # the client; an empty task id keeps the SDK in its poll loop
            # instead of surfacing a transient scaling delay as an error.
            await stream.send_message(api_pb2.SandboxGetTaskIdResponse(task_id=""))
            return
        status = info["status"]
        if status == "running":
            # The sandbox id doubles as the client task id.
            response = api_pb2.SandboxGetTaskIdResponse(task_id=request.sandbox_id)
        elif status in ("error", "terminated"):
            raise GRPCError(
                Status.FAILED_PRECONDITION,
                f"sandbox {request.sandbox_id} is {status}: "
                f"{info.get('error') or 'no longer running'}",
            )
        else:
            # Still pulling/starting; an empty task id makes the SDK poll.
            response = api_pb2.SandboxGetTaskIdResponse(task_id="")
        await stream.send_message(response)

    async def SandboxGetTaskIdV2(self, stream: Any) -> None:
        await self.SandboxGetTaskId(stream)

    async def SandboxWait(self, stream: Any) -> None:
        request = await stream.recv_message()
        handle = self._state.resolver.get(request.sandbox_id)
        deadline = asyncio.get_running_loop().time() + min(request.timeout or 0, 55)
        result = None
        while True:
            if handle is None:
                result = api_pb2.GenericResult(
                    status=api_pb2.GenericResult.GENERIC_STATUS_TERMINATED
                )
                break
            try:
                info = await _bounded(
                    handle.describe.remote(wait_seconds=1.0), extra_wait=1.0
                )
            except GRPCError as exc:
                if exc.status == Status.NOT_FOUND:
                    # Actor died: from the client's view the sandbox ended.
                    result = api_pb2.GenericResult(
                        status=api_pb2.GenericResult.GENERIC_STATUS_TERMINATED
                    )
                    break
                # Unreachable actor: treat as still-pending until the deadline.
                if asyncio.get_running_loop().time() >= deadline:
                    break
                continue
            if info["status"] == "terminated":
                result = api_pb2.GenericResult(
                    status=api_pb2.GenericResult.GENERIC_STATUS_TERMINATED
                )
                break
            if info["status"] == "error":
                result = api_pb2.GenericResult(
                    status=api_pb2.GenericResult.GENERIC_STATUS_FAILURE,
                    exception=info.get("error") or "sandbox failed",
                )
                break
            if asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(1.0)
        response = api_pb2.SandboxWaitResponse()
        if result is not None:
            response.result.CopyFrom(result)
        await stream.send_message(response)

    async def SandboxTerminate(self, stream: Any) -> None:
        request = await stream.recv_message()
        handle = self._state.resolver.get(request.sandbox_id)
        if handle is not None:
            try:
                await _bounded(handle.terminate.remote(), extra_wait=30.0)
            except Exception as exc:
                logger.debug("terminate(%s): %s", request.sandbox_id, exc)
            # terminate() only deletes the sandbox; the actor must be killed
            # by the caller (mirrors the REST DELETE endpoint). Skipping this
            # leaks the actor's CPU reservation and starves later sandboxes
            # out of the cluster.
            self._state.resolver.kill(handle)
            logger.info("Terminated sandbox %s", request.sandbox_id)
        await stream.send_message(api_pb2.SandboxTerminateResponse())

    async def SandboxTerminateV2(self, stream: Any) -> None:
        await self.SandboxTerminate(stream)

    async def TaskGetCommandRouterAccess(self, stream: Any) -> None:
        await stream.recv_message()
        await stream.send_message(
            api_pb2.TaskGetCommandRouterAccessResponse(
                url=self._state.advertise_url,
                # Deliberately not a parseable JWT: the SDK treats that as
                # "no client-side expiry" and only refreshes on
                # UNAUTHENTICATED, which this facade never returns.
                jwt="ray-sandbox-facade",
            )
        )


def _image_ref_from_proto_id(image_id: str) -> str:
    return _decode_id("im-", image_id)


def _pull_timeout_seconds() -> float:
    return float(os.environ.get("RAY_SANDBOX_PULL_TIMEOUT_SECONDS", "1800"))


@_fill_unimplemented
@DeveloperAPI
class RaySandboxRouterServicer(TaskCommandRouterBase):
    """Exec-plane RPCs: start, stdio, stdin, poll/wait.

    The client SDK talks to this service directly at the URL handed out by
    ``TaskGetCommandRouterAccess``; here that is the same server.
    """

    def __init__(self, state: _FacadeState) -> None:
        self._state = state

    async def TaskExecStart(self, stream: Any) -> None:
        request = await stream.recv_message()
        state = self._state
        handle = state.require_handle(request.task_id)

        env = dict(request.env)
        env.update(_env_from_secret_ids(request.secret_ids))

        command = list(request.command_args)
        if command and command[0] == _FS_TOOLS_PATH:
            logger.info(
                "exec %s @ %s: fs-tools %s",
                request.exec_id,
                request.task_id,
                command[1:2],
            )
            record = await self._start_fs_op(handle, command)
        else:
            logger.info(
                "exec %s @ %s: argv=%s timeout=%s",
                request.exec_id,
                request.task_id,
                [c[:80] for c in command[:3]],
                request.timeout_secs or None,
            )
            started = await _bounded(
                handle.start_exec.remote(
                    command,
                    cwd=request.workdir or None,
                    env=env or None,
                    timeout_seconds=request.timeout_secs or None,
                )
            )
            if started.get("error_code"):
                raise GRPCError(
                    Status.FAILED_PRECONDITION,
                    started.get("message", "sandbox is not running"),
                )
            record = _ExecRecord(
                kind="host", handle=handle, host_exec_id=started["exec_id"]
            )
        state.execs[request.exec_id] = record
        await stream.send_message(sr_pb2.TaskExecStartResponse())

    async def _start_fs_op(self, handle: Any, command: List[str]) -> _ExecRecord:
        """Emulate one filesystem-tools invocation."""
        try:
            op = json.loads(command[1]) if len(command) > 1 else {}
        except ValueError:
            op = {}
        if len(op) != 1:
            raise GRPCError(
                Status.INVALID_ARGUMENT, f"unrecognized fs-tools command: {command[1:]}"
            )
        ((name, payload),) = op.items()
        path = payload.get("path", "")
        record = _ExecRecord(kind="fs", handle=handle, fs_path=path)

        if name == "WriteFile":
            # Content arrives over stdin; the write happens at stdin EOF.
            record.fs_write = True
        elif name == "ReadFile":
            # Generous bound: downloads can be multi-hundred-MB archives.
            result = await _bounded(handle.read_file.remote(path), extra_wait=600.0)
            if result.get("error_code") == "file_not_found":
                record.finish(1, stderr=_fs_error("NotFound", "path does not exist"))
            elif result.get("error_code"):
                record.finish(
                    1, stderr=_fs_error("Other", result.get("message", "read failed"))
                )
            else:
                record.finish(0, stdout=result["content"])
        elif name == "ListFiles":
            # Emulated with a shell probe. The entry list is intentionally
            # empty: the SDK's typed errors (NotFound / NotDirectory) are what
            # callers use for existence and is-directory checks.
            quoted = shlex.quote(path)
            probe = (
                f"if [ ! -e {quoted} ]; then "
                f"printf %s {shlex.quote(_fs_error('NotFound', 'path does not exist').decode())} >&2; exit 1; "
                f"elif [ ! -d {quoted} ]; then "
                f"printf %s {shlex.quote(_fs_error('NotDirectory', 'path is not a directory').decode())} >&2; exit 1; "
                f"else printf '[]'; fi"
            )
            started = await _bounded(handle.start_exec.remote(["/bin/sh", "-c", probe]))
            if started.get("error_code"):
                raise GRPCError(
                    Status.FAILED_PRECONDITION,
                    started.get("message", "sandbox is not running"),
                )
            record = _ExecRecord(
                kind="host", handle=handle, host_exec_id=started["exec_id"]
            )
        else:
            raise GRPCError(
                Status.UNIMPLEMENTED,
                f"fs-tools operation {name!r} is not supported by the "
                "Ray Sandbox gRPC facade",
            )
        return record

    async def _finish_write(self, record: _ExecRecord) -> None:
        """Flush buffered stdin to the sandbox file and finish the record.

        Chunked so each actor call stays under the scheduling bound no matter
        how large the upload is, and wrapped so the record ALWAYS finishes:
        the client waits on this exec's exit code concurrently with stdin, so
        a record left pending would make it retry that wait forever.
        """
        content = b"".join(record.stdin_chunks)
        record.stdin_chunks.clear()
        logger.info("fs write %s: %d bytes", record.fs_path, len(content))
        try:
            offset = 0
            while True:
                chunk = content[offset : offset + _FS_WRITE_CHUNK_BYTES]
                result = await _bounded(
                    record.handle.write_file.remote(
                        record.fs_path, chunk, append=offset > 0
                    ),
                    extra_wait=_FS_WRITE_BOUND_SECONDS,
                )
                if result.get("error_code"):
                    record.finish(
                        1,
                        stderr=_fs_error(
                            "Other", result.get("message", "write failed")
                        ),
                    )
                    return
                offset += len(chunk)
                if offset >= len(content):
                    break
            record.finish(0)
        except GRPCError as exc:
            record.finish(1, stderr=_fs_error("Other", f"write failed: {exc.message}"))
        except Exception as exc:
            record.finish(1, stderr=_fs_error("Other", f"write failed: {exc}"))

    async def TaskExecStdioRead(self, stream: Any) -> None:
        request = await stream.recv_message()
        record = self._state.require_exec(request.exec_id)
        want_stdout = (
            request.file_descriptor == sr_pb2.TASK_EXEC_STDIO_FILE_DESCRIPTOR_STDOUT
        )
        if record.kind == "host":
            info = await _await_host_exec(record)
            data = _host_stream(info, want_stdout)
        else:
            await record.finished.wait()
            data = record.stdout if want_stdout else record.stderr
        data = data[request.offset :]
        chunk_size = 256 * 1024
        for start in range(0, len(data), chunk_size):
            await stream.send_message(
                sr_pb2.TaskExecStdioReadResponse(data=data[start : start + chunk_size])
            )

    async def TaskExecStdinWrite(self, stream: Any) -> None:
        request = await stream.recv_message()
        record = self._state.require_exec(request.exec_id)
        self._buffer_stdin(record, request.data, request.offset)
        if request.eof and not record.stdin_closed:
            record.stdin_closed = True
            if record.fs_write:
                await self._finish_write(record)
        await stream.send_message(sr_pb2.TaskExecStdinWriteResponse())

    async def TaskExecStdinWriteStream(self, stream: Any) -> None:
        request = await stream.recv_message()
        if request.WhichOneof("payload") != "start":
            raise GRPCError(
                Status.INVALID_ARGUMENT, "first stdin stream message must be start"
            )
        record = self._state.require_exec(request.start.exec_id)
        if request.start.offset != record.stdin_bytes:
            raise GRPCError(Status.FAILED_PRECONDITION, "stdin offset mismatch")
        while (request := await stream.recv_message()) is not None:
            which = request.WhichOneof("payload")
            if which == "end":
                if not record.stdin_closed:
                    record.stdin_closed = True
                    if record.fs_write:
                        await self._finish_write(record)
                break
            if which != "data":
                raise GRPCError(
                    Status.INVALID_ARGUMENT, "stdin stream message must contain data"
                )
            self._buffer_stdin(record, request.data, record.stdin_bytes)
        await stream.send_message(sr_pb2.TaskExecStdinWriteStreamResponse())

    def _buffer_stdin(self, record: _ExecRecord, data: bytes, offset: int) -> None:
        if record.kind == "host":
            # SandboxHost execs have no stdin; nothing sensible to do with it.
            raise GRPCError(
                Status.UNIMPLEMENTED,
                "exec stdin is not supported by the Ray Sandbox gRPC facade",
            )
        if data:
            if offset != record.stdin_bytes:
                raise GRPCError(Status.FAILED_PRECONDITION, "stdin offset mismatch")
            record.stdin_chunks.append(data)
            record.stdin_bytes += len(data)

    async def TaskExecStdinStatus(self, stream: Any) -> None:
        request = await stream.recv_message()
        record = self._state.require_exec(request.exec_id)
        await stream.send_message(
            sr_pb2.TaskExecStdinStatusResponse(
                num_bytes_written=record.stdin_bytes, closed=record.stdin_closed
            )
        )

    async def TaskExecPoll(self, stream: Any) -> None:
        request = await stream.recv_message()
        record = self._state.require_exec(request.exec_id)
        response = sr_pb2.TaskExecPollResponse()
        if record.kind == "host":
            info = await _bounded(record.handle.get_exec.remote(record.host_exec_id))
            if info.get("error_code"):
                raise GRPCError(Status.NOT_FOUND, info.get("message", "exec lost"))
            if info["status"] != "running":
                response.code = _host_exit_code(info)
        elif record.finished.is_set():
            response.code = record.exit_code
        await stream.send_message(response)

    async def TaskExecWait(self, stream: Any) -> None:
        request = await stream.recv_message()
        record = self._state.require_exec(request.exec_id)
        if record.kind == "host":
            info = await _await_host_exec(record)
            code = _host_exit_code(info)
        else:
            await record.finished.wait()
            code = record.exit_code
        await stream.send_message(sr_pb2.TaskExecWaitResponse(code=code))

    async def TaskSetNetworkAccess(self, stream: Any) -> None:
        await stream.recv_message()
        logger.warning(
            "TaskSetNetworkAccess ignored: the Ray Sandbox gRPC facade does "
            "not change network policy after sandbox creation."
        )
        await stream.send_message(sr_pb2.TaskSetNetworkAccessResponse())


@DeveloperAPI
def build_servicers(
    settings: Optional[SandboxAPISettings] = None,
    *,
    handle_resolver: Optional[Any] = None,
    advertise_url: str,
) -> List[Any]:
    """Build the two grpclib servicers sharing one facade state.

    Args:
        settings: Server settings; defaults are production-safe.
        handle_resolver: Test seam, same surface as in ``create_app``.
        advertise_url: URL handed to clients for the command router; must
            route back to this same server.

    Returns:
        The control-plane and command-router servicers, ready for
        ``grpclib.server.Server``.
    """
    settings = settings or SandboxAPISettings()
    resolver = handle_resolver or RayActorHandleResolver(settings)
    state = _FacadeState(resolver, settings, advertise_url)
    return [RaySandboxControlServicer(state), RaySandboxRouterServicer(state)]


async def serve(host: str, port: int, servicers: List[Any]) -> None:
    server = Server(servicers)
    await server.start(host, port)
    logger.info("Ray Sandbox gRPC facade listening on %s:%d", host, port)
    await server.wait_closed()


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument(
        "--advertise-url",
        default=None,
        help="Command-router URL handed to clients (default http://HOST:PORT)",
    )
    args = parser.parse_args(argv)
    advertise = args.advertise_url or f"http://{args.host}:{args.port}"

    logging.basicConfig(level=logging.INFO)
    import ray

    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), ignore_reinit_error=True)
    servicers = build_servicers(advertise_url=advertise)
    asyncio.run(serve(args.host, args.port, servicers))


if __name__ == "__main__":
    main()
