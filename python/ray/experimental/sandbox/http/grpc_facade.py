"""gRPC facade over Ray Sandbox for third-party sandbox clients.

Implements the subset of a sandbox client SDK's control-plane
(``ModalClient``) and command-router (``TaskCommandRouter``) services that
its Sandbox API uses, backed by the same detached ``SandboxHost`` actors as
the REST API in ``app.py``. An unmodified client pointed at this server can
create sandboxes, run commands, and use its filesystem API against a Ray
cluster. The wire contract is vendored under ``_proto/``.

The facade keeps no registry: object ids carry their payload (``im-`` wraps
an image ref, ``st-`` a secret's env dict) and the sandbox id doubles as the
client task id. The exec table is the one piece of in-process state, so run
a single facade process per cluster.

Requires ``grpclib``. Run with
``python -m ray.experimental.sandbox.http.grpc_facade``.
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
    _is_actor_gone,
    _is_actor_unavailable,
    _is_unschedulable,
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

# Helper binary the client SDK execs for its filesystem API. Sandbox images
# do not contain it; execs of this argv are emulated, never run.
_FS_TOOLS_PATH = "/__modal/.bin/modal-sandbox-fs-tools"

# Exit codes for exec jobs that did not produce one of their own.
_EXIT_TIMEOUT = 124
_EXIT_ERROR = 126

_LONG_POLL_SECONDS = 15.0
_WAIT_MAX_SECONDS = 55.0
_SCHEDULING_GRACE_SECONDS = 10.0
_PULL_TIMEOUT_SECONDS = 1800.0
_START_TIMEOUT_SECONDS = 120.0
_READ_BOUND_SECONDS = 600.0
_WRITE_CHUNK_BYTES = 4 * 1024 * 1024
_WRITE_BOUND_SECONDS = 120.0
_STDIO_CHUNK_BYTES = 256 * 1024
_MAX_EXEC_RECORDS = 1000


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
        keyword = stripped.split(None, 1)[0].upper()
        if keyword == "FROM":
            if ref is not None:
                raise GRPCError(
                    Status.INVALID_ARGUMENT,
                    "multi-stage image builds are not supported by the "
                    "Ray Sandbox gRPC facade",
                )
            ref = stripped.split(None, 1)[1].strip()
        elif keyword not in ("ENTRYPOINT", "CMD", "ENV", "WORKDIR", "LABEL"):
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


def _fs_error(error_kind: str, message: str) -> bytes:
    return json.dumps({"error_kind": error_kind, "message": message}).encode()


@dataclass
class _ExecRecord:
    """One client exec id and how the facade services it."""

    kind: str  # "host" (a SandboxHost exec job) or "fs" (an emulated file op)
    handle: Any
    host_exec_id: Optional[str] = None
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

    def add_exec(self, exec_id: str, record: _ExecRecord) -> None:
        """Register an exec, evicting the oldest finished ones past the cap."""
        self.execs[exec_id] = record
        excess = len(self.execs) - _MAX_EXEC_RECORDS
        for key, existing in list(self.execs.items()):
            if excess <= 0:
                break
            if existing.finished.is_set():
                del self.execs[key]
                excess -= 1


async def _bounded(awaitable: Any, extra_wait: float = 0.0) -> Any:
    """Await a SandboxHost call, mapping actor failures to gRPC statuses.

    A call to an actor that is not scheduled yet (the cluster may still be
    scaling) blocks indefinitely, so every call is capped at its own
    long-poll budget plus a grace period. Unreachable actors map to
    UNAVAILABLE, which the client SDK retries; a dead actor maps to
    NOT_FOUND, the SDK's "task shut down" signal.
    """
    try:
        return await asyncio.wait_for(
            awaitable, timeout=extra_wait + _SCHEDULING_GRACE_SECONDS
        )
    except asyncio.TimeoutError:
        raise GRPCError(
            Status.UNAVAILABLE,
            "sandbox actor is not reachable; the cluster may still be scaling",
        )
    except Exception as exc:
        if _is_actor_unavailable(exc):
            raise GRPCError(
                Status.UNAVAILABLE, "sandbox actor is temporarily unavailable"
            )
        if _is_actor_gone(exc):
            raise GRPCError(Status.NOT_FOUND, "sandbox is gone; its actor has died")
        if _is_unschedulable(exc):
            raise GRPCError(
                Status.FAILED_PRECONDITION,
                f"sandbox cannot be scheduled: {str(exc)[:300]}",
            )
        raise


async def _is_alive(handle: Any) -> bool:
    """False only when the actor has died; an unscheduled actor counts as alive."""
    try:
        await _bounded(handle.describe.remote())
    except GRPCError as exc:
        return exc.status != Status.NOT_FOUND
    return True


async def _await_host_exec(record: _ExecRecord) -> Dict[str, Any]:
    """Long-poll a SandboxHost exec job until it reaches a terminal state."""
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
            record.finished.set()
            return info


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
    # Spawn and timeout failures surface on stderr, where clients look.
    if info["status"] in ("error", "timeout") and info.get("error"):
        stderr += f"\n[ray-sandbox] {info['error']}".lstrip("\n").encode(
            "utf-8", errors="replace"
        )
    return stderr


def _terminated() -> Any:
    return api_pb2.GenericResult(status=api_pb2.GenericResult.GENERIC_STATUS_TERMINATED)


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
                    # From 2025.06 the SDK mounts its own dependencies at
                    # runtime, so a registry image reduces to a bare FROM.
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
                "only anonymous or ephemeral secrets (an inline dict) are "
                "supported by the Ray Sandbox gRPC facade",
            )
        await stream.send_message(
            api_pb2.SecretGetOrCreateResponse(
                secret_id=_encode_id("st-", dict(request.env_dict))
            )
        )

    async def ImageGetOrCreate(self, stream: Any) -> None:
        request = await stream.recv_message()
        ref = _image_ref_from_dockerfile(request.image)
        await stream.send_message(
            api_pb2.ImageGetOrCreateResponse(
                image_id=_encode_id("im-", ref),
                metadata=api_pb2.ImageMetadata(
                    image_builder_version=request.builder_version
                ),
            )
        )

    async def ImageJoinStreaming(self, stream: Any) -> None:
        # Images are pulled at sandbox boot, so the "build" is already done.
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
        sandbox_id = await self._create_sandbox(request)
        await stream.send_message(
            api_pb2.SandboxCreateResponse(
                sandbox_id=sandbox_id,
                metadata=api_pb2.SandboxHandleMetadata(app_id=request.app_id),
            )
        )

    async def SandboxCreateV2(self, stream: Any) -> None:
        await self.SandboxCreate(stream)

    async def _create_sandbox(self, request: Any) -> str:
        state = self._state
        settings = state.settings
        definition = request.definition

        if definition.name:
            # Names are unique per app while the sandbox lives, so a named
            # create is idempotent: retries converge on the existing actor.
            sandbox_id = _new_sandbox_id(f"{request.app_id}/{definition.name}")
            existing = state.resolver.get(sandbox_id)
            if existing is not None:
                if await _is_alive(existing):
                    return sandbox_id
                # The previous actor died (node loss, OOM); recreate it.
                state.resolver.kill(existing)
        else:
            sandbox_id = _new_sandbox_id()

        network_type = definition.network_access.network_access_type
        if network_type == api_pb2.NetworkAccess.ALLOWLIST:
            logger.warning(
                "Network allowlists are not enforced by the Ray Sandbox gRPC "
                "facade; granting open egress instead."
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
        spec = {
            "image": _decode_id("im-", definition.image_id),
            "env": _env_from_secret_ids(definition.secret_ids),
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
            "image_pull_timeout_seconds": _PULL_TIMEOUT_SECONDS,
            "start_timeout_seconds": _START_TIMEOUT_SECONDS,
            "labels": {tag.tag_name: tag.tag_value for tag in request.tags},
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
            "Created sandbox %s (image=%s, network=%s)",
            sandbox_id,
            spec["image"],
            network,
        )
        return sandbox_id

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
            # An unscheduled actor looks like a booting one to the client:
            # an empty task id keeps the SDK polling.
            await stream.send_message(api_pb2.SandboxGetTaskIdResponse(task_id=""))
            return
        status = info["status"]
        if status in ("error", "terminated"):
            raise GRPCError(
                Status.FAILED_PRECONDITION,
                f"sandbox {request.sandbox_id} is {status}: "
                f"{info.get('error') or 'no longer running'}",
            )
        # The sandbox id doubles as the task id once running; an empty task
        # id while pulling or starting makes the SDK poll.
        task_id = request.sandbox_id if status == "running" else ""
        await stream.send_message(api_pb2.SandboxGetTaskIdResponse(task_id=task_id))

    async def SandboxGetTaskIdV2(self, stream: Any) -> None:
        await self.SandboxGetTaskId(stream)

    async def SandboxWait(self, stream: Any) -> None:
        request = await stream.recv_message()
        handle = self._state.resolver.get(request.sandbox_id)
        loop = asyncio.get_running_loop()
        # timeout=0 is the SDK's non-blocking poll(); wait() loops with 10s.
        deadline = loop.time() + min(request.timeout, _WAIT_MAX_SECONDS)
        result = None
        while True:
            if handle is None:
                result = _terminated()
                break
            try:
                info = await _bounded(
                    handle.describe.remote(wait_seconds=1.0), extra_wait=1.0
                )
            except GRPCError as exc:
                if exc.status == Status.NOT_FOUND:
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
        await stream.send_message(response)

    async def SandboxTerminate(self, stream: Any) -> None:
        request = await stream.recv_message()
        handle = self._state.resolver.get(request.sandbox_id)
        if handle is not None:
            try:
                await _bounded(handle.terminate.remote(), extra_wait=30.0)
            except Exception as exc:
                logger.debug("terminate(%s): %s", request.sandbox_id, exc)
            # terminate() only deletes the sandbox; killing the actor
            # releases its cluster reservation (as in the REST DELETE).
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
                # Not a parseable JWT on purpose: the SDK then applies no
                # client-side expiry and only refreshes on UNAUTHENTICATED.
                jwt="ray-sandbox-facade",
            )
        )


@_fill_unimplemented
@DeveloperAPI
class RaySandboxRouterServicer(TaskCommandRouterBase):
    """Exec-plane RPCs: start, stdio, stdin, poll, and wait.

    The client SDK reaches this service at the URL handed out by
    ``TaskGetCommandRouterAccess``, which here is the same server.
    """

    def __init__(self, state: _FacadeState) -> None:
        self._state = state

    async def TaskExecStart(self, stream: Any) -> None:
        request = await stream.recv_message()
        state = self._state
        handle = state.require_handle(request.task_id)

        command = list(request.command_args)
        if command and command[0] == _FS_TOOLS_PATH:
            record = await self._start_fs_op(handle, command)
        else:
            env = dict(request.env)
            env.update(_env_from_secret_ids(request.secret_ids))
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
        logger.debug("exec %s on %s: %s", request.exec_id, request.task_id, command[:2])
        state.add_exec(request.exec_id, record)
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
            result = await _bounded(
                handle.read_file.remote(path), extra_wait=_READ_BOUND_SECONDS
            )
            if result.get("error_code") == "file_not_found":
                record.finish(1, stderr=_fs_error("NotFound", "path does not exist"))
            elif result.get("error_code"):
                record.finish(
                    1, stderr=_fs_error("Other", result.get("message", "read failed"))
                )
            else:
                record.finish(0, stdout=result["content"])
        elif name == "ListFiles":
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
        """Flush buffered stdin to the sandbox file in bounded slices.

        The record always finishes, even on failure: the client waits on the
        exec's exit code concurrently with stdin and would retry forever on a
        record left pending.
        """
        content = b"".join(record.stdin_chunks)
        record.stdin_chunks.clear()
        try:
            for offset in range(0, max(len(content), 1), _WRITE_CHUNK_BYTES):
                result = await _bounded(
                    record.handle.write_file.remote(
                        record.fs_path,
                        content[offset : offset + _WRITE_CHUNK_BYTES],
                        append=offset > 0,
                    ),
                    extra_wait=_WRITE_BOUND_SECONDS,
                )
                if result.get("error_code"):
                    record.finish(
                        1,
                        stderr=_fs_error(
                            "Other", result.get("message", "write failed")
                        ),
                    )
                    return
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
        for start in range(0, len(data), _STDIO_CHUNK_BYTES):
            await stream.send_message(
                sr_pb2.TaskExecStdioReadResponse(
                    data=data[start : start + _STDIO_CHUNK_BYTES]
                )
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
            # SandboxHost execs have no stdin.
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
                record.finished.set()
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
        advertise_url: Command-router URL handed to clients; must route
            back to this same server.

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
