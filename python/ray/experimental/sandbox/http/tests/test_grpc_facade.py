"""Wire-level tests for the gRPC facade.

The facade is driven over gRPC with the vendored client stubs against the
same fake resolver and runtime seams the REST app tests use. Skipped when
the optional ``grpclib`` package is not installed.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any, List, Tuple

import pytest

from ray.experimental.sandbox.http.tests.conftest import (
    FakeExecResult,
    FakeResolver,
    FakeSandboxRuntime,
)

# Guarded import so the module collects (and skips) without grpclib; a
# module-level importorskip would collect nothing and fail the bazel target.
try:
    from grpclib.client import Channel
    from grpclib.exceptions import GRPCError
    from grpclib.server import Server

    from ray.experimental.sandbox.http import grpc_facade
    from ray.experimental.sandbox.http._proto import (
        sandbox_control_pb2 as api_pb2,
        sandbox_exec_pb2 as sr_pb2,
    )
    from ray.experimental.sandbox.http._proto.sandbox_control_grpc import (
        ModalClientStub,
    )
    from ray.experimental.sandbox.http._proto.sandbox_exec_grpc import (
        TaskCommandRouterStub,
    )
    from ray.experimental.sandbox.http.grpc_facade import (
        _FS_TOOLS_PATH,
        build_servicers,
    )

    _HAVE_GRPCLIB = True
except ImportError:
    _HAVE_GRPCLIB = False

pytestmark = pytest.mark.skipif(
    not _HAVE_GRPCLIB,
    reason="grpclib is not installed (optional; absent in the default CI image)",
)

# Each test gets its own port so a leftover HTTP/2 connection from a prior test
# can never bleed into the next one.
_next_port = iter(range(50917, 50947))


class _Facade:
    """Run the facade servicers on a local gRPC server for the test."""

    def __init__(self, resolver: FakeResolver, port: int) -> None:
        self._resolver = resolver
        self._port = port
        self._server: Server = None

    async def __aenter__(self) -> "_Facade":
        servicers = build_servicers(
            handle_resolver=self._resolver,
            advertise_url=f"http://127.0.0.1:{self._port}",
        )
        self._server = Server(servicers)
        await self._server.start("127.0.0.1", self._port)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self._server.close()
        await self._server.wait_closed()


def _channel(port: int) -> Tuple[Channel, ModalClientStub, TaskCommandRouterStub]:
    channel = Channel("127.0.0.1", port)
    return channel, ModalClientStub(channel), TaskCommandRouterStub(channel)


async def _make_sandbox(
    control: ModalClientStub,
    name: str = "test-session",
    app_name: str = "facade-test",
) -> str:
    """Drive the create handshake the client SDK performs, over the wire."""
    app = await control.AppGetOrCreate(api_pb2.AppGetOrCreateRequest(app_name=app_name))
    image = await control.ImageGetOrCreate(
        api_pb2.ImageGetOrCreateRequest(
            image=api_pb2.Image(dockerfile_commands=["FROM ubuntu:24.04"])
        )
    )
    secret = await control.SecretGetOrCreate(
        api_pb2.SecretGetOrCreateRequest(
            object_creation_type=api_pb2.OBJECT_CREATION_TYPE_ANONYMOUS_OWNED_BY_APP,
            env_dict={"STARTUP": "env"},
        )
    )
    created = await control.SandboxCreate(
        api_pb2.SandboxCreateRequest(
            app_id=app.app_id,
            definition=api_pb2.Sandbox(
                entrypoint_args=["sh", "-c", "sleep infinity"],
                image_id=image.image_id,
                secret_ids=[secret.secret_id],
                timeout_secs=3600,
                name=name,
                resources=api_pb2.Resources(milli_cpu=2000, memory_mb=1024),
            ),
        )
    )
    return created.sandbox_id


async def _wait_running(control: ModalClientStub, sandbox_id: str) -> None:
    for _ in range(50):
        resp = await control.SandboxGetTaskId(
            api_pb2.SandboxGetTaskIdRequest(sandbox_id=sandbox_id)
        )
        if resp.task_id:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("sandbox never reached running")


async def _read_stdout(router: TaskCommandRouterStub, exec_id: str) -> bytes:
    chunks: List[bytes] = []
    async with router.TaskExecStdioRead.open() as stream:
        await stream.send_message(
            sr_pb2.TaskExecStdioReadRequest(
                exec_id=exec_id,
                offset=0,
                file_descriptor=sr_pb2.TASK_EXEC_STDIO_FILE_DESCRIPTOR_STDOUT,
            ),
            end=True,
        )
        async for msg in stream:
            chunks.append(msg.data)
    return b"".join(chunks)


def test_wire_paths_match_contract() -> None:
    """The vendored stubs route on the external service's exact method paths."""
    mapping = {}
    for servicer in build_servicers(
        handle_resolver=FakeResolver(), advertise_url="http://x"
    ):
        mapping.update(servicer.__mapping__())
    assert "/modal.client.ModalClient/SandboxCreate" in mapping
    assert "/modal.client.ModalClient/SandboxGetTaskId" in mapping
    assert "/modal.task_command_router.TaskCommandRouter/TaskExecStart" in mapping
    assert "/modal.task_command_router.TaskCommandRouter/TaskExecWait" in mapping


def test_full_sandbox_lifecycle(tmp_path) -> None:
    """Create → exec → filesystem round-trip → terminate over the wire."""
    port = next(_next_port)

    async def scenario() -> dict:
        resolver = FakeResolver()
        runtime = FakeSandboxRuntime()
        runtime.readable_files["/tmp/data.bin"] = b"binary \x00\xff payload"
        resolver.next_runtime = runtime
        async with _Facade(resolver, port):
            channel, control, router = _channel(port)
            try:
                sandbox_id = await _make_sandbox(control)
                await _wait_running(control, sandbox_id)

                assert runtime.create_calls[0]["image"] == "ubuntu:24.04"

                # A plain command exec.
                runtime.exec_results.append(
                    FakeExecResult(exit_code=3, stdout="out", stderr="err")
                )
                await router.TaskExecStart(
                    sr_pb2.TaskExecStartRequest(
                        task_id=sandbox_id,
                        exec_id="ex-cmd",
                        command_args=["bash", "-c", "cmd"],
                        workdir="/w",
                        env={"K": "V"},
                    )
                )
                out = await _read_stdout(router, "ex-cmd")
                code = await router.TaskExecWait(
                    sr_pb2.TaskExecWaitRequest(exec_id="ex-cmd")
                )
                exec_call = runtime.exec_calls[-1]

                # Filesystem write over exec stdin (multi-chunk stdin).
                payload = os.urandom(600 * 1024)
                await router.TaskExecStart(
                    sr_pb2.TaskExecStartRequest(
                        task_id=sandbox_id,
                        exec_id="ex-write",
                        command_args=[
                            _FS_TOOLS_PATH,
                            json.dumps({"WriteFile": {"path": "/tmp/upload.bin"}}),
                        ],
                    )
                )
                await router.TaskExecStdinWrite(
                    sr_pb2.TaskExecStdinWriteRequest(
                        exec_id="ex-write", offset=0, data=payload, eof=True
                    )
                )
                write_code = await router.TaskExecWait(
                    sr_pb2.TaskExecWaitRequest(exec_id="ex-write")
                )

                # Filesystem read.
                await router.TaskExecStart(
                    sr_pb2.TaskExecStartRequest(
                        task_id=sandbox_id,
                        exec_id="ex-read",
                        command_args=[
                            _FS_TOOLS_PATH,
                            json.dumps({"ReadFile": {"path": "/tmp/data.bin"}}),
                        ],
                    )
                )
                downloaded = await _read_stdout(router, "ex-read")

                await control.SandboxTerminate(
                    api_pb2.SandboxTerminateRequest(sandbox_id=sandbox_id)
                )
                return {
                    "sandbox_id": sandbox_id,
                    "stdout": out,
                    "code": code.code,
                    "exec_call": exec_call,
                    "uploaded": runtime.written_files.get("/tmp/upload.bin") == payload,
                    "write_code": write_code.code,
                    "downloaded": downloaded,
                    "deleted": bool(runtime.deleted),
                    "actor_killed": bool(resolver.killed),
                }
            finally:
                channel.close()

    result = asyncio.run(scenario())
    assert result["sandbox_id"].startswith("sb-")
    assert len(result["sandbox_id"]) == len("sb-") + 22  # V1 id shape
    assert result["stdout"] == b"out"
    assert result["code"] == 3
    assert result["exec_call"]["command"] == ["bash", "-c", "cmd"]
    assert result["exec_call"]["cwd"] == "/w"
    assert result["exec_call"]["env"] == {"K": "V"}
    assert result["uploaded"]
    assert result["write_code"] == 0
    assert result["downloaded"] == b"binary \x00\xff payload"
    assert result["deleted"]
    assert result["actor_killed"]


def test_named_create_is_idempotent() -> None:
    """Two creates under one sandbox name converge on one sandbox."""
    port = next(_next_port)

    async def scenario() -> Tuple[str, str]:
        resolver = FakeResolver()
        async with _Facade(resolver, port):
            channel, control, _ = _channel(port)
            try:
                first = await _make_sandbox(control, name="same-name")
                second = await _make_sandbox(control, name="same-name")
                return first, second
            finally:
                channel.close()

    first_id, second_id = asyncio.run(scenario())
    assert first_id == second_id


def test_named_sandboxes_are_scoped_to_app() -> None:
    """The same sandbox name under two apps yields two sandboxes."""
    port = next(_next_port)

    async def scenario() -> Tuple[str, str]:
        resolver = FakeResolver()
        async with _Facade(resolver, port):
            channel, control, _ = _channel(port)
            try:
                first = await _make_sandbox(control, name="shared", app_name="app-a")
                second = await _make_sandbox(control, name="shared", app_name="app-b")
                return first, second
            finally:
                channel.close()

    first_id, second_id = asyncio.run(scenario())
    assert first_id != second_id


class _DeadHandle:
    """A handle whose actor has died: every call raises ``ActorDiedError``."""

    def __getattr__(self, name: str) -> Any:
        class _Method:
            def remote(self, *args: Any, **kwargs: Any) -> "asyncio.Task":
                async def _raise() -> None:
                    from ray.exceptions import ActorDiedError

                    raise ActorDiedError()

                return asyncio.get_running_loop().create_task(_raise())

        return _Method()


def test_named_create_replaces_dead_actor() -> None:
    """A named create whose actor died kills it and boots a fresh sandbox."""
    port = next(_next_port)

    async def scenario() -> Tuple[str, str, List[str], bool]:
        resolver = FakeResolver()
        async with _Facade(resolver, port):
            channel, control, _ = _channel(port)
            try:
                first = await _make_sandbox(control, name="same-name")
                dead = _DeadHandle()
                resolver.handles[first] = dead
                second = await _make_sandbox(control, name="same-name")
                return first, second, resolver.killed, resolver.handles[second] is dead
            finally:
                channel.close()

    first_id, second_id, killed, still_dead = asyncio.run(scenario())
    assert first_id == second_id
    assert killed == [first_id]
    assert not still_dead


def test_wait_with_zero_timeout_polls() -> None:
    """``timeout=0`` is the SDK's poll(): return at once with no result."""
    port = next(_next_port)

    async def scenario() -> Tuple[int, float]:
        resolver = FakeResolver()
        resolver.next_runtime = FakeSandboxRuntime()
        async with _Facade(resolver, port):
            channel, control, _ = _channel(port)
            try:
                sandbox_id = await _make_sandbox(control)
                await _wait_running(control, sandbox_id)
                started = asyncio.get_running_loop().time()
                resp = await control.SandboxWait(
                    api_pb2.SandboxWaitRequest(sandbox_id=sandbox_id, timeout=0)
                )
                return resp.result.status, asyncio.get_running_loop().time() - started
            finally:
                channel.close()

    status, elapsed = asyncio.run(scenario())
    assert status == api_pb2.GenericResult.GENERIC_STATUS_UNSPECIFIED
    assert elapsed < 1.0


def test_exec_table_evicts_finished_records(monkeypatch) -> None:
    """Finished execs are evicted past the cap; running ones are kept."""
    monkeypatch.setattr(grpc_facade, "_MAX_EXEC_RECORDS", 3)

    async def scenario() -> List[str]:
        state = grpc_facade._FacadeState(FakeResolver(), None, "http://x")
        running = grpc_facade._ExecRecord(kind="fs", handle=None)
        state.add_exec("running", running)
        for i in range(4):
            record = grpc_facade._ExecRecord(kind="fs", handle=None)
            record.finish(0)
            state.add_exec(f"done-{i}", record)
        return list(state.execs)

    assert asyncio.run(scenario()) == ["running", "done-2", "done-3"]


def test_read_missing_file_is_typed() -> None:
    """A missing file surfaces as a NotFound fs error on the exec's stderr."""
    port = next(_next_port)

    async def scenario() -> Tuple[int, bytes]:
        resolver = FakeResolver()
        resolver.next_runtime = FakeSandboxRuntime()
        async with _Facade(resolver, port):
            channel, control, router = _channel(port)
            try:
                sandbox_id = await _make_sandbox(control)
                await _wait_running(control, sandbox_id)
                await router.TaskExecStart(
                    sr_pb2.TaskExecStartRequest(
                        task_id=sandbox_id,
                        exec_id="ex-miss",
                        command_args=[
                            _FS_TOOLS_PATH,
                            json.dumps({"ReadFile": {"path": "/missing"}}),
                        ],
                    )
                )
                stderr_chunks: List[bytes] = []
                async with router.TaskExecStdioRead.open() as stream:
                    await stream.send_message(
                        sr_pb2.TaskExecStdioReadRequest(
                            exec_id="ex-miss",
                            offset=0,
                            file_descriptor=(
                                sr_pb2.TASK_EXEC_STDIO_FILE_DESCRIPTOR_STDERR
                            ),
                        ),
                        end=True,
                    )
                    async for msg in stream:
                        stderr_chunks.append(msg.data)
                code = await router.TaskExecWait(
                    sr_pb2.TaskExecWaitRequest(exec_id="ex-miss")
                )
                return code.code, b"".join(stderr_chunks)
            finally:
                channel.close()

    code, stderr = asyncio.run(scenario())
    assert code == 1
    assert json.loads(stderr)["error_kind"] == "NotFound"


def test_build_step_images_are_rejected() -> None:
    """Images needing a server-side build fail with INVALID_ARGUMENT."""
    port = next(_next_port)

    async def scenario() -> str:
        resolver = FakeResolver()
        async with _Facade(resolver, port):
            channel, control, _ = _channel(port)
            try:
                await control.ImageGetOrCreate(
                    api_pb2.ImageGetOrCreateRequest(
                        image=api_pb2.Image(
                            dockerfile_commands=[
                                "FROM ubuntu:24.04",
                                "RUN apt-get update",
                            ]
                        )
                    )
                )
                return ""
            except GRPCError as exc:
                return exc.message or ""
            finally:
                channel.close()

    message = asyncio.run(scenario())
    assert "prebuilt registry images" in message


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
