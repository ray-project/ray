"""Unit tests for SandboxHost against a fake runtime (no Ray, no runsc)."""

import asyncio
import sys
import threading

import pytest

from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxExecError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.schemas import DOCKER_DEFAULT_CAPABILITIES
from ray.experimental.sandbox.http.tests.conftest import (
    FakeExecResult,
    FakeSandboxRuntime,
)


def _make_host(
    runtime: FakeSandboxRuntime,
    *,
    spec_overrides=None,
    settings_overrides=None,
) -> SandboxHost:
    spec = {
        "image": "python:3.12-slim",
        "env": {"TASK_VAR": "1"},
        "workdir": "/",
        "ttl_seconds": 3600,
        "network": "none",
        "rootless": True,
        "readonly": False,
        "capabilities": list(DOCKER_DEFAULT_CAPABILITIES),
        "cpu_limit": 2.0,
        "memory_limit_mb": 1024,
        "image_pull_timeout_seconds": 300.0,
        "start_timeout_seconds": 45.0,
        "labels": {"harbor-session-id": "abc"},
    }
    spec.update(spec_overrides or {})
    settings = {"max_output_bytes": 10 * 1024 * 1024, "max_exec_history": 256}
    settings.update(settings_overrides or {})
    return SandboxHost(
        sandbox_id="sb-test00000001",
        spec=spec,
        settings=settings,
        runtime_factory=lambda: runtime,
    )


# ----------------------------------------------------------------------
# Boot
# ----------------------------------------------------------------------


def test_boot_reaches_running_with_expected_create_kwargs() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        return await host.describe()

    info = asyncio.run(scenario())
    assert info["status"] == "running"
    assert info["error"] is None
    assert info["image"] == "python:3.12-slim"
    assert info["labels"] == {"harbor-session-id": "abc"}
    assert info["ttl_seconds"] == 3600
    assert info["expires_at"] is not None

    assert runtime.pull_calls == [
        {"image": "python:3.12-slim", "timeout_seconds": 300.0}
    ]
    (create_call,) = runtime.create_calls
    assert create_call["image"] == "python:3.12-slim"
    assert create_call["cpu"] == 2.0
    assert create_call["memory"] == "1024Mi"
    assert create_call["env"] == {"TASK_VAR": "1"}
    assert create_call["workdir"] == "/"
    # The API owns the TTL; the upstream runtime must not double-manage it.
    assert create_call["ttl_seconds"] is None
    # Writability follows the runtime's (readonly, workdir) contract; no
    # extra knobs are forwarded.
    assert "mount_workdir" not in create_call
    assert create_call["timeout_seconds"] == 45.0
    assert create_call["rootless"] is True
    assert create_call["network"] == "none"
    assert create_call["readonly"] is False
    assert create_call["capabilities"] == list(DOCKER_DEFAULT_CAPABILITIES)
    assert "_oci_spec_transform_fn" not in create_call


def test_boot_failure_is_pollable_not_raised() -> None:
    runtime = FakeSandboxRuntime()
    runtime.pull_error = SandboxCreationError("registry says no")
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        return await host.describe()

    info = asyncio.run(scenario())
    assert info["status"] == "error"
    assert "registry says no" in info["error"]
    # Nothing was created, so nothing to delete.
    assert runtime.deleted == []


def test_boot_create_failure_cleans_up_nothing_and_reports() -> None:
    runtime = FakeSandboxRuntime()
    runtime.create_error = SandboxCreationError("runsc not found")
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        return await host.describe()

    info = asyncio.run(scenario())
    assert info["status"] == "error"
    assert "runsc not found" in info["error"]


def test_boot_is_idempotent() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    async def scenario() -> None:
        await asyncio.gather(host.boot(), host.boot())
        await host.boot()

    asyncio.run(scenario())
    assert len(runtime.pull_calls) == 1
    assert len(runtime.create_calls) == 1


def test_describe_long_poll_wakes_on_status_change() -> None:
    runtime = FakeSandboxRuntime()
    runtime.pull_gate = threading.Event()
    host = _make_host(runtime)

    async def scenario() -> dict:
        boot_task = asyncio.create_task(host.boot())
        # Wait until the pull is actually in flight.
        while not runtime.pull_calls:
            await asyncio.sleep(0.01)
        assert (await host.describe())["status"] == "pulling"

        waiter = asyncio.create_task(host.describe(wait_seconds=10.0))
        await asyncio.sleep(0.05)
        runtime.pull_gate.set()
        info = await asyncio.wait_for(waiter, timeout=5.0)
        await boot_task
        return info

    info = asyncio.run(scenario())
    # The long-poll woke on the pulling->starting transition (or later).
    assert info["status"] in ("starting", "running")


# ----------------------------------------------------------------------
# Capabilities and network passthrough (behavior itself is covered by the
# core sandbox tests; the host only forwards SandboxConfig fields)
# ----------------------------------------------------------------------


def test_network_mode_passed_through_natively() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime, spec_overrides={"network": "sandbox"})
    asyncio.run(host.boot())

    (create_call,) = runtime.create_calls
    assert create_call["network"] == "sandbox"


def test_explicit_capabilities_passed_through() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime, spec_overrides={"capabilities": ["CAP_CHOWN"]})
    asyncio.run(host.boot())

    (create_call,) = runtime.create_calls
    assert create_call["capabilities"] == ["CAP_CHOWN"]


def test_missing_capabilities_default_to_docker_set() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime, spec_overrides={"capabilities": None})
    asyncio.run(host.boot())

    (create_call,) = runtime.create_calls
    assert create_call["capabilities"] == list(DOCKER_DEFAULT_CAPABILITIES)


# ----------------------------------------------------------------------
# Exec jobs
# ----------------------------------------------------------------------


def test_exec_job_completes_with_passthrough_args() -> None:
    runtime = FakeSandboxRuntime()
    runtime.exec_results = [
        FakeExecResult(exit_code=7, stdout="out", stderr="err", duration_seconds=1.5)
    ]
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        started = await host.start_exec(
            ["bash", "-c", "exit 7"],
            cwd="/app",
            env={"K": "V"},
            timeout_seconds=30.0,
        )
        assert started["status"] == "running"
        return await host.get_exec(started["exec_id"], wait_seconds=10.0)

    info = asyncio.run(scenario())
    assert info["status"] == "completed"
    assert info["exit_code"] == 7
    assert info["stdout"] == "out"
    assert info["stderr"] == "err"
    assert info["duration_seconds"] == 1.5
    assert info["stdout_truncated"] is False

    exec_call = runtime.exec_calls[-1]
    assert exec_call["command"] == ["bash", "-c", "exit 7"]
    assert exec_call["cwd"] == "/app"
    assert exec_call["env"] == {"K": "V"}
    assert exec_call["timeout"] == 30.0


def test_exec_timeout_maps_to_timeout_status() -> None:
    runtime = FakeSandboxRuntime()
    runtime.exec_error = SandboxTimeoutError("too slow")
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        started = await host.start_exec("sleep 100", timeout_seconds=1.0)
        return await host.get_exec(started["exec_id"], wait_seconds=10.0)

    info = asyncio.run(scenario())
    assert info["status"] == "timeout"
    assert "timed out after 1.0 seconds" in info["error"]
    assert info["exit_code"] is None


@pytest.mark.parametrize(
    "error", [SandboxExecError("backend broke"), ValueError("surprise")]
)
def test_exec_errors_map_to_error_status(error: Exception) -> None:
    runtime = FakeSandboxRuntime()
    runtime.exec_error = error
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        started = await host.start_exec("true")
        return await host.get_exec(started["exec_id"], wait_seconds=10.0)

    info = asyncio.run(scenario())
    assert info["status"] == "error"
    assert str(error) in info["error"]


def test_exec_output_truncated_with_marker() -> None:
    runtime = FakeSandboxRuntime()
    runtime.exec_results = [FakeExecResult(stdout="x" * 100, stderr="y")]
    host = _make_host(runtime, settings_overrides={"max_output_bytes": 10})

    async def scenario() -> dict:
        await host.boot()
        started = await host.start_exec("true")
        return await host.get_exec(started["exec_id"], wait_seconds=10.0)

    info = asyncio.run(scenario())
    assert info["stdout_truncated"] is True
    assert info["stdout"].startswith("x" * 10)
    assert "[truncated by ray-sandbox" in info["stdout"]
    assert info["stderr_truncated"] is False
    assert info["stderr"] == "y"


def test_exec_rejected_while_not_running() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    async def scenario() -> dict:
        return await host.start_exec("true")

    result = asyncio.run(scenario())
    assert result["error_code"] == "conflict"
    assert "pending" in result["message"]


def test_get_exec_unknown_id() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    async def scenario() -> dict:
        await host.boot()
        return await host.get_exec("ex-doesnotexist")

    result = asyncio.run(scenario())
    assert result["error_code"] == "exec_not_found"


def test_exec_history_evicts_oldest_finished() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime, settings_overrides={"max_exec_history": 2})

    async def scenario() -> tuple:
        await host.boot()
        ids = []
        for _ in range(3):
            started = await host.start_exec("true")
            await host.get_exec(started["exec_id"], wait_seconds=10.0)
            ids.append(started["exec_id"])
        first = await host.get_exec(ids[0])
        last = await host.get_exec(ids[-1])
        return first, last

    first, last = asyncio.run(scenario())
    assert first["error_code"] == "exec_not_found"
    assert last["status"] == "completed"


# ----------------------------------------------------------------------
# Files
# ----------------------------------------------------------------------


def test_file_roundtrip_and_not_found() -> None:
    runtime = FakeSandboxRuntime()
    runtime.readable_files["/data/in.txt"] = b"hello"
    host = _make_host(runtime)

    async def scenario() -> tuple:
        await host.boot()
        wrote = await host.write_file("/data/out.txt", b"payload")
        read = await host.read_file("/data/in.txt")
        missing = await host.read_file("/data/nope.txt")
        return wrote, read, missing

    wrote, read, missing = asyncio.run(scenario())
    assert wrote == {"ok": True}
    assert runtime.written_files["/data/out.txt"] == b"payload"
    assert read["ok"] is True
    assert read["content"] == b"hello"
    assert missing["error_code"] == "file_not_found"


def test_file_ops_conflict_before_running() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    async def scenario() -> tuple:
        return (
            await host.write_file("/x", b"1"),
            await host.read_file("/x"),
        )

    wrote, read = asyncio.run(scenario())
    assert wrote["error_code"] == "conflict"
    assert read["error_code"] == "conflict"


# ----------------------------------------------------------------------
# TTL and termination
# ----------------------------------------------------------------------


def test_ttl_deletes_sandbox_and_marks_terminated() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime, spec_overrides={"ttl_seconds": 0.05})

    async def scenario() -> dict:
        await host.boot()
        await asyncio.sleep(0.3)
        return await host.describe()

    info = asyncio.run(scenario())
    assert info["status"] == "terminated"
    assert runtime.deleted == [runtime.instance_id]


def test_terminate_cancels_running_exec_and_deletes() -> None:
    runtime = FakeSandboxRuntime()
    exec_gate = threading.Event()
    original_exec = runtime.exec

    def blocking_exec(*args, **kwargs):
        exec_gate.wait(timeout=30)
        return original_exec(*args, **kwargs)

    runtime.exec = blocking_exec
    host = _make_host(runtime)

    async def scenario() -> tuple:
        await host.boot()
        started = await host.start_exec("sleep forever")
        await asyncio.sleep(0.05)
        try:
            result = await host.terminate()
            info = await host.describe()
            exec_info = await host.get_exec(started["exec_id"])
        finally:
            exec_gate.set()
        return result, info, exec_info

    result, info, exec_info = asyncio.run(scenario())
    assert result == {"ok": True}
    assert info["status"] == "terminated"
    assert runtime.deleted == [runtime.instance_id]
    assert exec_info["status"] == "error"
    assert "terminated" in exec_info["error"]


def test_terminate_is_idempotent() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    async def scenario() -> None:
        await host.boot()
        await host.terminate()
        await host.terminate()

    asyncio.run(scenario())
    assert runtime.deleted == [runtime.instance_id]


def test_shell_passthrough() -> None:
    """A sandbox-level shell reaches runtime.create only when set, and a
    per-exec shell reaches exec_async."""
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime, spec_overrides={"shell": "/bin/sh"})

    async def scenario() -> dict:
        await host.boot()
        started = await host.start_exec("echo hi", shell="/bin/dash")
        return await host.get_exec(started["exec_id"], wait_seconds=5)

    result = asyncio.run(scenario())
    assert result["status"] == "completed"
    assert runtime.create_calls[0]["shell"] == "/bin/sh"
    assert runtime.exec_calls[0]["shell"] == "/bin/dash"


def test_no_shell_in_spec_keeps_the_runtime_default() -> None:
    runtime = FakeSandboxRuntime()
    host = _make_host(runtime)

    asyncio.run(host.boot())
    # Omitted, not None: SandboxConfig.shell keeps its /bin/bash default.
    assert "shell" not in runtime.create_calls[0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
