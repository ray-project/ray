"""Fixtures for the Ray Sandbox HTTP API tests.

Unlike ``sandbox/tests/conftest.py`` there is no TEST_SANDBOX gate here: the
unit tests fake the sandbox runtime and the actor layer, so they run on any
platform with no cluster and no runsc. Only ``test_http_integration.py``
needs the real thing and gates itself.
"""

import asyncio
import os
import platform
import shutil
import tempfile
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional, Union

import pytest

from ray.experimental.sandbox.exceptions import (
    SandboxExecError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.http.host import SandboxHost


def _sandbox_test_enabled() -> bool:
    try:
        from ray._private.test_utils import sandbox_test_enabled
    except ImportError:
        return os.environ.get("TEST_SANDBOX") == "1"
    return sandbox_test_enabled()


class FakeExecResult:
    def __init__(
        self,
        exit_code: int = 0,
        stdout: str = "",
        stderr: str = "",
        duration_seconds: float = 0.01,
    ) -> None:
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.duration_seconds = duration_seconds


class FakeSandboxRuntime:
    """Scriptable stand-in for ``ray.experimental.sandbox.SandboxRuntime``."""

    def __init__(self) -> None:
        self.instance_id = "ray-sandbox-fake0001"
        self.pull_calls: List[Dict[str, Any]] = []
        self.create_calls: List[Dict[str, Any]] = []
        self.exec_calls: List[Dict[str, Any]] = []
        self.written_files: Dict[str, bytes] = {}
        self.readable_files: Dict[str, bytes] = {}
        self.deleted: List[str] = []
        self.pull_error: Optional[Exception] = None
        self.create_error: Optional[Exception] = None
        self.exec_error: Optional[Exception] = None
        self.exec_results: List[FakeExecResult] = []
        # When set, pull_image blocks until the event fires, holding the
        # sandbox in the "pulling" state for conflict tests.
        self.pull_gate: Optional[threading.Event] = None

    def pull_image(self, image: str, timeout_seconds: float = 120.0) -> str:
        self.pull_calls.append({"image": image, "timeout_seconds": timeout_seconds})
        if self.pull_gate is not None:
            if not self.pull_gate.wait(timeout=30):
                raise RuntimeError("pull gate never released")
        if self.pull_error is not None:
            raise self.pull_error
        return f"/tmp/fake-images/{image}"

    def create(self, image: str, **kwargs: Any) -> str:
        self.create_calls.append({"image": image, **kwargs})
        if self.create_error is not None:
            raise self.create_error
        return self.instance_id

    def exec(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> FakeExecResult:
        self.exec_calls.append(
            {
                "instance_id": instance_id,
                "command": command,
                "timeout": timeout,
                "cwd": cwd,
                "env": env,
            }
        )
        if self.exec_error is not None:
            raise self.exec_error
        if self.exec_results:
            return self.exec_results.pop(0)
        return FakeExecResult()

    async def exec_async(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> FakeExecResult:
        return await asyncio.to_thread(
            self.exec, instance_id, command, timeout=timeout, cwd=cwd, env=env
        )

    def write_file(
        self, instance_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        if isinstance(content, str):
            content = content.encode("utf-8")
        self.written_files[path] = content

    def read_file(self, instance_id: str, path: str) -> bytes:
        if path not in self.readable_files:
            raise SandboxExecError(f"cat: {path}: No such file or directory")
        return self.readable_files[path]

    def delete(self, instance_id: str) -> None:
        self.deleted.append(instance_id)


class _FakeRemoteMethod:
    """Mimics ``handle.method.remote(...)``: schedules eagerly, returns an awaitable."""

    def __init__(self, fn: Any) -> None:
        self._fn = fn

    def remote(self, *args: Any, **kwargs: Any) -> "asyncio.Task":
        return asyncio.get_running_loop().create_task(self._fn(*args, **kwargs))


class FakeHandle:
    def __init__(self, host: SandboxHost) -> None:
        self.host = host

    def __getattr__(self, name: str) -> _FakeRemoteMethod:
        return _FakeRemoteMethod(getattr(self.host, name))


class FakeResolver:
    """In-process stand-in for ``RayActorHandleResolver``."""

    def __init__(self) -> None:
        self.handles: Dict[str, FakeHandle] = {}
        self.runtimes: List[FakeSandboxRuntime] = []
        self.create_options: List[Dict[str, Any]] = []
        self.killed: List[str] = []
        # Applied to the next runtime a created host builds.
        self.next_runtime: Optional[FakeSandboxRuntime] = None

    def _runtime_factory(self) -> FakeSandboxRuntime:
        runtime = self.next_runtime or FakeSandboxRuntime()
        self.next_runtime = None
        self.runtimes.append(runtime)
        return runtime

    def create(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
    ) -> FakeHandle:
        self.create_options.append({"name": name, **actor_options})
        # get_if_exists semantics: racing creates converge on one host.
        if name in self.handles:
            return self.handles[name]
        host = SandboxHost(runtime_factory=self._runtime_factory, **ctor_kwargs)
        handle = FakeHandle(host)
        self.handles[name] = handle
        return handle

    def get(self, name: str) -> Optional[FakeHandle]:
        return self.handles.get(name)

    def list_names(self) -> List[str]:
        return list(self.handles)

    def kill(self, handle: FakeHandle) -> None:
        for name, existing in list(self.handles.items()):
            if existing is handle:
                del self.handles[name]
                self.killed.append(name)


@pytest.fixture
def fake_resolver() -> FakeResolver:
    return FakeResolver()


@pytest.fixture(scope="session", autouse=True)
def ensure_runsc():
    """Provision runsc for the integration test, mirroring sandbox/tests."""
    if not _sandbox_test_enabled():
        return

    os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"

    if not shutil.which("runsc"):
        temp_bin = tempfile.mkdtemp()
        os.chmod(temp_bin, 0o755)
        runsc_path = os.path.join(temp_bin, "runsc")
        arch = (
            "aarch64"
            if platform.machine().lower() in ("aarch64", "arm64")
            else "x86_64"
        )
        url = f"https://storage.googleapis.com/gvisor/releases/release/latest/{arch}/runsc"
        try:
            urllib.request.urlretrieve(url, runsc_path)
            os.chmod(runsc_path, 0o755)
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.02) -> None:
    """Poll *predicate* until true or fail the test."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("condition not met within timeout")


__all__ = [
    "FakeExecResult",
    "FakeSandboxRuntime",
    "FakeHandle",
    "FakeResolver",
    "SandboxExecError",
    "SandboxTimeoutError",
    "wait_until",
]
