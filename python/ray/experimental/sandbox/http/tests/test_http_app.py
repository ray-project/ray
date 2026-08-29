"""HTTP-layer tests: FastAPI app + fake resolver + real SandboxHost logic.

No Ray cluster and no runsc: the resolver seam replaces actor creation with
in-process ``SandboxHost`` instances driven by ``FakeSandboxRuntime``. The
``TestClient`` context manager keeps one event loop alive across requests so
background boot tasks progress between calls, exactly like a live server.
"""

import sys

import pytest
from fastapi.testclient import TestClient

from ray.experimental.sandbox.http.app import create_app
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.experimental.sandbox.http.tests.conftest import (
    FakeExecResult,
    FakeResolver,
    FakeSandboxRuntime,
)

BASE = "/api/v1"


def _client(resolver: FakeResolver, settings: SandboxAPISettings = None) -> TestClient:
    app = create_app(settings or SandboxAPISettings(), handle_resolver=resolver)
    return TestClient(app)


def _create_sandbox(client: TestClient, **overrides) -> dict:
    body = {"image": "python:3.12-slim", "readonly": False, **overrides}
    response = client.post(f"{BASE}/sandboxes", json=body)
    assert response.status_code == 202, response.text
    return response.json()


def _wait_running(client: TestClient, sandbox_id: str) -> dict:
    for _ in range(100):
        response = client.get(
            f"{BASE}/sandboxes/{sandbox_id}", params={"wait_seconds": 1}
        )
        assert response.status_code == 200, response.text
        info = response.json()
        if info["status"] not in ("pending", "pulling", "starting"):
            return info
    raise AssertionError("sandbox never left its boot states")


# ----------------------------------------------------------------------
# Auth
# ----------------------------------------------------------------------


def test_bearer_auth_enforced_when_token_configured(
    fake_resolver: FakeResolver, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RAY_SANDBOX_API_TOKEN", "sekret")
    with _client(fake_resolver) as client:
        # Health stays public.
        assert client.get(f"{BASE}/health").status_code == 200

        response = client.get(f"{BASE}/sandboxes")
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "unauthorized"

        response = client.get(
            f"{BASE}/sandboxes", headers={"Authorization": "Bearer wrong"}
        )
        assert response.status_code == 401

        response = client.get(
            f"{BASE}/sandboxes", headers={"Authorization": "Bearer sekret"}
        )
        assert response.status_code == 200


def test_auth_disabled_without_token(
    fake_resolver: FakeResolver, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RAY_SANDBOX_API_TOKEN", raising=False)
    with _client(fake_resolver) as client:
        assert client.get(f"{BASE}/sandboxes").status_code == 200


# ----------------------------------------------------------------------
# Sandbox lifecycle
# ----------------------------------------------------------------------


def test_create_then_poll_until_running(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(client, labels={"team": "eval"})
        # The boot task starts immediately, so the 202 body reports whatever
        # early state it reached — any pre-terminal status is legitimate.
        assert info["status"] in ("pending", "pulling", "starting", "running")
        assert info["sandbox_id"].startswith("sb-")
        assert info["image"] == "python:3.12-slim"
        assert info["labels"] == {"team": "eval"}

        running = _wait_running(client, info["sandbox_id"])
        assert running["status"] == "running"
        assert running["error"] is None

        (runtime,) = fake_resolver.runtimes
        assert runtime.create_calls[0]["readonly"] is False


def test_create_maps_resources_to_actor_options_and_limits(
    fake_resolver: FakeResolver,
) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(
            client,
            resources={
                "cpu_request": 0.5,
                "cpu_limit": 2.0,
                "memory_request_mb": 256,
                "memory_limit_mb": 2048,
                "custom": {"gvisor": 1.0},
            },
        )
        _wait_running(client, info["sandbox_id"])

    (options,) = fake_resolver.create_options
    assert options["num_cpus"] == 0.5
    assert options["memory"] == 256 * 1024 * 1024
    assert options["resources"] == {"gvisor": 1.0}

    (runtime,) = fake_resolver.runtimes
    create_call = runtime.create_calls[0]
    assert create_call["cpu"] == 2.0
    assert create_call["memory"] == "2048Mi"


def test_create_defaults_request_to_limit(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        _create_sandbox(client, resources={"cpu_limit": 4.0, "memory_limit_mb": 512})

    (options,) = fake_resolver.create_options
    assert options["num_cpus"] == 4.0
    assert options["memory"] == 512 * 1024 * 1024


def test_create_default_actor_cpus_without_resources(
    fake_resolver: FakeResolver,
) -> None:
    with _client(fake_resolver) as client:
        _create_sandbox(client)

    (options,) = fake_resolver.create_options
    assert options["num_cpus"] == 1.0
    assert "memory" not in options


def test_create_ttl_over_cap_rejected(fake_resolver: FakeResolver) -> None:
    settings = SandboxAPISettings(max_ttl_seconds=100)
    with _client(fake_resolver, settings) as client:
        response = client.post(
            f"{BASE}/sandboxes",
            json={"image": "x", "ttl_seconds": 101},
        )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_request"


def test_create_null_ttl_clamped_to_server_cap(
    fake_resolver: FakeResolver,
) -> None:
    settings = SandboxAPISettings(max_ttl_seconds=1234)
    with _client(fake_resolver, settings) as client:
        info = _create_sandbox(client, ttl_seconds=None)
        assert info["ttl_seconds"] == 1234


def test_create_missing_image_is_422(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        assert client.post(f"{BASE}/sandboxes", json={}).status_code == 422


def test_client_token_makes_create_idempotent(
    fake_resolver: FakeResolver,
) -> None:
    with _client(fake_resolver) as client:
        first = client.post(
            f"{BASE}/sandboxes",
            json={"image": "x", "client_token": "trial-42"},
        )
        assert first.status_code == 202
        second = client.post(
            f"{BASE}/sandboxes",
            json={"image": "x", "client_token": "trial-42"},
        )
        assert second.status_code == 200
        assert second.json()["sandbox_id"] == first.json()["sandbox_id"]
        assert len(fake_resolver.runtimes) == 1


def test_get_unknown_sandbox_404(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        response = client.get(f"{BASE}/sandboxes/sb-missing")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "sandbox_not_found"


def test_delete_is_idempotent_and_kills_actor(
    fake_resolver: FakeResolver,
) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)

        response = client.delete(f"{BASE}/sandboxes/{sandbox_id}")
        assert response.status_code == 200
        assert response.json() == {
            "sandbox_id": sandbox_id,
            "status": "terminated",
        }
        assert fake_resolver.killed == [sandbox_id]
        (runtime,) = fake_resolver.runtimes
        assert runtime.deleted == [runtime.instance_id]

        # Gone now, and deleting again still succeeds.
        assert client.get(f"{BASE}/sandboxes/{sandbox_id}").status_code == 404
        assert client.delete(f"{BASE}/sandboxes/{sandbox_id}").status_code == 200


def test_boot_failure_surfaces_as_error_status(
    fake_resolver: FakeResolver,
) -> None:
    runtime = FakeSandboxRuntime()
    runtime.pull_error = RuntimeError("no such image")
    fake_resolver.next_runtime = runtime
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        final = _wait_running(client, info["sandbox_id"])
        assert final["status"] == "error"
        assert "no such image" in final["error"]


def test_list_sandboxes_with_label_filter(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        a = _create_sandbox(client, labels={"job": "j1", "role": "env"})
        b = _create_sandbox(client, labels={"job": "j2"})

        response = client.get(f"{BASE}/sandboxes")
        assert response.status_code == 200
        assert {s["sandbox_id"] for s in response.json()["sandboxes"]} == {
            a["sandbox_id"],
            b["sandbox_id"],
        }

        response = client.get(f"{BASE}/sandboxes", params=[("label", "job=j1")])
        assert [s["sandbox_id"] for s in response.json()["sandboxes"]] == [
            a["sandbox_id"]
        ]

        response = client.get(f"{BASE}/sandboxes", params=[("label", "nonsense")])
        assert response.status_code == 400


# ----------------------------------------------------------------------
# Execs
# ----------------------------------------------------------------------


def test_exec_submit_and_poll(fake_resolver: FakeResolver) -> None:
    runtime = FakeSandboxRuntime()
    runtime.exec_results = [FakeExecResult(exit_code=3, stdout="done")]
    fake_resolver.next_runtime = runtime
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)

        response = client.post(
            f"{BASE}/sandboxes/{sandbox_id}/execs",
            json={
                "command": ["bash", "-c", "exit 3"],
                "cwd": "/app",
                "env": {"A": "1"},
                "timeout_seconds": 5,
            },
        )
        assert response.status_code == 202, response.text
        exec_id = response.json()["exec_id"]
        assert exec_id.startswith("ex-")

        response = client.get(
            f"{BASE}/sandboxes/{sandbox_id}/execs/{exec_id}",
            params={"wait_seconds": 10},
        )
        assert response.status_code == 200
        result = response.json()
        assert result["status"] == "completed"
        assert result["exit_code"] == 3
        assert result["stdout"] == "done"

        exec_call = runtime.exec_calls[-1]
        assert exec_call["command"] == ["bash", "-c", "exit 3"]
        assert exec_call["cwd"] == "/app"
        assert exec_call["env"] == {"A": "1"}
        assert exec_call["timeout"] == 5


def test_exec_on_unknown_sandbox_404(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        response = client.post(
            f"{BASE}/sandboxes/sb-missing/execs", json={"command": "true"}
        )
        assert response.status_code == 404


def test_unknown_exec_id_404(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)
        response = client.get(f"{BASE}/sandboxes/{sandbox_id}/execs/ex-nope")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "exec_not_found"


def test_exec_while_booting_is_409(fake_resolver: FakeResolver) -> None:
    import threading

    runtime = FakeSandboxRuntime()
    runtime.pull_gate = threading.Event()
    fake_resolver.next_runtime = runtime
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]

        response = client.post(
            f"{BASE}/sandboxes/{sandbox_id}/execs", json={"command": "true"}
        )
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "conflict"

        runtime.pull_gate.set()
        _wait_running(client, sandbox_id)
        response = client.post(
            f"{BASE}/sandboxes/{sandbox_id}/execs", json={"command": "true"}
        )
        assert response.status_code == 202


def test_exec_timeout_over_cap_rejected(fake_resolver: FakeResolver) -> None:
    settings = SandboxAPISettings(max_exec_timeout_seconds=10.0)
    with _client(fake_resolver, settings) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)
        response = client.post(
            f"{BASE}/sandboxes/{sandbox_id}/execs",
            json={"command": "true", "timeout_seconds": 11},
        )
        assert response.status_code == 400


def test_empty_command_is_422(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)
        response = client.post(
            f"{BASE}/sandboxes/{sandbox_id}/execs", json={"command": "  "}
        )
        assert response.status_code == 422


# ----------------------------------------------------------------------
# Files
# ----------------------------------------------------------------------


def test_file_put_and_get_roundtrip(fake_resolver: FakeResolver) -> None:
    runtime = FakeSandboxRuntime()
    runtime.readable_files["/data/report.bin"] = b"\x00\x01binary"
    fake_resolver.next_runtime = runtime
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)

        response = client.put(
            f"{BASE}/sandboxes/{sandbox_id}/files",
            params={"path": "/data/in.bin"},
            content=b"payload-bytes",
        )
        assert response.status_code == 204
        assert runtime.written_files["/data/in.bin"] == b"payload-bytes"

        response = client.get(
            f"{BASE}/sandboxes/{sandbox_id}/files",
            params={"path": "/data/report.bin"},
        )
        assert response.status_code == 200
        assert response.content == b"\x00\x01binary"
        assert response.headers["content-type"].startswith("application/octet-stream")


def test_file_get_missing_404(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)
        response = client.get(
            f"{BASE}/sandboxes/{sandbox_id}/files", params={"path": "/nope"}
        )
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "file_not_found"


def test_file_put_too_large_413(fake_resolver: FakeResolver) -> None:
    settings = SandboxAPISettings(max_file_bytes=8)
    with _client(fake_resolver, settings) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        _wait_running(client, sandbox_id)
        response = client.put(
            f"{BASE}/sandboxes/{sandbox_id}/files",
            params={"path": "/big"},
            content=b"123456789",
        )
        assert response.status_code == 413
        assert response.json()["error"]["code"] == "payload_too_large"


def test_file_relative_path_400(fake_resolver: FakeResolver) -> None:
    with _client(fake_resolver) as client:
        info = _create_sandbox(client)
        sandbox_id = info["sandbox_id"]
        response = client.put(
            f"{BASE}/sandboxes/{sandbox_id}/files",
            params={"path": "relative/path"},
            content=b"x",
        )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_request"


def test_file_ops_while_booting_409(fake_resolver: FakeResolver) -> None:
    import threading

    runtime = FakeSandboxRuntime()
    runtime.pull_gate = threading.Event()
    fake_resolver.next_runtime = runtime
    try:
        with _client(fake_resolver) as client:
            info = _create_sandbox(client)
            sandbox_id = info["sandbox_id"]
            response = client.put(
                f"{BASE}/sandboxes/{sandbox_id}/files",
                params={"path": "/x"},
                content=b"x",
            )
            assert response.status_code == 409
    finally:
        runtime.pull_gate.set()


class _UnschedulableHandle:
    """Mimics a handle whose actor Ray reports as permanently unschedulable."""

    def __getattr__(self, name: str):
        class _Method:
            def remote(self, *args, **kwargs):
                import asyncio

                async def _raise():
                    exc_type = type("ActorUnschedulableError", (Exception,), {})
                    raise exc_type("resource shapes cannot fit the cluster")

                return asyncio.get_running_loop().create_task(_raise())

        return _Method()


def test_unschedulable_actor_maps_to_409(fake_resolver: FakeResolver) -> None:
    client = _client(fake_resolver, _fast_settings())
    fake_resolver.handles["sb-unsched0001"] = _UnschedulableHandle()

    response = client.get(f"{BASE}/sandboxes/sb-unsched0001")

    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "unschedulable"


class _StalledHandle:
    """Mimics a handle to a created-but-unscheduled detached actor: every
    remote call returns an awaitable that never resolves."""

    def __getattr__(self, name: str):
        class _Method:
            def remote(self, *args, **kwargs):
                import asyncio

                return asyncio.get_running_loop().create_future()

        return _Method()


def _fast_settings() -> SandboxAPISettings:
    return SandboxAPISettings(scheduling_grace_seconds=0.2)


def test_get_sandbox_reports_pending_while_actor_is_scheduling(
    fake_resolver: FakeResolver,
) -> None:
    client = _client(fake_resolver, _fast_settings())
    fake_resolver.handles["sb-stalled0001"] = _StalledHandle()

    response = client.get(f"{BASE}/sandboxes/sb-stalled0001")

    assert response.status_code == 200, response.text
    info = response.json()
    assert info["status"] == "pending"
    assert info["sandbox_id"] == "sb-stalled0001"


def test_exec_on_scheduling_actor_is_409(fake_resolver: FakeResolver) -> None:
    client = _client(fake_resolver, _fast_settings())
    fake_resolver.handles["sb-stalled0001"] = _StalledHandle()

    response = client.post(
        f"{BASE}/sandboxes/sb-stalled0001/execs", json={"command": "echo hi"}
    )

    assert response.status_code == 409, response.text
    assert "scheduled" in response.json()["error"]["message"]


def test_list_includes_scheduling_sandboxes_as_pending(
    fake_resolver: FakeResolver,
) -> None:
    client = _client(fake_resolver, _fast_settings())
    _create_sandbox(client)
    fake_resolver.handles["sb-stalled0001"] = _StalledHandle()

    response = client.get(f"{BASE}/sandboxes")

    assert response.status_code == 200, response.text
    statuses = {s["sandbox_id"]: s["status"] for s in response.json()["sandboxes"]}
    assert statuses["sb-stalled0001"] == "pending"


def test_shell_passthrough_to_runtime(fake_resolver: FakeResolver) -> None:
    """The create-level and per-exec shell fields reach the sandbox runtime."""
    client = _client(fake_resolver)
    info = _create_sandbox(client, shell="/bin/sh")
    _wait_running(client, info["sandbox_id"])

    response = client.post(
        f"{BASE}/sandboxes/{info['sandbox_id']}/execs",
        json={"command": "echo hi", "shell": "/bin/dash"},
    )
    assert response.status_code == 202, response.text
    exec_id = response.json()["exec_id"]
    result = client.get(
        f"{BASE}/sandboxes/{info['sandbox_id']}/execs/{exec_id}",
        params={"wait_seconds": 5},
    ).json()
    assert result["status"] == "completed"

    runtime = fake_resolver.runtimes[0]
    assert runtime.create_calls[0]["shell"] == "/bin/sh"
    assert runtime.exec_calls[0]["shell"] == "/bin/dash"


def test_exec_user_passthrough(fake_resolver: FakeResolver) -> None:
    client = _client(fake_resolver)
    info = _create_sandbox(client)
    _wait_running(client, info["sandbox_id"])

    response = client.post(
        f"{BASE}/sandboxes/{info['sandbox_id']}/execs",
        json={"command": "id", "user": "1000:1000"},
    )
    assert response.status_code == 202, response.text
    exec_id = response.json()["exec_id"]
    client.get(
        f"{BASE}/sandboxes/{info['sandbox_id']}/execs/{exec_id}",
        params={"wait_seconds": 5},
    )
    assert fake_resolver.runtimes[0].exec_calls[0]["user"] == "1000:1000"


def test_invalid_network_mode_is_422(fake_resolver: FakeResolver) -> None:
    client = _client(fake_resolver)
    response = client.post(
        f"{BASE}/sandboxes", json={"image": "python:3.12", "network": "bridge"}
    )
    assert response.status_code == 422


def test_chunked_file_upload_via_append(fake_resolver: FakeResolver) -> None:
    """PUT ?append=true extends the file, so clients can chunk large uploads
    under proxy body-size limits."""
    client = _client(fake_resolver)
    info = _create_sandbox(client)
    _wait_running(client, info["sandbox_id"])
    base = f"{BASE}/sandboxes/{info['sandbox_id']}/files"

    assert client.put(base, params={"path": "/big"}, content=b"aaa").status_code == 204
    assert (
        client.put(
            base, params={"path": "/big", "append": "true"}, content=b"bbb"
        ).status_code
        == 204
    )

    runtime = fake_resolver.runtimes[0]
    assert runtime.written_files["/big"] == b"aaabbb"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
