"""End-to-end test of the HTTP API against a real Serve deployment.

Requires TEST_SANDBOX=1, a local Ray, and runsc (the session fixture in
conftest.py downloads one on Linux). Runs in the Buildkite sandbox job, which
is privileged and sets TEST_SANDBOX=1.
"""

import os
import shutil
import sys
import time

import pytest


def _sandbox_test_enabled() -> bool:
    try:
        from ray._private.test_utils import sandbox_test_enabled
    except ImportError:
        return os.environ.get("TEST_SANDBOX") == "1"
    return sandbox_test_enabled()


pytestmark = pytest.mark.skipif(
    not _sandbox_test_enabled(),
    reason="Sandbox tests are only run when TEST_SANDBOX=1",
)

_TOKEN = "integration-test-token"
_IMAGE = "busybox:latest"


@pytest.fixture(scope="module")
def api_base_url():
    if not shutil.which("runsc"):
        pytest.skip("runsc is not on PATH")

    # Ray Serve is stripped from the core sandbox CI image (the sandbox
    # test job installs with --install-mask all-ray-libraries): the module
    # stays importable but loses serve.run, so this end-to-end test only
    # runs where Serve is fully installed, e.g. a dev container.
    serve = pytest.importorskip("ray.serve")
    if not hasattr(serve, "run"):
        pytest.skip("ray.serve is present but not fully installed")

    os.environ["RAY_SANDBOX_API_TOKEN"] = _TOKEN

    import ray
    from ray.experimental.sandbox.http.app import build_app

    ray.init()
    serve.run(build_app({}), name="sandbox-api-integration")
    try:
        yield "http://127.0.0.1:8000/api/v1"
    finally:
        serve.shutdown()
        ray.shutdown()


def _wait_for(fetch, accept, timeout: float = 300.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = fetch()
        if accept(last):
            return last
        time.sleep(1)
    raise AssertionError(f"timed out waiting; last state: {last}")


def test_auth_is_enforced(api_base_url):
    import httpx

    with httpx.Client(timeout=30) as anonymous:
        assert anonymous.get(f"{api_base_url}/health").status_code == 200
        response = anonymous.get(f"{api_base_url}/sandboxes")
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "unauthorized"


def test_full_sandbox_lifecycle(api_base_url):
    import httpx

    headers = {"Authorization": f"Bearer {_TOKEN}"}
    with httpx.Client(headers=headers, timeout=60) as client:
        response = client.post(
            f"{api_base_url}/sandboxes",
            json={
                "image": _IMAGE,
                "readonly": False,
                "network": "none",
                "ttl_seconds": 600,
                "labels": {"suite": "integration"},
            },
        )
        assert response.status_code == 202, response.text
        sandbox_id = response.json()["sandbox_id"]

        info = _wait_for(
            lambda: client.get(
                f"{api_base_url}/sandboxes/{sandbox_id}",
                params={"wait_seconds": 10},
            ).json(),
            lambda i: i["status"] in ("running", "error"),
        )
        assert info["status"] == "running", info

        # Exec: submit then poll.
        response = client.post(
            f"{api_base_url}/sandboxes/{sandbox_id}/execs",
            json={
                "command": ["sh", "-c", "echo hello-from-sandbox && exit 4"],
                "timeout_seconds": 60,
            },
        )
        assert response.status_code == 202, response.text
        exec_id = response.json()["exec_id"]
        result = _wait_for(
            lambda: client.get(
                f"{api_base_url}/sandboxes/{sandbox_id}/execs/{exec_id}",
                params={"wait_seconds": 10},
            ).json(),
            lambda r: r["status"] != "running",
            timeout=120,
        )
        assert result["status"] == "completed", result
        assert result["exit_code"] == 4
        assert "hello-from-sandbox" in result["stdout"]

        # Files: round-trip through the API and verify inside the sandbox.
        payload = b"file-payload-123"
        response = client.put(
            f"{api_base_url}/sandboxes/{sandbox_id}/files",
            params={"path": "/tmp/in.txt"},
            content=payload,
        )
        assert response.status_code == 204, response.text
        response = client.get(
            f"{api_base_url}/sandboxes/{sandbox_id}/files",
            params={"path": "/tmp/in.txt"},
        )
        assert response.status_code == 200
        assert response.content == payload

        response = client.get(
            f"{api_base_url}/sandboxes/{sandbox_id}/files",
            params={"path": "/tmp/does-not-exist"},
        )
        assert response.status_code == 404

        # Listing sees it.
        response = client.get(
            f"{api_base_url}/sandboxes", params=[("label", "suite=integration")]
        )
        assert sandbox_id in {s["sandbox_id"] for s in response.json()["sandboxes"]}

        # Delete is effective and idempotent.
        assert (
            client.delete(f"{api_base_url}/sandboxes/{sandbox_id}").status_code == 200
        )
        _wait_for(
            lambda: client.get(f"{api_base_url}/sandboxes/{sandbox_id}").status_code,
            lambda code: code == 404,
            timeout=60,
        )
        assert (
            client.delete(f"{api_base_url}/sandboxes/{sandbox_id}").status_code == 200
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
