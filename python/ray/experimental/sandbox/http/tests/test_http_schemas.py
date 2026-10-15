"""Contract snapshot for the Ray Sandbox HTTP API.

Pins the v1 surface (paths, methods, and the fields of the wire models) so a
change to it is a deliberate act — clients like Harbor's ``ray-sandbox``
environment are written against exactly this shape. If this test fails, you
are changing the API contract: update it and the consumers together.
"""

import sys

import pytest

from ray.experimental.sandbox.http.app import create_app
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.experimental.sandbox.http.tests.conftest import FakeResolver

EXPECTED_OPERATIONS = {
    ("/api/v1/health", "get"),
    ("/api/v1/sandboxes", "get"),
    ("/api/v1/sandboxes", "post"),
    ("/api/v1/sandboxes/{sandbox_id}", "get"),
    ("/api/v1/sandboxes/{sandbox_id}", "delete"),
    ("/api/v1/sandboxes/{sandbox_id}/execs", "post"),
    ("/api/v1/sandboxes/{sandbox_id}/execs/{exec_id}", "get"),
    ("/api/v1/sandboxes/{sandbox_id}/files", "put"),
    ("/api/v1/sandboxes/{sandbox_id}/files", "get"),
}

EXPECTED_MODEL_FIELDS = {
    "CreateSandboxRequest": {
        "image",
        "env",
        "workdir",
        "ttl_seconds",
        "network",
        "rootless",
        "readonly",
        "resources",
        "labels",
        "capabilities",
        "image_pull_timeout_seconds",
        "start_timeout_seconds",
        "client_token",
        "dns",
        "shell",
    },
    "ResourceSpec": {
        "cpu_request",
        "cpu_limit",
        "memory_request_mb",
        "memory_limit_mb",
        "custom",
    },
    "SandboxInfo": {
        "sandbox_id",
        "status",
        "image",
        "created_at",
        "ttl_seconds",
        "expires_at",
        "network",
        "labels",
        "error",
    },
    "StartExecRequest": {
        "command",
        "cwd",
        "env",
        "timeout_seconds",
        "shell",
        "user",
    },
    "ExecStarted": {"exec_id", "status"},
    "ExecInfo": {
        "exec_id",
        "status",
        "exit_code",
        "stdout",
        "stderr",
        "stdout_truncated",
        "stderr_truncated",
        "duration_seconds",
        "error",
    },
}

_HTTP_METHODS = {"get", "post", "put", "delete", "patch", "head", "options"}


def _openapi() -> dict:
    app = create_app(SandboxAPISettings(), handle_resolver=FakeResolver())
    return app.openapi()


def test_v1_operations_are_exactly_the_contract() -> None:
    schema = _openapi()
    operations = {
        (path, method)
        for path, item in schema["paths"].items()
        for method in item
        if method in _HTTP_METHODS
    }
    assert operations == EXPECTED_OPERATIONS


def test_wire_models_carry_exactly_the_contract_fields() -> None:
    schema = _openapi()
    components = schema["components"]["schemas"]
    for model, expected_fields in EXPECTED_MODEL_FIELDS.items():
        assert model in components, f"model {model} missing from the OpenAPI schema"
        actual = set(components[model].get("properties", {}))
        assert actual == expected_fields, f"{model} fields drifted"


def test_sandbox_status_enum_is_stable() -> None:
    schema = _openapi()
    components = schema["components"]["schemas"]
    status = components["SandboxInfo"]["properties"]["status"]
    # pydantic may inline or $ref the literal; normalize.
    enum = status.get("enum") or components.get(
        status.get("$ref", "").rsplit("/", 1)[-1], {}
    ).get("enum")
    assert set(enum) == {
        "pending",
        "pulling",
        "starting",
        "running",
        "error",
        "terminated",
    }


def test_network_modes_track_the_core_config() -> None:
    """Mode validation is delegated to the core config's list, so a new Ray
    mode is accepted here without an API change."""
    from ray.experimental.sandbox.config import VALID_NETWORK_MODES
    from ray.experimental.sandbox.http.schemas import CreateSandboxRequest

    for mode in VALID_NETWORK_MODES:
        request = CreateSandboxRequest(image="python:3.12", network=mode)
        assert request.network == mode
    with pytest.raises(ValueError, match="network must be one of"):
        CreateSandboxRequest(image="python:3.12", network="bridge")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
