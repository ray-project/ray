"""HTTP API service for Ray Sandbox.

This subpackage exposes ``ray.experimental.sandbox`` over a versioned REST
API (``/api/v1``) served by Ray Serve, so sandboxes can be managed from
outside the Ray cluster with nothing but an HTTP client and a bearer token.

Importing this package requires the Serve extra
(``pip install "ray[serve]"``); the base ``ray.experimental.sandbox``
package deliberately never imports it.
"""

from ray.experimental.sandbox.http.app import build_app, create_app
from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.schemas import (
    DOCKER_DEFAULT_CAPABILITIES,
    CreateSandboxRequest,
    ExecInfo,
    ResourceSpec,
    SandboxAPISettings,
    SandboxInfo,
    StartExecRequest,
)

__all__ = [
    "build_app",
    "create_app",
    "SandboxHost",
    "DOCKER_DEFAULT_CAPABILITIES",
    "CreateSandboxRequest",
    "ExecInfo",
    "ResourceSpec",
    "SandboxAPISettings",
    "SandboxInfo",
    "StartExecRequest",
]
