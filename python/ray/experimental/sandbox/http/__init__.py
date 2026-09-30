"""HTTP API service for Ray Sandbox.

This subpackage exposes ``ray.experimental.sandbox`` over a versioned REST
API (``/api/v1``) served by Ray Serve, so sandboxes can be managed from
outside the Ray cluster with nothing but an HTTP client and a bearer token.

The REST app (``build_app``, ``create_app``) requires the Serve extra
(``pip install "ray[serve]"``). The package itself imports without FastAPI,
so its other submodules need only ``ray[default]``: the gRPC facade
(``grpc_facade``) adds just ``grpclib``. The base
``ray.experimental.sandbox`` package never imports this one.
"""

from typing import Any

# Resolved on first access (PEP 562) so that importing a submodule, such as
# the gRPC facade, does not import FastAPI through app.py.
_EXPORTS = {
    "build_app": "app",
    "create_app": "app",
    "SandboxHost": "host",
    "DOCKER_DEFAULT_CAPABILITIES": "schemas",
    "CreateSandboxRequest": "schemas",
    "ExecInfo": "schemas",
    "ResourceSpec": "schemas",
    "SandboxAPISettings": "schemas",
    "SandboxInfo": "schemas",
    "StartExecRequest": "schemas",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(f"{__name__}.{module}"), name)
