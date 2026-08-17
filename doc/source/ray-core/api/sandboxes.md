---
myst:
  html_meta:
    description: "API reference for Ray Sandboxes (gVisor-isolated container environments)."
---

(ray-sandbox-ref)=

# Sandbox API

:::{note}
Ray Sandboxes (`ray.experimental.sandbox`) is an {ref}`alpha <api-stability-alpha>` library. The API can change before it graduates to stable.
:::

For an introduction and usage guides, see {ref}`ray-core-sandboxes`.

## Sandbox lifecycle and execution

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.experimental.sandbox.create
    ray.experimental.sandbox.Sandbox
    ray.experimental.sandbox.SandboxRuntime
```

## Data structures and status

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ray.experimental.sandbox.ExecResult
    ray.experimental.sandbox.SandboxStatus
```

## Exceptions

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.experimental.sandbox.SandboxError
    ray.experimental.sandbox.SandboxCreationError
    ray.experimental.sandbox.SandboxTimeoutError
    ray.experimental.sandbox.SandboxExecError
    ray.experimental.sandbox.SandboxNotFoundError
```
