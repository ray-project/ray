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

## Modal-compatible API

An alternative surface that mirrors the [Modal Sandbox API](https://modal.com/docs/guide/sandbox), with streaming command execution and a path-based filesystem namespace. For an introduction and the list of unsupported features, see {ref}`ray-sandbox-modal-api`.

### Sandbox lifecycle and execution

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ray.experimental.sandbox.modal.Sandbox
    ray.experimental.sandbox.modal.ContainerProcess
    ray.experimental.sandbox.modal.SandboxFilesystem
    ray.experimental.sandbox.modal.StreamReader
    ray.experimental.sandbox.modal.StreamWriter
```

### Configuration and data structures

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ray.experimental.sandbox.modal.App
    ray.experimental.sandbox.modal.Image
    ray.experimental.sandbox.modal.StreamType
    ray.experimental.sandbox.modal.types.FileInfo
    ray.experimental.sandbox.modal.types.FileType
    ray.experimental.sandbox.modal.types.FileWatchEvent
    ray.experimental.sandbox.modal.types.FileWatchEventType
```

### Exceptions

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.experimental.sandbox.modal.Error
    ray.experimental.sandbox.modal.InvalidError
    ray.experimental.sandbox.modal.NotFoundError
    ray.experimental.sandbox.modal.SandboxTimeoutError
    ray.experimental.sandbox.modal.SandboxTerminatedError
    ray.experimental.sandbox.modal.SandboxFilesystemError
    ray.experimental.sandbox.modal.SandboxFilesystemNotFoundError
    ray.experimental.sandbox.modal.SandboxFilesystemDirectoryNotEmptyError
    ray.experimental.sandbox.modal.SandboxFilesystemIsADirectoryError
    ray.experimental.sandbox.modal.SandboxFilesystemNotADirectoryError
    ray.experimental.sandbox.modal.SandboxFilesystemPermissionError
    ray.experimental.sandbox.modal.SandboxFilesystemFileTooLargeError
    ray.experimental.sandbox.modal.SandboxFilesystemPathAlreadyExistsError
```
