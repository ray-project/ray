---
myst:
  html_meta:
    description: "API reference for Ray Serve deployment handles in ray.serve.handle, plus the ray.serve functions that return a handle to a running application or deployment."
---

(serve-api-handle)=

# Deployment handles

Deployment handles call one deployment from another, or call a running application from Python. The handle and response classes live in `ray.serve.handle`.

:::{note}
The deprecated `RayServeHandle` and `RayServeSyncHandle` APIs have been fully removed as of Ray 2.10. See the [model composition guide](serve-model-composition) for how to update code to use the {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` API instead.
:::

```{eval-rst}
.. currentmodule:: ray
```

## Handle and response classes

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_init_args.rst

   serve.handle.DeploymentHandle
   serve.handle.DeploymentResponse
   serve.handle.DeploymentResponseGenerator
   serve.handle.DeploymentBroadcastResponse
```

## Get a handle

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.get_app_handle
   serve.get_deployment_handle
```
