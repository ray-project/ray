---
myst:
  html_meta:
    description: "API reference for the exceptions Ray Serve raises, defined in ray.serve.exceptions."
---

(serve-api-exceptions)=

# Exceptions

The `ray.serve.exceptions` module defines the errors Serve raises to callers and deployments.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.exceptions.BackPressureError
   serve.exceptions.RayServeException
   serve.exceptions.RequestCancelledError
   serve.exceptions.gRPCStatusError
   serve.exceptions.DeploymentUnavailableError
   serve.exceptions.ReplicaUnavailableError
```
