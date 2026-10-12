---
myst:
  html_meta:
    description: "API reference for ray.serve.request_router, the extension points for writing a custom Ray Serve request router."
---

(serve-api-request-router)=

# Request router

The `ray.serve.request_router` module holds the base class, mixins, and data types for writing a custom request router.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.request_router.ReplicaID
   serve.request_router.PendingRequest
   serve.request_router.RunningReplica
   serve.request_router.FIFOMixin
   serve.request_router.LocalityMixin
   serve.request_router.MultiplexMixin
   serve.request_router.RequestRouter
```
