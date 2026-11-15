---
myst:
  html_meta:
    description: "API reference for Ray Serve runtime context: replica, gang, trace, and multiplexing context from ray.serve and ray.serve.context, and the gRPC helpers in ray.serve.grpc_util."
---

(serve-api-context)=

# Runtime context

These APIs read context from inside a running replica, such as the replica's identity, the current trace, or the multiplexed model ID of the current request.

```{eval-rst}
.. currentmodule:: ray
```

## Replica and request context

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.get_replica_context
   serve.get_trace_context
   serve.get_deployment_actor
   serve.get_multiplexed_model_id
   serve.context.ReplicaContext
   serve.context.GangContext
```

## gRPC context

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.grpc_util.RayServegRPCContext
   serve.grpc_util.gRPCInputStream
```
