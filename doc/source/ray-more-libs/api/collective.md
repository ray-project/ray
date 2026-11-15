---
myst:
  html_meta:
    description: "API reference for Ray Collective: collective group management, collective and point-to-point communication, and backend registration."
---

(ray-collective-api-ref)=

# Ray Collective API

For usage, see {doc}`/ray-more-libs/ray-collective`.

## Collective groups

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.collective.collective.init_collective_group
   ray.util.collective.collective.create_collective_group
   ray.util.collective.collective.destroy_collective_group
   ray.util.collective.collective.is_group_initialized
   ray.util.collective.collective.get_rank
   ray.util.collective.collective.get_collective_group_size
   ray.util.collective.collective.get_group_handle
   ray.util.collective.collective.GroupManager
```

## Collective communication

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.collective.collective.allreduce
   ray.util.collective.collective.allreduce_multigpu
   ray.util.collective.collective.barrier
   ray.util.collective.collective.reduce
   ray.util.collective.collective.reduce_multigpu
   ray.util.collective.collective.broadcast
   ray.util.collective.collective.broadcast_multigpu
   ray.util.collective.collective.allgather
   ray.util.collective.collective.allgather_multigpu
   ray.util.collective.collective.reducescatter
   ray.util.collective.collective.reducescatter_multigpu
```

## Point-to-point communication

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.collective.collective.send
   ray.util.collective.collective.send_multigpu
   ray.util.collective.collective.recv
   ray.util.collective.collective.recv_multigpu
```

## Backends

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.collective.collective.is_backend_available
   ray.util.collective.backend_registry.register_collective_backend
```

## Utilities

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.collective.collective.get_address_and_port
   ray.util.collective.collective.synchronize
```
