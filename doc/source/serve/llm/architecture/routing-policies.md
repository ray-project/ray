# Request routing

Ray Serve LLM provides customizable request routing to optimize request distribution across replicas for different workload patterns. Request routing operates at the **replica selection level**, distinct from ingress-level model routing.

## Routing vs ingress

It's important to distinguish between two levels of routing:

**Ingress routing** (model-level):
- Maps model_id to deployment
- Example: `/v1/chat/completions` with `model="gptoss"` → which deployment?
- Handled by OpenAiIngress

**Request routing** (replica-level):
- Chooses which replica to send request to
- Example: Which replica of the "gptoss" deployment (1, 2, or 3)?
- Handled by Ray Serve's RequestRouter

This document focuses on **request routing** (replica selection).

```
HTTP Request → Ingress (model routing) → Request Router (replica selection) → Server Replica
```

## Request routing architecture

Ray Serve LLM request routing operates at the deployment handle level:

```
┌──────────────┐
│   Ingress    │
│  (Replica 1) │
└──────┬───────┘
       │
       │ handle.remote(request)
       ↓
┌──────────────────┐
│ Deployment Handle│
│   + Router       │  ← Request routing happens here
└──────┬───────────┘
       │
       │ Chooses replica based on policy
       ↓
   ┌───┴────┬────────┬────────┐
   │        │        │        │
┌──▼──┐  ┌──▼──┐  ┌──▼──┐  ┌──▼──┐
│ LLM │  │ LLM │  │ LLM │  │ LLM │
│  1  │  │  2  │  │  3  │  │  4  │
└─────┘  └─────┘  └─────┘  └─────┘
```

## Available routing policies

Ray Serve LLM provides multiple request routing policies to optimize for different workload patterns:

### Default routing: Power of Two Choices

The default router uses the Power of Two Choices algorithm:
1. Randomly sample two replicas
2. Route to the replica with fewer ongoing requests

This provides good load balancing with minimal coordination overhead.

### Prefix-aware routing

The `PrefixCacheAffinityRouter` optimizes for workloads with shared prefixes by routing requests with similar prefixes to the same replicas. This improves KV cache hit rates in vLLM's Automatic Prefix Caching (APC).

The routing strategy:
1. **Check load balance**: If replicas are balanced (queue difference < threshold), use prefix matching
2. **High match rate (≥10%)**: Route to replicas with highest prefix match
3. **Low match rate (<10%)**: Route to replicas with lowest cache utilization
4. **Fallback**: Use Power of Two Choices when load is imbalanced

For more details, see {ref}`prefix-aware-routing-guide`.

## Design patterns for custom routing policies

Request routing policies can often follow two design patterns in Ray serve:

### Pattern 1: Centralized metric store

Uses a singleton actor to store metric-to-replica mappings:

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Ingress │────►│         │◄────│ Ingress │
│    1    │     │  Store  │     │    2    │
└────┬────┘     └─────────┘     └────┬────┘
     │                               │
     └────────────────┬──────────────┘
                      │
           ┌──────────┴──────────┐
           │                     │
      ┌────▼────┐           ┌────▼────┐
      │  LLM    │           │  LLM    │
      │ Server  │           │ Server  │
      └─────────┘           └─────────┘
```


```{figure} ../images/request_router_1.png
---
width: 600px
name: session_store_request_router
---
Session-aware routing with centralized metric store
```


Pros: Simple implementation, immediate consistency  
Cons: Potential bottleneck at high scale

### Pattern 2: Broadcast metrics

Uses Ray Serve controller to broadcast metrics:

```
          ┌──────────────┐
          │    Serve     │
          │  Controller  │
          └──────┬───────┘
                 │ (broadcast)
       ┌─────────┴─────────┐
       │                   │
  ┌────▼────┐         ┌────▼────┐
  │ Ingress │         │ Ingress │
  │  +Cache │         │  +Cache │
  └────┬────┘         └────┬────┘
       │                   │
       └────────┬──────────┘
                │
         ┌──────┴──────┐
         │             │
    ┌────▼────┐   ┌────▼────┐
    │  LLM    │   │  LLM    │
    │ Server  │   │ Server  │
    └─────────┘   └─────────┘
```


```{figure} ../images/request_router_2.png
---
width: 600px
name: broadcast_request_router
---
Session-aware routing with broadcast metrics
```

Pros: Highly scalable, distributed routing  
Cons: Eventual consistency, stale session data possible

## Custom routing policies

You can implement custom routing policies by extending Ray Serve's [`RequestRouter`](../../api/doc/ray.serve.request_router.RequestRouter.rst) base class. For detailed examples and step-by-step guides on implementing custom routers, see the {ref}`custom-request-router-guide`.

Key methods to implement:
- [`choose_replicas()`](../../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst): Select which replicas should handle a request
- [`on_request_routed()`](../../api/doc/ray.serve.request_router.RequestRouter.on_request_routed.rst): Update router state after a request is routed
- [`on_replica_actor_died()`](../../api/doc/ray.serve.request_router.RequestRouter.on_replica_actor_died.rst): Clean up state when a replica dies

### Utility mixins

Ray Serve provides mixin classes that add common functionality to routers. See the {ref}`custom-request-router-guide` for examples:

- [`LocalityMixin`](../../api/doc/ray.serve.request_router.LocalityMixin.rst): Prefers replicas on the same node to reduce network latency
- [`MultiplexMixin`](../../api/doc/ray.serve.request_router.MultiplexMixin.rst): Tracks which models are loaded on each replica for LoRA deployments
- [`FIFOMixin`](../../api/doc/ray.serve.request_router.FIFOMixin.rst): Ensures FIFO ordering of requests



### Router lifecycle

1. **Initialization**: Router created with list of replicas
2. **Request routing**: `choose_replicas()` called for each request
3. **Callback**: `on_request_routed()` called after successful routing
4. **Replica failure**: `on_replica_actor_died()` called when replica dies
5. **Cleanup**: Router cleaned up when deployment is deleted

#### Async operations

Routers should use async operations for best performance:

```python
# Good: Async operation
async def choose_replicas(self, ...):
    state = await self.state_actor.get.remote()
    return self._select(state)

# Bad: Blocking operation
async def choose_replicas(self, ...):
    state = ray.get(self.state_actor.get.remote())  # Blocks!
    return self._select(state)
```

#### State management

For routers with state, use appropriate synchronization:

```python
class StatefulRouter(RequestRouter):
    def __init__(self):
        self.lock = asyncio.Lock()  # For async code
        self.state = {}
    
    async def choose_replicas(self, ...):
        async with self.lock:  # Protect shared state
            # Update state
            self.state[...] = ...
            return [...]
```

## See also

- {ref}`prefix-aware-routing-guide` - User guide for deploying prefix-aware routing
- {ref}`custom-request-router-guide` - Ray Serve guide for implementing custom routers
- [`RequestRouter` API Reference](../../api/doc/ray.serve.request_router.RequestRouter.rst) - Complete API documentation

