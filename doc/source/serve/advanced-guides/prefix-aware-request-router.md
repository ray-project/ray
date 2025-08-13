(prefix-aware-request-router-guide)=
# `PrefixCacheAffinityRouter` for LLM Inference Optimization

:::{warning}
This API is in alpha and may change before becoming stable.
:::

LLM inference can benefit significantly from cache locality optimization. When prompts that share a prefix are processed by the same replica, the engine can reuse previously computed KV-cache entries, reducing computation overhead and improving response times. This technique is known as [Automatic Prefix Caching (APC)](https://docs.vllm.ai/en/stable/features/automatic_prefix_caching.html) in vLLM. The `PrefixCacheAffinityRouter` is designed specifically for this use case.

This guide covers:
- Understanding the prefix cache-aware routing algorithm
- Building the components of a prefix-aware router
- Configuration parameters and their impact

(prefix-aware-algorithm)=
## How Ray Serve LLM Prefix Cache-Aware Routing Works

The `PrefixCacheAffinityRouter` implements a multi-tier routing strategy that balances cache locality with load distribution:

### 1. Load Balance Check
First, it evaluates whether the current load is balanced across replicas by comparing queue lengths. If the difference between the highest and lowest queue lengths is below the `imbalanced_threshold`, it proceeds with prefix cache-aware routing.

### 2. Prefix Matching Strategy
When load is balanced, the router uses a prefix tree to find replicas that have previously processed similar input text:

- **High Match Rate (≥10%)**: Routes to replicas with the highest prefix match rate for better cache hit rates
- **Low Match Rate (<10%)**: Falls back to replicas with the lowest prefix cache utilization to increase utilization
- **No Prefix Data**: Uses the default Power of Two Choices selection

### 3. Imbalanced Load Fallback
When load is imbalanced (queue length difference exceeds threshold), the router prioritizes load balancing over cache locality and falls back to the standard Power of Two Choices algorithm.

### Prefix Tree Management
The router maintains a distributed prefix tree actor that:
- Tracks input text prefixes processed by each replica
- Supports automatic eviction of old entries to manage memory usage
- Persists across router instances using Ray's detached actor pattern

(building-prefix-aware-components)=
## Building Prefix-Aware Router Components

This section breaks down the key components of `PrefixCacheAffinityRouter` and shows how they work together. For a more basic example, see [custom request router guide](custom-request-router-guide).

### Base RequestRouter Foundation

Like all custom routers in Ray Serve, the `PrefixCacheAffinityRouter` extends the base [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst) class. The two core methods that define router behavior are:

- **`choose_replicas()`**: The main routing logic that selects which replicas should handle a request
- **`on_request_routed()`**: A callback that updates router state after a request is successfully routed

For a detailed explanation of these methods and their parameters, see the [simple uniform request router](simple-uniform-request-router) example in the custom request router guide.

### 1. Load Balance Detection Component

The first component evaluates whether the current load is balanced across replicas:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: __begin_load_balance_component__
:end-before: __end_load_balance_component__
:language: python
```

This component prioritizes load balancing over cache locality when replicas become too imbalanced.


### 2. Prefix Tree Management Component

The prefix tree component is implemented as a detached Ray actor that manages prefix tracking across the Serve application. The actual tree structure uses a multi-tenant prefix tree (approximate radix tree).

This distributed architecture allows the prefix information to persist across router restarts and be shared among multiple router instances.

### 3. Prefix Matching Logic Component

The core prefix matching component implements the routing decision logic in the `_prefix_match_best_replicas` method. When load is balanced, it performs prefix matching to find the best replica:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: __begin_prefix_match_component__
:end-before: __end_prefix_match_component__
:language: python
:linenos:
```

This logic implements the three-tier strategy:
1. **High match rate**: Routes to replicas with the highest prefix match when `match_rate >= match_rate_threshold`
2. **Low match rate**: Falls back to replicas with smallest KV-cache usage when match rate is below threshold
3. **No match**: Fall back to default Power of Two Choices selection when `_prefix_match_best_replicas` returns to `choose_replicas`.

### 4. Integration with Power of Two Choices

The prefix-aware router extends the proven Power of Two Choices algorithm, falling back to it when prefix-based routing would degenerate. `PrefixCacheAffinityRouter` integrates this component in the `choose_replicas` method:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: __begin_pow2_router_base__
:end-before: __end_pow2_router_base__
:language: python
```


### 5. State Management and Callbacks

The router uses the `on_request_routed()` callback to update the prefix tree with routing decisions:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: __begin_on_request_routed__
:end-before: __end_on_request_routed__
:language: python
:linenos:
```

When a replica dies, the `on_replica_actor_died` callback is used to remove its entries from the shared prefix tree:
```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: __begin_on_replica_actor_died__
:end-before: __end_on_replica_actor_died__
:language: python
:linenos:
```

(mixin-components)=
## Mixin Components

The `PrefixCacheAffinityRouter` inherits from two mixins. For more details about these and other available mixins, see [utility mixins for request routing](utility-mixins-for-request-routing). These mixins are used to optimize the list of candidate replicas against which to calculate prefix cache hit rate.

The [`LocalityMixin`](../api/doc/ray.serve.request_router.LocalityMixin.rst) provides locality-aware routing to optimize network latency by preferring replicas on the same node. The [`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst) enables model multiplexing support by tracking which models are loaded on each replica and routing requests to replicas that already have the requested model in memory.

## Configuration Parameters

The `PrefixCacheAffinityRouter` provides several configuration parameters to tune its behavior:

### Core Routing Parameters

- **`imbalanced_threshold`** (default: 10): Queue length difference threshold for considering load balanced. Lower values prioritize load balancing over cache locality.

- **`match_rate_threshold`** (default: 0.1): Minimum prefix match rate (0.0-1.0) required to use prefix cache-aware routing. Higher values require stronger prefix matches before routing for cache locality.

### Memory Management Parameters

- **`do_eviction`** (default: False): Enable automatic eviction of old prefix tree entries to approximate the LLM engine's eviction policy.

- **`eviction_threshold_chars`** (default: 400,000): Maximum number of characters in the prefix tree before eviction is triggered.

- **`eviction_target_chars`** (default: 360,000): Target number of characters to reduce the prefix tree to during eviction.

- **`eviction_interval_secs`** (default: 10): Interval in seconds between eviction checks when eviction is enabled.

(deploy-llm-with-prefix-aware-router)=
## Deploying LLM Applications with Prefix Cache-Aware Routing

Here's how to deploy an LLM application using the prefix cache-aware request router:

```{literalinclude} ../../python/ray/serve/doc_code/prefix_aware_request_router.py
:start-after: __begin_prefix_aware_deployment__
:end-before: __end_prefix_aware_deployment__
:language: python
```
