(prefix-aware-request-router-guide)=
# `PrefixCacheAffinityRouter` for LLM Inference Optimization

:::{warning}
This API is in alpha and may change before becoming stable.
:::

Large Language Model (LLM) inference can benefit significantly from cache locality optimization. When similar or related prompts are processed by the same replica, the engine can reuse previously computed KV-cache entries, reducing computation overhead and improving response times. This technique is known as [Automatic Prefix Caching (APC)](https://docs.vllm.ai/en/stable/features/automatic_prefix_caching.html) in vLLM. The [`PrefixCacheAffinityRouter`](../api/doc/ray.llm._internal.serve.request_router.prefix_aware.prefix_aware_router.PrefixAwarePow2ReplicaRouter.rst) is designed specifically for this use case.

This advanced request router extends the Power of Two Choices algorithm with prefix-matching capabilities to optimize replica selection for LLM workloads. It intelligently routes requests based on input text prefixes while maintaining load balance across replicas.

This guide covers:
- Understanding the prefix cache-aware routing algorithm
- Building the components of a prefix-aware router
- Configuration parameters and their impact

(prefix-aware-algorithm)=
## How Ray Serve LLM Prefix Cache-Aware Routing Works

The `PrefixCacheAffinityRouter` implements a three-tier routing strategy that balances cache locality with load distribution:

### 1. Load Balance Check
First, it evaluates whether the current load is balanced across replicas by comparing queue lengths. If the difference between the highest and lowest queue lengths is below the `imbalanced_threshold`, it proceeds with prefix cache-aware routing.

### 2. Prefix Matching Strategy
When load is balanced, the router uses a prefix tree to find replicas that have previously processed similar input text:

- **High Match Rate (≥10%)**: Routes to replicas with the highest prefix match rate for better cache hit rates
- **Low Match Rate (<10%)**: Falls back to replicas with the smallest KV-cache usage to optimize memory utilization
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

Understanding how to build the components of a prefix-aware router helps you customize or extend the `PrefixCacheAffinityRouter` for your specific use cases. This section breaks down the key components and shows how they work together, referencing patterns from the [custom request router guide](custom-request-router-guide).

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

The actual implementation in `PrefixCacheAffinityRouter` uses this logic within the `_prefix_match_best_replicas` method:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: # Check for imbalanced load.
:end-before: is_imbalanced = (
:language: python
:linenos:
```

This component prioritizes load balancing over cache locality when replicas become too imbalanced, similar to how the [throughput-aware request router](throughput-aware-request-router) uses replica statistics to make routing decisions.

### 2. Prefix Tree Management Component

The prefix tree component is implemented as a separate Ray actor that manages prefix tracking across the distributed system. The actual tree structure uses a multi-tenant prefix tree (approximate radix tree):

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_tree.py
:start-after: class Node:
:end-before: def __init__(self, text: str = "", parent: Optional[Node] = None) -> None:
:language: python
:linenos:
```

The PrefixTreeActor provides key methods for prefix matching:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_tree.py
:start-after: def prefix_match(
:end-before: """
:language: python
:linenos:
```

This distributed architecture allows the prefix information to persist across router restarts and be shared among multiple router instances, similar to how the [utility mixins](utility-mixin) in custom routers manage shared state.

### 3. Prefix Matching Logic Component

The core prefix matching component implements the routing decision logic in the `_prefix_match_best_replicas` method. When load is balanced, it performs prefix matching to find the best replica:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: if not is_imbalanced:
:end-before: return [
:language: python
:linenos:
```

This logic implements the three-tier strategy:
1. **High match rate**: Routes to replicas with the highest prefix match when `match_rate >= match_rate_threshold`
2. **Low match rate**: Falls back to replicas with smallest KV-cache usage when match rate is below threshold
3. **No match**: Uses default Power of Two Choices selection

### 4. Integration with Power of Two Choices

The prefix-aware router extends the proven Power of Two Choices algorithm, falling back to it when prefix-based routing isn't advantageous. Here's an educational example showing the integration pattern:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: __begin_router_foundation__
:end-before: __end_router_foundation__
:language: python
```

The actual implementation in `PrefixCacheAffinityRouter` integrates these components in the `choose_replicas` method:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: # Get fallback replicas from PowerOfTwoChoicesRequestRouter
:end-before: return fallback_replicas
:language: python
:linenos:
```

### 5. State Management and Callbacks

The router uses the `on_request_routed()` callback to update the prefix tree with routing decisions:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: def on_request_routed(
:end-before: ray.get(
:language: python
:linenos:
```

The actual prefix tree update happens asynchronously:

```{literalinclude} ../../python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_aware_router.py
:start-after: # Insert into prefix tree
:end-before: )
:language: python
:linenos:
```

This pattern of using the callback for state updates is similar to the approach shown in the [uniform request router example](simple-uniform-request-router).

### Component Integration Patterns

When building custom prefix-aware routers, consider these integration patterns:

1. **Layered Decision Making**: Like the [throughput-aware router](throughput-aware-request-router), combine multiple routing factors (load balance, prefix matching, resource utilization) in a hierarchical decision process.

2. **Mixin Compatibility**: The prefix-aware router can potentially be combined with [`LocalityMixin`](../api/doc/ray.serve.request_router.LocalityMixin.rst) or [`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst) for additional routing intelligence.

3. **Graceful Degradation**: Always provide fallback mechanisms when specialized routing conditions aren't met, ensuring robust operation under all circumstances.

(prefix-aware-configuration)=
## Configuration Parameters

The `PrefixCacheAffinityRouter` provides several configuration parameters to tune its behavior:

### Core Routing Parameters

- **`imbalanced_threshold`** (default: 10): Queue length difference threshold for considering load balanced. Lower values prioritize load balancing over cache locality.

- **`match_rate_threshold`** (default: 0.1): Minimum prefix match rate (0.0-1.0) required to use prefix cache-aware routing. Higher values require stronger prefix matches before routing for cache locality.

### Memory Management Parameters

- **`do_eviction`** (default: False): Enable automatic eviction of old prefix tree entries to prevent memory growth.

- **`eviction_threshold_chars`** (default: 400,000): Maximum number of characters in the prefix tree before eviction is triggered.

- **`eviction_target_chars`** (default: 360,000): Target number of characters to reduce the prefix tree to during eviction.

- **`eviction_interval_secs`** (default: 10): Interval in seconds between eviction checks when eviction is enabled.

(deploy-llm-with-prefix-aware-router)=
## Deploying LLM Applications with Prefix Cache-Aware Routing

Here's how to deploy an LLM application using the prefix cache-aware request router:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.request_router import PrefixCacheAffinityRouter

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=4,
            max_replicas=4
        ),
        request_router_config=dict(
            request_router_class=PrefixCacheAffinityRouter,
            request_router_kwargs={
                "imbalanced_threshold": 5,  # More aggressive load balancing
                "match_rate_threshold": 0.15,  # Require 15% match rate
                "do_eviction": True,  # Enable memory management
                "eviction_threshold_chars": 500_000,
                "eviction_target_chars": 400_000,
                "eviction_interval_secs": 30,
            }
        ),
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```
