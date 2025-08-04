(prefix-aware-request-router-guide)=
# PrefixAwarePow2RequestRouter for LLM Inference Optimization

:::{warning}
This API is in alpha and may change before becoming stable.
:::

Large Language Model (LLM) inference can benefit significantly from cache locality optimization. When similar or related prompts are processed by the same replica, the model can reuse previously computed KV-cache entries, reducing computation overhead and improving response times. The [`PrefixAwarePow2RequestRouter`](../api/doc/ray.llm._internal.serve.request_router.prefix_aware.prefix_aware_router.PrefixAwarePow2ReplicaRouter.rst) is designed specifically for this use case.

This advanced request router extends the Power of Two Choices algorithm with prefix-matching capabilities to optimize replica selection for LLM workloads. It intelligently routes requests based on input text prefixes while maintaining load balance across replicas.

This guide covers:
- Understanding the prefix cache-aware routing algorithm
- Configuration parameters and their impact
- Deployment examples for LLM applications
- Performance optimization strategies
- Monitoring and debugging prefix cache effectiveness

(prefix-aware-algorithm)=
## How Prefix Cache-Aware Routing Works

The `PrefixAwarePow2RequestRouter` implements a three tier routing strategy that balances cache locality with load distribution:

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

(prefix-aware-configuration)=
## Configuration Parameters

The `PrefixAwarePow2RequestRouter` provides several configuration parameters to tune its behavior:

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
import ray
from ray import serve
from ray.llm._internal.serve.request_router.prefix_aware.prefix_aware_router import (
    PrefixAwarePow2ReplicaRouter
)

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        request_router_config=dict(
            request_router_class=PrefixAwarePow2ReplicaRouter,
            request_router_kwargs=
        ),
        # Configure routing behavior
        request_router_kwargs={
            "imbalanced_threshold": 5,  # More aggressive load balancing
            "match_rate_threshold": 0.15,  # Require 15% match rate
            "do_eviction": True,  # Enable memory management
            "eviction_threshold_chars": 500_000,
            "eviction_target_chars": 400_000,
            "eviction_interval_secs": 30,
        }
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```

(optimizing-prefix-performance)=
## Performance Optimization Strategies

### 1. Tuning Load Balance Threshold

For workloads with high cache locality potential:
```python
# More aggressive prefix cache routing (higher cache hit rate, potential load imbalance)
request_router_kwargs={
    "imbalanced_threshold": 15,  # Allow more queue length variation
    "match_rate_threshold": 0.05,  # Lower match requirement
}
```

For load-balancing prioritized workloads:
```python
# Prioritize load balancing (more predictable latency, reduced cache hits)
request_router_kwargs={
    "imbalanced_threshold": 3,  # Strict load balancing
    "match_rate_threshold": 0.2,  # Higher match requirement
}
```

### 2. Memory Management for Long-Running Services

For production deployments processing large volumes:
```python
request_router_kwargs={
    "do_eviction": True,
    "eviction_threshold_chars": 1_000_000,  # 1M character threshold
    "eviction_target_chars": 800_000,  # Reduce to 800K
    "eviction_interval_secs": 60,  # Check every minute
}
```

### 3. Replica Scaling Considerations

The prefix cache-aware router works best with sufficient replicas (4+ recommended) to allow effective prefix grouping across different request patterns.

(monitoring-prefix-routing)=
## Monitoring and Debugging

### Understanding Routing Decisions

The router makes routing decisions based on three key metrics you can monitor:

1. **Queue Length Imbalance**: Monitor the difference between max and min queue lengths across replicas
2. **Prefix Match Rate**: Track the percentage of input text that matches existing prefixes
3. **Cache Hit Rate**: Measure KV-cache effectiveness at the model level

### Debugging Common Issues

**Low Cache Hit Rates:**
- Increase `imbalanced_threshold` to allow more prefix cache-based routing
- Decrease `match_rate_threshold` to accept lower match rates
- Check if input prompts have sufficient commonality

**Load Imbalance:**
- Decrease `imbalanced_threshold` for stricter load balancing
- Monitor replica queue lengths and processing times
- Consider adjusting the number of replicas

**Memory Growth:**
- Enable eviction with appropriate thresholds
- Monitor prefix tree size through the tree actor
- Adjust eviction parameters based on workload patterns

(prefix-aware-best-practices)=
## Best Practices

### 1. Gradual Parameter Tuning
Start with aggressive prefix routing to verify effectiveness, then tune for balance:

```python
# Phase 1: Aggressive settings to verify prefix routing works
request_router_kwargs={
    "imbalanced_threshold": 20,  # Allow significant load imbalance
    "match_rate_threshold": 0.05,  # Accept weak prefix matches
    "do_eviction": False,  # Disable initially for testing
}

# Phase 2: Tune for your workload's balance of cache hits vs load distribution
# Monitor cache hit rates and queue length variations, then adjust thresholds
```

### 2. Testing and Validation
Test the router's effectiveness with your specific workload patterns:

```python
# Test with similar prompts to verify cache locality
test_prompts = [
    "Translate the following English text to French: Hello",
    "Translate the following English text to French: Goodbye", 
    "Translate the following English text to Spanish: Hello",
]
```

The `PrefixAwarePow2RequestRouter` provides a powerful way to optimize LLM inference by intelligently routing related requests to the same replicas, improving cache locality while maintaining load balance. Proper configuration and monitoring are key to achieving optimal performance for your specific use case.