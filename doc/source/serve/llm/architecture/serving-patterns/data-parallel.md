(serve-llm-architecture-data-parallel)=
# Data parallel attention

Data parallel attention (DP) is a serving pattern that creates multiple inference engine instances to process requests in parallel. This pattern is most useful when you combine it with expert parallelism for sparse MoE models. In this case, the experts are parallelized across multiple machines and attention (QKV) layers are replicated across GPUs, providing an opportunity to shard across requests. 

In this serving pattern, engine replicas aren't isolated. In fact, they need to run in sync with each other to serve a large number of requests concurrently. 

## Architecture overview

```{figure} ../../images/dp.png
---
width: 700px
name: dp-architecture
---
Data parallel attention architecture showing DPRankAssigner coordinating multiple LLMServer replicas.
```

In data parallel attention serving:

- The system creates `dp_size` replicas of the LLM server.
- Each replica runs an independent inference engine with the same model.
- Requests are distributed across replicas through Ray Serve's routing.
- All replicas work together as a cohesive unit.


### When to use DP

Data parallel attention serving works best when:

- **Large sparse MoE with MLA**: Allows reaching larger batch sizes by utilizing the sparsity of the experts more efficiently. MLA (Multi-head Latent Attention) reduces KV cache memory requirements. 
- **High throughput required**: You need to serve many concurrent requests.
- **KV-cache limited**: Adding more KV cache capacity increases throughput, so that parallelization of experts could effectively increase the capacity of KV-cache for handling concurrent requests.

### When not to use DP

Consider alternatives when:

- **Low to medium throughput**: If you can't saturate the MoE layers, don't use DP. 
- **Non-MLA Attention with sufficient TP**: DP is most beneficial with MLA (Multi-head Latent Attention), where KV cache can't be sharded along the head dimension. For models with GQA (Grouped Query Attention), you can use TP to shard the KV cache up to the degree where `TP_size <= num_kv_heads`. Beyond that point, TP requires KV cache replication, which wastes memory—DP becomes a better choice to avoid duplication. For example, for Qwen-235b, using `DP=2, TP=4, EP=8` makes more sense than `DP=8, EP=8` because you can still shard the KV cache with TP=4 before needing to replicate it. Benchmark these configurations with your workload to determine the optimal setup.
- **Non-MoE models**: The main reason for using DP at the cost of this complexity is to lift the effective batch size during decoding for saturating the experts. 

## Components

The following are the main components of DP deployments: 

### DPServer

`DPServer` extends `LLMServer` with data parallel attention coordination. The following pseudocode shows the structure:

```python
from ray import serve

class DPServer(LLMServer):
    """LLM server with data parallel attention coordination."""
    
    async def __init__(
        self,
        llm_config: LLMConfig,
        rank_assigner_handle: DeploymentHandle,
        dp_size: int,
        **kwargs
    ):
        self.rank_assigner = rank_assigner_handle
        self.dp_size = dp_size
        
        # Get assigned rank from coordinator and pass it to engine.
        replica_id = serve.get_replica_context().replica_id
        llm_config.rank = await self.rank_assigner.assign_rank.remote(replica_id)
        
        # Call parent initialization
        await super().__init__(llm_config, **kwargs)
```

Key responsibilities:

- Register with the rank assigner coordinator.
- Obtain a unique rank (0 to `dp_size-1`).
- Coordinate with other replicas for collective operations.
- Handle replica failures and re-registration.

### DPRankAssigner

`DPRankAssigner` is a singleton coordinator that manages rank assignment for data parallel attention replicas. The following pseudocode shows the structure:

```python
class DPRankAssigner:
    """Coordinator for data parallel attention rank assignment."""
    
    def __init__(self, dp_size: int):
        self.dp_size = dp_size
        self.assigned_ranks: Set[int] = set()
        self.rank_to_replica: Dict[int, str] = {}
        self.lock = asyncio.Lock()
    
    async def assign_rank(self, replica_id: str) -> int:
        """Assign a rank to a replica.
        
        Returns:
            int: Assigned rank (0 to dp_size-1)
        """
        async with self.lock:
            # Find first available rank
            for rank in range(self.dp_size):
                if rank not in self.assigned_ranks:
                    self.assigned_ranks.add(rank)
                    self.rank_to_replica[rank] = replica_id
                    return rank
    
    async def release_rank(self, rank: int):
        """Release a rank when replica dies."""
        async with self.lock:
            self.assigned_ranks.discard(rank)
            self.rank_to_replica.pop(rank, None)
```

Key responsibilities:

- Assign unique ranks to replicas.
- Ensure exactly `dp_size` replicas are serving.

## Request flow

```{figure} ../../images/dp_flow.png
---
width: 700px
name: dp-flow
---
Data parallel attention request flow from client to distributed replicas.
```

The following is the request flow through a data parallel attention deployment:

1. **Client request**: HTTP request arrives at ingress.
2. **Ingress routing**: Ingress uses deployment handle to call DPServer.
3. **Ray Serve routing**: Ray Serve's request router selects a replica.
   - Default: Power of Two Choices (load balancing).
   - Custom: Prefix-aware, session-aware, etc.
4. **Replica processing**: Selected DPServer replica processes request.
5. **Engine inference**: vLLM engine generates response.
6. **Streaming response**: Tokens stream back to client.

The key difference from basic serving is that all the `dp_size` replicas are working in coordination with each other rather than in isolation.

## Scaling

### Scaling behavior

Data parallel attention deployments require a fixed number of replicas equal to `dp_size`, as autoscaling isn't supported for this pattern. You must set `num_replicas` to `dp_size`, or if using `autoscaling_config`, both `min_replicas` and `max_replicas` must equal `dp_size`.


## Design considerations

### Coordination overhead

The `DPRankAssigner` introduces minimal coordination overhead:

- **Startup**: Each replica makes one RPC to get its rank.
- **Runtime**: No coordination overhead during request processing.

The singleton actor pattern ensures consistency during startup time.

### Placement strategy

The PACK strategy places each replica's resources together:

- Tensor parallel workers for one replica pack on the same node when possible.
- Different replicas can be on different nodes.
- This minimizes inter-node communication within each replica.

## Combining with other patterns

### DP + Prefill-decode disaggregation

You can run data parallel attention on both prefill and decode phases:

```
┌─────────────────────────────────────────────┐
│              OpenAiIngress                  │
└─────────────┬───────────────────────────────┘
              │
              ▼
        ┌─────────────┐
        │PDProxyServer│
        └──┬───────┬──┘
           │       │
     ┌─────┘       └─────┐
     ▼                   ▼
┌──────────┐        ┌──────────┐
│ Prefill  │        │  Decode  │
│   DP-2   │        │   DP-4   │
│          │        │          │
│ Replica0 │        │ Replica0 │
│ Replica1 │        │ Replica1 │
└──────────┘        │ Replica2 │
                    │ Replica3 │
                    └──────────┘
```

## See also

- {doc}`../overview` - High-level architecture overview
- {doc}`../core` - Core components and protocols
- {doc}`prefill-decode` - Prefill-decode disaggregation architecture
- {doc}`../routing-policies` - Request routing architecture

