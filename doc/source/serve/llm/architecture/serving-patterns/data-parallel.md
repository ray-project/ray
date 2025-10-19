(serve-llm-architecture-data-parallel)=
# Data parallelism

Data parallelism (DP) is a serving pattern that creates multiple inference engine instances to process requests in parallel. This pattern is most useful when you combine it with expert parallelism for sparse MoE models. In this case, the experts are parallelized across multiple machines and attention (QKV) layers are replicated across GPUs, providing an opportunity to shard across requests. 

In this serving pattern, engine replicas aren't isolated. In fact, they need to run in sync with each other to serve a large number of requests concurrently. 

## Architecture overview

```{figure} ../../images/dp.png
---
width: 700px
name: dp-architecture
---
Data parallel architecture showing DPRankAssigner coordinating multiple LLMServer replicas.
```

In data parallel serving:

- The system creates `dp_size` replicas of the LLM server.
- Each replica runs an independent inference engine with the same model.
- Requests are distributed across replicas through Ray Serve's routing.
- All replicas work together as a cohesive unit.


### When to use DP

Data parallel serving works best when:

- **Large sparse MoE with MLA**: Allows reaching larger batch sizes by utilizing the sparsity of the experts more efficiently. MLA (Multi-head Latent Attention) reduces KV cache memory requirements. 
- **High throughput required**: You need to serve many concurrent requests.
- **KV-cache limited**: Adding more KV cache capacity increases throughput, so that parallelization of experts could effectively increase the capacity of KV-cache for handling concurrent requests.

### When not to use DP

Consider alternatives when:

- **Low to medium throughput**: If you can't saturate the MoE layers, don't use DP. 
- **Non-MLA Attention**: DP is beneficial with MLA. Without DP (using TP instead), you need to replicate the KV cache, which isn't beneficial because you want to maximize batch size. As long as the KV cache can be sharded, using TP might be sufficient. 
- **Non-MoE Models**: The main reason for using DP at the cost of this complexity is to lift the effective batch size during decoding for saturating the experts. 

## Components

The following are the main components of DP deployments: 

### DPServer

`DPServer` extends `LLMServer` with data parallel coordination. The following pseudocode shows the structure:

```python
class DPServer(LLMServer):
    """LLM server with data parallel coordination."""
    
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
        llm_config.rank = await self.rank_assigner.assign_rank.remote()
        
        # Call parent initialization
        await super().__init__(llm_config, **kwargs)
```

Key responsibilities:

- Register with the rank assigner coordinator.
- Obtain a unique rank (0 to `dp_size-1`).
- Coordinate with other replicas for collective operations.
- Handle replica failures and re-registration.

### DPRankAssigner

`DPRankAssigner` is a singleton coordinator that manages rank assignment. The following pseudocode shows the structure:

```python
class DPRankAssigner:
    """Coordinator for data parallel rank assignment."""
    
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
Data parallel request flow from client to distributed replicas.
```

The following is the request flow through a data parallel deployment:

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

Data parallel deployments scale in units of `dp_size`:

- **Minimum replicas**: Must be a multiple of `dp_size` (fixed to `dp_size` as only static scaling is supported).
- **Maximum replicas**: Must be a multiple of `dp_size` (fixed to `dp_size` as only static scaling is supported).


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

You can run data parallelism on both prefill and decode phases:

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

