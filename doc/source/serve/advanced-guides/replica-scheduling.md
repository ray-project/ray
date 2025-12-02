(serve-replica-scheduling)=

# Replica scheduling

This guide explains how Ray Serve schedules deployment replicas across your cluster and the APIs and environment variables you can use to control placement behavior.

## Quick reference: Choosing the right approach

| Goal | Solution | Example |
|------|----------|---------|
| Multi-GPU inference with tensor parallelism | `placement_group_bundles` + `STRICT_PACK` | vLLM with `tensor_parallel_size=4` |
| Target specific GPU types or zones | Custom resources in `ray_actor_options` | Schedule on A100 nodes only |
| Limit replicas per node for HA | `max_replicas_per_node` | Max 2 replicas of each deployment per node |
| Reduce cloud costs by packing nodes | `RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY=1` | Many small models sharing nodes |
| Reserve resources for worker actors | `placement_group_bundles` | Replica spawns Ray Data workers |
| Shard large embeddings across nodes | `placement_group_bundles` + `STRICT_SPREAD` | Recommendation model with distributed embedding table |
| Simple deployment, no special needs | Default (just `ray_actor_options`) | Single-GPU model |

## How replica scheduling works

When you deploy an application, Ray Serve's deployment scheduler determines where to place each replica actor across the available nodes in your Ray cluster. The scheduler runs on the Serve Controller and makes batch scheduling decisions during each update cycle. For information on configuring CPU, GPU, and other resource requirements for your replicas, see [Resource allocation](serve-resource-allocation).

By default, Ray Serve uses a **spread scheduling strategy** that distributes replicas across nodes with best effort. This approach:
- Maximizes fault tolerance by avoiding concentration of replicas on a single node
- Balances load across the cluster
- Helps prevent resource contention between replicas

### Scheduling priority

When scheduling a replica, the scheduler evaluates strategies in the following priority order:

1. **Placement groups**: If you specify `placement_group_bundles`, the scheduler uses a `PlacementGroupSchedulingStrategy` to co-locate the replica with its required resources.
2. **Node affinity**: If the scheduler identifies a target node (for example, during compact scheduling), it uses a `NodeAffinitySchedulingStrategy` with soft constraints.
4. **Default strategy**: Falls back to either `SPREAD` (default) or `DEFAULT` (when compact scheduling is enabled).

### Downscaling behavior

When Ray Serve scales down a deployment, it intelligently selects which replicas to stop:

1. **Non-running replicas first**: Pending, launching, or recovering replicas are stopped before running replicas.
2. **Minimize node count**: Running replicas are stopped from nodes with the fewest total replicas across all deployments, helping to free up nodes faster.
3. **Head node protection**: Replicas on the head node have the lowest priority for removal since the head node can't be released.
4. **Newest first**: Among replicas on the same node, newer replicas are stopped before older ones.

## APIs for controlling replica placement

Ray Serve provides several options to control where replicas are scheduled. These parameters are configured through the [`@serve.deployment`](serve-deployment-api) decorator. For the full API reference, see the [deployment decorator documentation](../api/doc/ray.serve.deployment_decorator.rst).

### Limit replicas per node with `max_replicas_per_node`

Use [`max_replicas_per_node`](../api/doc/ray.serve.deployment_decorator.rst) to cap the number of replicas of a deployment that can run on a single node. This is useful when:
- You want to ensure high availability by spreading replicas across nodes
- You want to avoid resource contention between replicas of the same deployment

```python
from ray import serve

@serve.deployment(num_replicas=6, max_replicas_per_node=2)
class MyDeployment:
    def __call__(self, request):
        return "Hello!"

app = MyDeployment.bind()
```

In this example, if you have 6 replicas and `max_replicas_per_node=2`, Ray Serve requires at least 3 nodes to schedule all replicas.

:::{note}
Valid values for `max_replicas_per_node` are `None` (default, no limit) or an integer. You can't set `max_replicas_per_node` together with `placement_group_bundles`.
:::

You can also specify this in a config file:

```yaml
applications:
  - name: my_app
    import_path: my_module:app
    deployments:
      - name: MyDeployment
        num_replicas: 6
        max_replicas_per_node: 2
```

### Reserve resources with placement groups

For more details on placement group strategies, see the [Ray Core placement groups documentation](ray-placement-group-doc-ref).

A **placement group** is a Ray primitive that reserves a group of resources (called **bundles**) across one or more nodes in your cluster. When you configure [`placement_group_bundles`](../api/doc/ray.serve.deployment_decorator.rst) for a Ray Serve deployment, Ray creates a dedicated placement group for *each replica*, ensuring those resources are reserved and available for that replica's use.

A **bundle** is a dictionary specifying resource requirements, such as `{"CPU": 2, "GPU": 1}`. When you define multiple bundles, you're telling Ray to reserve multiple sets of resources that can be placed according to your chosen strategy.

#### What placement groups and bundles mean

The following diagram illustrates how a deployment with `placement_group_bundles=[{"GPU": 1}, {"GPU": 1}, {"CPU": 4}]` and [`placement_group_strategy`](../api/doc/ray.serve.deployment_decorator.rst)`="STRICT_PACK"` is scheduled:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Node (8 CPUs, 4 GPUs)                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                     Placement Group (per replica)                     │  │
│  │                                                                       │  │
│  │   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐   │  │
│  │   │   Bundle 0      │  │   Bundle 1      │  │     Bundle 2        │   │  │
│  │   │   {"GPU": 1}    │  │   {"GPU": 1}    │  │    {"CPU": 4}       │   │  │
│  │   │                 │  │                 │  │                     │   │  │
│  │   │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────────┐ │   │  │
│  │   │ │   Replica   │ │  │ │   Worker    │ │  │ │  Worker Tasks   │ │   │  │
│  │   │ │   Actor     │ │  │ │   Actor     │ │  │ │  (preprocessing)│ │   │  │
│  │   │ │  (main GPU) │ │  │ │ (2nd GPU)   │ │  │ │                 │ │   │  │
│  │   │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────────┘ │   │  │
│  │   └─────────────────┘  └─────────────────┘  └─────────────────────┘   │  │
│  │           ▲                                                           │  │
│  │           │                                                           │  │
│  │    Replica runs in                                                    │  │
│  │    first bundle                                                       │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘

With STRICT_PACK: All bundles guaranteed on same node
```

Consider a deployment with `placement_group_bundles=[{"GPU": 1}, {"GPU": 1}, {"CPU": 4}]`:

- Ray reserves 3 bundles of resources for each replica
- The replica actor runs in the **first bundle** (so `ray_actor_options` must fit within it)
- The remaining bundles are available for worker actors/tasks spawned by the replica
- All child actors and tasks are automatically scheduled within the placement group

This is different from simply requesting resources in `ray_actor_options`. With `ray_actor_options={"num_gpus": 2}`, your replica actor gets 2 GPUs but you have no control over where additional worker processes run. With placement groups, you explicitly reserve resources for both the replica and its workers.

#### When to use placement groups

Use `placement_group_bundles` when:

| Scenario | Why placement groups help |
|----------|---------------------------|
| **Model parallelism** | Tensor parallelism or pipeline parallelism requires multiple GPUs that must communicate efficiently. Use `STRICT_PACK` to guarantee all GPUs are on the same node. For example, vLLM with `tensor_parallel_size=4` and the Ray distributed executor backend spawns 4 Ray worker actors (one per GPU shard), all of which must be on the same node for efficient inter-GPU communication via NVLink/NVSwitch. |
| **Replica spawns workers** | Your deployment creates Ray actors or tasks for parallel processing. Placement groups reserve resources for these workers. For example, a video processing service that spawns Ray tasks to decode frames in parallel, or a batch inference service using Ray Data to preprocess inputs before model inference. |
| **Heterogeneous resources** | Your replica needs different resource types, such as one GPU bundle and one CPU-only bundle for preprocessing. For example, an LLM service where tokenization and detokenization run on CPU workers while inference runs on the GPU, or an image classification pipeline with CPU-based image decoding/resizing before GPU inference. |
| **Cross-node distribution** | You need bundles spread across different nodes. Use `SPREAD` or `STRICT_SPREAD`. For example, serving a model with a massive embedding table (such as a recommendation model with billions of item embeddings) that must be sharded across multiple nodes because it exceeds single-node memory. Each bundle holds one shard, and `STRICT_SPREAD` ensures each shard is on a separate node. |

Don't use placement groups when:
- Your replica is self-contained and doesn't spawn additional actors/tasks
- You only need simple resource requirements (use `ray_actor_options` instead)
- You want to use `max_replicas_per_node` (the two options are mutually exclusive)

#### Configuring placement groups

The following example reserves 2 GPUs for each replica using a strict pack strategy:

```python
from ray import serve

@serve.deployment(
    ray_actor_options={"num_cpus": 0.1},
    placement_group_bundles=[{"GPU": 1}, {"GPU": 1}],
    placement_group_strategy="STRICT_PACK",
)
class MultiGPUModel:
    def __init__(self):
        # Initialize model shards across both GPUs
        pass

    def __call__(self, request):
        # Use both GPUs for inference
        return "Processed with 2 GPUs"

app = MultiGPUModel.bind()
```

The replica actor is scheduled in the first bundle, so the resources specified in `ray_actor_options` must be a subset of the first bundle's resources. All actors and tasks created by the replica are scheduled in the placement group by default (`placement_group_capture_child_tasks=True`).

### Target nodes with custom resources

You can use custom resources in [`ray_actor_options`](../api/doc/ray.serve.deployment_decorator.rst) to target replicas to specific nodes. This is the recommended approach for controlling which nodes run your replicas.

First, start your Ray nodes with custom resources that identify their capabilities:

```bash
# Start head node with GPU type label
ray start --head --resources='{"A100": 4}'

# Start worker node with different GPU type
ray start --address=<head-address> --resources='{"T4": 2}'
```

Then configure your deployment to require the specific resource:

```python
from ray import serve

# Schedule only on nodes with A100 GPUs
@serve.deployment(ray_actor_options={"resources": {"A100": 1}})
class A100Model:
    def __call__(self, request):
        return "Running on A100"

# Schedule only on nodes with T4 GPUs
@serve.deployment(ray_actor_options={"resources": {"T4": 1}})
class T4Model:
    def __call__(self, request):
        return "Running on T4"

a100_app = A100Model.bind()
t4_app = T4Model.bind()
```

Custom resources offer several advantages for Ray Serve deployments:

- **Quantifiable**: You can request specific amounts (such as `{"A100": 2}` for 2 GPUs or `{"A100": 0.5}` to share a GPU between 2 replicas), while labels are binary (present or absent).
- **Autoscaler-aware**: The Ray autoscaler understands custom resources and can provision nodes with the required resources automatically.
- **Scheduling guarantees**: Replicas won't be scheduled until nodes with the required custom resources are available, preventing placement on incompatible nodes.

:::{tip}
Use descriptive resource names that reflect the node's capabilities, such as GPU types, availability zones, or hardware generations.
:::

## Environment variables

These environment variables modify Ray Serve's scheduling behavior. Set them before starting Ray.

### `RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY`

**Default**: `0` (disabled)

When enabled, switches from spread scheduling to **compact scheduling**. Compact scheduling:
- Packs replicas onto fewer nodes to minimize resource fragmentation
- Sorts pending replicas by resource requirements (largest first)
- Prefers scheduling on nodes that already have replicas (non-idle nodes)
- Uses best-fit bin packing to find the optimal node for each replica

```bash
export RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY=1
ray start --head
```
**When to use compact scheduling:** When you run many small deployments (such as 10 models each needing 0.5 CPUs), spread scheduling scatters them across nodes, wasting capacity. Compact scheduling fills nodes efficiently before using new ones. Cloud providers bill per node-hour. Packing replicas onto fewer nodes allows idle nodes to be released by the autoscaler, directly reducing your bill.

**When to avoid compact scheduling:** High availability is critical and you want replicas spread across nodes

:::{note}
Compact scheduling automatically falls back to spread scheduling when any deployment uses placement groups with `PACK`, `SPREAD`, or `STRICT_SPREAD` strategies. This happens because compact scheduling needs to predict where resources will be consumed to bin-pack effectively. With `STRICT_PACK`, all bundles are guaranteed to land on one node, making resource consumption predictable. With other strategies, bundles may spread across multiple nodes unpredictably, so the scheduler can't accurately track available resources per node.
:::

### `RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES`

**Default**: empty

A comma-separated list of custom resource names that should be prioritized when sorting replicas for compact scheduling. Resources listed earlier have higher priority.

```bash
export RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES="TPU,custom_accelerator"
ray start --head
```

When compact scheduling sorts replicas by resource requirements, the priority order is:
1. Custom resources in `RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES` (in order)
2. GPU
3. CPU
4. Memory
5. Other custom resources

This ensures that replicas requiring high-priority resources are scheduled first, reducing the chance of resource fragmentation.

## See also

- [Resource allocation](serve-resource-allocation) for configuring CPU, GPU, and other resources
- [Autoscaling](serve-autoscaling) for automatically adjusting replica count
- [Ray placement groups](placement-group-doc) for advanced resource co-location
