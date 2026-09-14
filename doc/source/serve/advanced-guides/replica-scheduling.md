---
myst:
  html_meta:
    description: "Control where Ray Serve places replicas using max_replicas_per_node, placement groups, and node labels, plus downscaling behavior."
---

(serve-replica-scheduling)=

# Replica scheduling

This guide explains how Ray Serve schedules deployment replicas across your cluster and the APIs and environment variables you can use to control placement behavior.

## Quick reference: Choosing the right approach

| Goal | Solution | Example |
|------|----------|---------|
| Multi-GPU inference with tensor parallelism | `placement_group_bundles` + `STRICT_PACK` | vLLM with `tensor_parallel_size=4` |
| Target specific GPU types or zones | `label_selector` in `ray_actor_options` | Schedule on A100 nodes only |
| Limit replicas per node for high availability | `max_replicas_per_node` | Max 2 replicas of each deployment per node |
| Reduce cloud costs by packing nodes | `RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY=1` | Many small models sharing nodes |
| Reserve resources for worker actors | `placement_group_bundles` | Replica spawns Ray Data workers |
| Shard large embeddings across nodes | `placement_group_bundles` + `STRICT_SPREAD` | Recommendation model with distributed embedding table |
| Simple deployment, no special needs | Default (just `ray_actor_options`) | Single-GPU model |

## How replica scheduling works

When you deploy an application, Ray Serve's deployment scheduler determines where to place each replica actor across the available nodes in your Ray cluster. The scheduler runs on the Serve Controller and makes batch scheduling decisions during each update cycle. For information on configuring CPU, GPU, and other resource requirements for your replicas, see [Resource allocation](serve-resource-allocation).

```text
                              ┌──────────────────────────────────┐
                              │        serve.run(app)            │
                              └────────────────┬─────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Serve Controller                                   │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                        Deployment Scheduler                               │  │
│  │                                                                           │  │
│  │   1. Check placement_group_bundles  ──▶  PlacementGroupSchedulingStrategy │  │
│  │   2. Check target node affinity     ──▶  NodeAffinitySchedulingStrategy   │  │
│  │   3. Use default strategy           ──▶  SPREAD (default) or PACK         │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                               │
             ┌─────────────────────────────────┴─────────────────────────────────┐
             │                                                                   │
             ▼                                                                   ▼
┌─────────────────────────────────────┐               ┌─────────────────────────────────────┐
│    SPREAD Strategy (default)        │               │           PACK Strategy             │
│                                     │               │                                     │
│  Distributes replicas across nodes  │               │   Packs replicas onto fewer nodes   │
│  for fault tolerance                │               │   to minimize resource waste        │
│                                     │               │                                     │
│  ┌─────────┐ ┌─────────┐ ┌───────┐  │               │  ┌─────────┐ ┌─────────┐ ┌───────┐  │
│  │ Node 1  │ │ Node 2  │ │Node 3 │  │               │  │ Node 1  │ │ Node 2  │ │Node 3 │  │
│  │ ┌─────┐ │ │ ┌─────┐ │ │┌─────┐│  │               │  │ ┌─────┐ │ │         │ │       │  │
│  │ │ R1  │ │ │ │ R2  │ │ ││ R3  ││  │               │  │ │ R1  │ │ │  idle   │ │ idle  │  │
│  │ └─────┘ │ │ └─────┘ │ │└─────┘│  │               │  │ │ R2  │ │ │         │ │       │  │
│  │         │ │         │ │       │  │               │  │ │ R3  │ │ │         │ │       │  │
│  └─────────┘ └─────────┘ └───────┘  │               │  └─────────┘ └─────────┘ └───────┘  │
│                                     │               │               ▲           ▲        │
│  ✓ High availability                │               │               └───────────┘        │
│  ✓ Load balanced                    │               │           Can be released          │
│  ✓ Reduced contention               │               │  ✓ Fewer nodes = lower cloud costs │
└─────────────────────────────────────┘               └────────────────────────────────────┘
```

By default, Ray Serve uses a **spread scheduling strategy** that distributes replicas across nodes with best effort. This approach:
- Maximizes fault tolerance by avoiding concentration of replicas on a single node
- Balances load across the cluster
- Helps prevent resource contention between replicas

### Scheduling pipeline

The scheduler places each pending replica in three steps, largest resource request first:

1. **Filter**: Drop nodes that break a hard rule. A `label_selector` removes nodes without matching labels. While a deployment covers fewer nodes than its floor (see `RAY_SERVE_MIN_REPLICA_NODES`), nodes already hosting that deployment are removed as well.
2. **Score**: Rank the remaining nodes with the active scorer. The spread scorer (default) prefers the node with the fewest replicas of the same deployment, then the most free resources. The pack scorer (`RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY=1`) prefers nodes that already run replicas and picks the tightest fit to minimize fragmentation. If a `fallback_strategy` is provided, the scheduler tries the primary labels first and then each fallback in order.
3. **Bind**: Launch the replica on the chosen node with a soft `NodeAffinitySchedulingStrategy`. For deployments with `placement_group_bundles` and the `STRICT_PACK` strategy, the placement group is created with the chosen node as a soft target. Placement groups with other strategies are handed to Ray Core, which decides where the bundles land. If no node passed filtering, the replica is launched without a target so the autoscaler can add a node. If it was filtered out only by the floor, the launch carries a hard `ray.io/node-id` exclusion, so the replica waits for a new node instead of doubling up.

### Downscaling behavior

When Ray Serve scales down a deployment, it intelligently selects which replicas to stop:

1. **Non-running replicas first**: Pending, launching, or recovering replicas are stopped before running replicas.
2. **Minimize node count**: Running replicas are stopped from nodes with the fewest total replicas across all deployments, helping to free up nodes faster. Among replicas on the same node, newer replicas are stopped before older ones.
3. **Head node protection**: Replicas on the head node have the lowest priority for removal since the head node can't be released. Among replicas on the head node, newer replicas are stopped before older ones.
4. **Keep the floor**: A replica is skipped if stopping it would leave the deployment on fewer nodes than `RAY_SERVE_MIN_REPLICA_NODES` allows for the new replica count, so downscaling never collapses a spread deployment onto one node.

:::{note}
Running replicas on the head node isn't recommended for production deployments. The head node runs critical cluster processes such as the GCS and Serve controller, and replica workloads can compete for resources.
:::

## APIs for controlling replica placement

Ray Serve provides several options to control where replicas are scheduled. These parameters are configured through the [`@serve.deployment`](serve-configure-deployment) decorator. For the full API reference, see the [deployment decorator documentation](../api/doc/ray.serve.deployment_decorator.rst).

### Limit replicas per node with `max_replicas_per_node`

Use [`max_replicas_per_node`](../api/doc/ray.serve.deployment_decorator.rst) to cap the number of replicas of a deployment that can run on a single node. This is useful when:
- You want to ensure high availability by spreading replicas across nodes
- You want to avoid resource contention between replicas of the same deployment

```{literalinclude} ../doc_code/replica_scheduling.py
:start-after: __max_replicas_per_node_start__
:end-before: __max_replicas_per_node_end__
:language: python
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

#### Controlling placement group location with label selectors
You can further refine where placement groups are scheduled using a `placement_group_bundle_label_selector`. This field defines a list of label selectors to apply per-bundle when scheduling the Serve deployment. This allows you to restrict the nodes where your bundles (and therefore your replicas) are placed based on Ray node labels. For more information on Ray label selectors, see [Use labels to control scheduling](https://docs.ray.io/en/latest/ray-core/scheduling/labels.html).

```{literalinclude} ../doc_code/replica_scheduling.py
:start-after: __placement_group_labels_start__
:end-before: __placement_group_labels_end__
:language: python
```

The `placement_group_bundle_label_selector` accepts a list of dictionaries.
- Single selector: If you provide a list containing a single dictionary, that selector is applied to all bundles in `placement_group_bundles`.
- Per-bundle selector: If you provide a list of multiple dictionaries, the length must match `placement_group_bundles`. The *i*-th selector applies to the *i*-th bundle.

#### What placement groups and bundles mean

The following diagram illustrates how a deployment with `placement_group_bundles=[{"GPU": 1}, {"GPU": 1}, {"CPU": 4}]` and [`placement_group_strategy`](../api/doc/ray.serve.deployment_decorator.rst)` set to  "STRICT_PACK"` is scheduled:

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

| Scenario | Why placement groups help |
|----------|---------------------------|
| **Model parallelism** | Tensor parallelism or pipeline parallelism requires multiple GPUs that must communicate efficiently. Use `STRICT_PACK` to guarantee all GPUs are on the same node. For example, vLLM with `tensor_parallel_size=4` and the Ray distributed executor backend spawns 4 Ray worker actors (one per GPU shard), all of which must be on the same node for efficient inter-GPU communication via NVLink/NVSwitch. |
| **Replica spawns workers** | Your deployment creates Ray actors or tasks for parallel processing. Placement groups reserve resources for these workers. For example, a video processing service that spawns Ray tasks to decode frames in parallel, or a batch inference service using Ray Data to preprocess inputs before model inference. |
| **Cross-node distribution** | You need bundles spread across different nodes. Use `SPREAD` or `STRICT_SPREAD`. For example, serving a model with a massive embedding table (such as a recommendation model with billions of item embeddings) that must be sharded across multiple nodes because it exceeds single-node memory. Each bundle holds one shard, and `STRICT_SPREAD` ensures each shard is on a separate node. |

Don't use placement groups when:
- Your replica is self-contained and doesn't spawn additional actors/tasks
- You only need simple resource requirements (use `ray_actor_options` instead)
- You want to use `max_replicas_per_node`. The combination of these two options is not supported today.

:::{note}
**How `max_replicas_per_node` works:** Ray Serve creates a synthetic custom resource for each deployment. Every node implicitly has 1.0 of this resource, and each replica requests `1.0 / max_replicas_per_node` of it. For example, with `max_replicas_per_node=3`, each replica requests ~0.33 of the resource, so only 3 replicas can fit on a node before the resource is exhausted. This mechanism relies on Ray's standard resource scheduling, which conflicts with placement group scheduling.
:::

#### Configuring placement groups

The following example reserves 2 GPUs for each replica using a strict pack strategy:

```{literalinclude} ../doc_code/replica_scheduling.py
:start-after: __placement_group_start__
:end-before: __placement_group_end__
:language: python
```

The replica actor is scheduled in the first bundle, so the resources specified in `ray_actor_options` must be a subset of the first bundle's resources. All actors and tasks created by the replica are scheduled in the placement group by default (`placement_group_capture_child_tasks=True`).

### Target nodes with labels

You can use label selectors in [`ray_actor_options`](../api/doc/ray.serve.deployment_decorator.rst) to target replicas to specific nodes. This is the recommended approach for controlling which nodes run your replicas.

Then configure your deployment to require the specific labels:

```{literalinclude} ../doc_code/replica_scheduling.py
:start-after: __label_selectors_start__
:end-before: __label_selectors_end__
:language: python
```

First, start your Ray nodes with labels that identify their capabilities:

```{literalinclude} ../doc_code/replica_scheduling.py
:start-after: __label_selector_main_start__
:end-before: __label_selector_main_end__
:language: python
```

#### Soft constraints with `fallback_strategy`

By default, a `label_selector` acts as a hard constraint. If no node matches the selector, the replica remains pending indefinitely. You can relax this requirement by providing a `fallback_strategy` in `ray_actor_options`.

```{literalinclude} ../doc_code/replica_scheduling.py
:start-after: __fallback_strategy_start__
:end-before: __fallback_strategy_end__
:language: python
```

This allows you to express preferences. For example, when using PACK scheduling, the scheduler will attempt to find a node that matches the `label_selector` first. If no available node is found, the scheduler will retry scheduling using the rules defined in your fallback strategy.

Label selectors and fallback strategies offer several advantages for Ray Serve deployments:

- **Expressive placement constraints**: Ray automatically detects and populates labels for node attributes like `ray.io/accelerator-type`, or you can add custom labels at startup using the `--labels` flag. You can target these labels utilizing familiar Kubernetes-like syntax with complex operators (equality, negation (`!`), inclusion (`in`), and exclusion (`!in`)) to precisely filter which nodes run your replicas.
- **Autoscaler-aware**: The Ray autoscaler understands label selectors and can provision nodes with the required labels automatically.
- **Soft constraints**: Unlike custom resources which are strict requirements, label selectors can also be specified in the `fallback_strategy` field. This allows you to define preferred scheduling options while permitting the scheduler to utilize alternative nodes if the primary targets are unavailable, preventing deployments from stalling.

## Environment variables

These environment variables modify Ray Serve's scheduling behavior. Set them before starting Ray.

### `RAY_SERVE_MIN_REPLICA_NODES`

**Default**: `1`

The minimum number of distinct nodes each deployment's replicas must cover before the scheduler is free to pack them. The effective floor is the smaller of this value and the deployment's replica count, so a single-replica deployment is never blocked. The floor is enforced when placing replicas, when choosing replicas to stop during downscaling, and for either scorer.

```bash
export RAY_SERVE_MIN_REPLICA_NODES=2
ray start --head
```

**When to use it:** You run small clusters and need a service to survive the loss of any one node without giving up pack scheduling on the rest of the replicas. With a floor of `2`, six replicas of one CPU each on two eight-CPU nodes land as `3/3` or `5/1` instead of `6/0`, and all sixteen CPUs stay usable. Compare this with `max_replicas_per_node`, which caps replicas per node and must be recomputed whenever the node size or replica count changes.

### `RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY`

**Default**: `0` (disabled)

When enabled, switches from spread scheduling to **pack scheduling**. Pack scheduling:
- Packs replicas onto fewer nodes to minimize resource fragmentation
- Sorts pending replicas by resource requirements (largest first)
- Prefers scheduling on nodes that already have replicas (non-idle nodes)
- Uses best-fit bin packing to find the optimal node for each replica

```bash
export RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY=1
ray start --head
```
**When to use pack scheduling:** When you run many small deployments (such as 10 models each needing 0.5 CPUs), spread scheduling scatters them across nodes, wasting capacity. Pack scheduling fills nodes efficiently before using new ones. Cloud providers bill per node-hour. Packing replicas onto fewer nodes allows idle nodes to be released by the autoscaler, directly reducing your bill.

**When to avoid pack scheduling:** High availability is critical and you want replicas spread across nodes

:::{note}
Replicas whose placement groups use the `PACK`, `SPREAD`, or `STRICT_SPREAD` strategies are placed by Ray Core rather than by the Serve scheduler, because their bundles can land on several nodes and Serve can't predict which. With `STRICT_PACK`, all bundles are guaranteed to land on one node, so Serve chooses that node. Other deployments in the same cluster are unaffected and keep using the pipeline above.
:::

### `RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES`

**Default**: empty

A comma-separated list of custom resource names that should be prioritized when sorting replicas for pack scheduling. Resources listed earlier have higher priority.

```bash
export RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES="TPU,custom_accelerator"
ray start --head
```

When pack scheduling is enabled, the scheduler first filters the cluster to find nodes that match the `label_selector` (if specified). It then sorts the pending replicas by resource requirements to pack them efficiently. The priority order for sorting replicas is:
1. Custom resources in `RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES` (in order)
2. GPU
3. CPU
4. Memory
5. Other custom resources

This ensures that replicas requiring high-priority resources are scheduled first, reducing the chance of resource fragmentation.

## See also

- [Resource allocation](serve-resource-allocation) for configuring CPU, GPU, and other resources
- [Autoscaling](serve-autoscaling) for automatically adjusting replica count
- [Ray placement groups](ray-placement-group-doc-ref) for advanced resource co-location
