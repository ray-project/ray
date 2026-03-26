(serve-gang-scheduling)=

# Gang scheduling

:::{note}
Gang scheduling is an **alpha** feature. The API may change in future releases.
:::

Gang scheduling enables you to co-schedule groups of deployment replicas atomically. A **gang** is a set of replicas that are reserved and started together using a single [Ray placement group](ray-placement-group-doc-ref). If the cluster doesn't have enough resources for the entire gang, none of the replicas in that gang are started.

This is useful for workloads where a partial set of replicas is useless, such as:
- **Data parallel attention deployment**: In WideEP deployments, data parallel attention - expert parallelism ranks are required coordinate with each other to perform dispatch-combine collective communication. Any rank failure leads to dispatch-combine collective hangs, and the entire data parallel attention - expert parallelism group needs to go through failover mechanism to re-establish collectives.
- **Any workload requiring coordinated startup**, where replicas need to discover each other's identities and establish communication before serving traffic.

## Getting started

Configure gang scheduling by passing a `GangSchedulingConfig` to the `@serve.deployment` decorator:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __basic_gang_start__
:end-before: __basic_gang_end__
:language: python
```

This creates 2 gangs of 4 replicas each resulting in a total of 8 replicas. Partial gang isn't allowed, and therefore `num_replicas` must be a multiple of `gang_size`.

### How resources are reserved within a gang

The `ray_actor_options` field defines the resource requirements for each replica actor (for example, CPU, GPU, memory). When gang scheduling is enabled **without** `placement_group_bundles`, Ray uses the resources from `ray_actor_options` as the bundle template for each replica's slot in the gang placement group. For example, with `ray_actor_options={"num_cpus": 0.25}` and `gang_size=4`, the gang placement group contains 4 bundles of `{"CPU": 0.25}` each.

When `placement_group_bundles` is also set, each replica occupies multiple consecutive bundles in the gang placement group instead of a single flat bundle. The replica actor runs in the **first** bundle, so the resources in `ray_actor_options` must fit within that first bundle. The remaining bundles are available for child actors or tasks spawned by the replica. See [Combining with placement group bundles](#combining-with-placement-group-bundles) for details.

You can also configure gang scheduling via `.options()`:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __options_start__
:end-before: __options_end__
:language: python
```

In production deployments, declarative YAML config files are often used:

```yaml
applications:
- name: my_app
  route_prefix: /
  import_path: my_module:app
  deployments:
  - name: MyModel
    num_replicas: 8
    ray_actor_options:
      num_cpus: 0.25
    gang_scheduling_config:
      gang_size: 4
```

### Accessing gang context

Each replica in a gang has access to a `GangContext` through the replica context. This provides the information replicas need to discover each other and coordinate:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __gang_context_start__
:end-before: __gang_context_end__
:language: python
```

`GangContext` contains the following information:

| Attribute | Type | Description |
|-------|------|-------------|
| `gang_id` | `str` | Unique identifier for this gang |
| `rank` | `int` | This replica's rank within the gang (0-indexed) |
| `world_size` | `int` | Total number of replicas in this gang (equal to `gang_size`) |
| `member_replica_ids` | `List[str]` | Replica IDs of all gang members, ordered by rank |
| `pg_name` | `str` | Name of the gang placement group |

Replicas can use `rank` and `world_size` to set up distributed communication, e.g. initializing NCCL process groups, and `member_replica_ids` to discover and connect to their peers.

## Placement group strategies

Gang scheduling supports two placement group strategies that control how replicas within a gang are distributed across nodes:

### PACK (default)

Packs all replicas in a gang onto as few nodes as possible. This is best for workloads that benefit from locality, such as data parallel ranks within data parallel attention - expert parallelism deployment for MoE LLMs.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __pack_strategy_start__
:end-before: __pack_strategy_end__
:language: python
```

### SPREAD

Spreads replicas in a gang across as many distinct nodes as possible. This is useful for fault isolation, where you want to minimize the impact of a single node failure.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __spread_strategy_start__
:end-before: __spread_strategy_end__
:language: python
```

Both strategies are **best effort**. PACK tries to colocate but may spread across nodes if a single node lacks capacity. SPREAD tries to distribute but may colocate if there aren't enough nodes.

### Combining with placement group bundles

You can combine gang scheduling with `placement_group_bundles` to reserve additional resources per replica within the gang. When both are set, the gang placement group contains the flattened bundles for all replicas in the gang. Each replica occupies `len(placement_group_bundles)` consecutive bundles, with the replica actor running in the first bundle.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __placement_group_bundles_start__
:end-before: __placement_group_bundles_end__
:language: python
```

```{image} images/gang-single-pg.png
:alt: Gang scheduling with a single bundle per replica
:width: 600px
```

In this example, each gang of 2 replicas creates a single gang placement group with 2 bundles (one `{"CPU": 1, "GPU": 1}` bundle per replica) upon scheduling. Note that `ray_actor_options={"num_cpus": 0}` is set so the replica actor doesn't request resources outside the placement group — all resource reservation is handled through the bundles.

If each replica needed multiple bundles, for example, one for the replica actor and one for a worker, the gang PG would contain `gang_size * len(placement_group_bundles)` total bundles. Replica 0 would occupy bundle indices 0 and 1, while replica 1 would occupy indices 2 and 3.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __multi_placement_group_bundles_start__
:end-before: __multi_placement_group_bundles_end__
:language: python
```

```{image} images/gang-multi-pg.png
:alt: Gang scheduling with multiple bundles per replica
:width: 600px
```

You can also use `placement_group_bundle_label_selector` to control which nodes the gang's bundles are placed on. The per-replica label selector is replicated across all replicas in the gang, so every replica is steered to nodes matching the selector. For example, to schedule all gang members on nodes with A100 GPUs:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __label_selector_start__
:end-before: __label_selector_end__
:language: python
```

## Autoscaling

Gang scheduling works with Ray Serve autoscaling (`num_replicas="auto"`). When autoscaling is enabled, replica counts are automatically quantized to multiples of `gang_size`:

- **Scaling up**: The target replica count is rounded **up** to the next multiple of `gang_size`, ensuring sufficient capacity.
- **Scaling down**: The target replica count is rounded **down** to the nearest multiple of `gang_size`, releasing only complete gangs.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __autoscaling_start__
:end-before: __autoscaling_end__
:language: python
```

When using autoscaling with gang scheduling, `min_replicas`, `max_replicas`, and `initial_replicas` must all be multiples of `gang_size`.

:::{note}
Scale-to-zero (`min_replicas=0`) is not supported with gang scheduling.
:::

In Ray Serve autoscaler, gang quantization is handled automatically by a `GangSchedulingAutoscalingPolicy` wrapper that is injected around the base autoscaling policy.

**Example**: With `gang_size=4` and 8 current replicas, if the base autoscaling policy recommends 5 replicas (scale down), the gang-aware policy rounds down to 4, releasing one complete gang. If the policy recommends 10 replicas (scale up), the gang-aware policy rounds up to 12, creating one complete new gang.

## Fault tolerance

### RESTART_GANG policy

The `runtime_failure_policy` controls what happens when a replica in a running gang fails a health check. The default policy is `RESTART_GANG`:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __fault_tolerance_start__
:end-before: __fault_tolerance_end__
:language: python
```

When any replica in a gang fails its health check, **all replicas in that gang** are torn down and a fresh gang is created. This ensures gang members always start together and can re-establish coordinated state (for example, NCCL communicators).

:::{note}
`RESTART_REPLICA` (restarting only the failed replica while keeping healthy gang members running) is not yet supported. If you need this behavior, file a [GitHub issue](https://github.com/ray-project/ray/issues).
:::

### Incomplete gang detection

If a gang member dies while the Serve controller is down, the controller detects the incomplete gang on recovery. It checks each running replica's `gang_context.member_replica_ids` against the set of tracked replicas. If any member is missing, the entire gang is restarted.

### Controller recovery

Gang scheduling state survives controller restarts: `GangContext` is persisted in replica metadata and restored when the controller reconnects to existing replicas.


## How gang scheduling works

This section describes the internal mechanics for users who want to understand the system in depth.

### Placement group lifecycle

1. **Reservation**: During each reconciliation loop, the Serve controller's deployment scheduler creates named placement groups for each gang. The bundles in the placement group are constructed by repeating the per-replica resource requirements `gang_size` times, or flattening `placement_group_bundles` across all replicas in the gang.

2. **Atomic startup**: Once a gang placement group is ready, all replicas in the gang are scheduled together. Each replica is assigned a `rank` (0 to `gang_size - 1`) and receives a `GangContext`. Replicas are scheduled into specific bundle indices within the gang placement group.

3. **Cleanup**: When a deployment is deleted or a gang is replaced, its placement group is removed. The controller also runs periodic leak detection to clean up orphaned gang placement groups.

### Scaling

**Upscaling**: The controller reserves gang placement groups, then starts all replicas in each gang together once the placement group is ready. Gangs that can't be scheduled due to insufficient resources are retried on subsequent reconciliation loops while successfully scheduled gangs proceed. If any replica fails during startup, the entire gang is stopped and retried.

**Downscaling**: The controller selects **complete gangs** for removal rather than individual replicas, ensuring no gang is left partially running.

### Rolling updates

When a deployment config or code changes, the rolling update process replaces **complete gangs** atomically. The rollout size is aligned to `gang_size` multiples, so each update wave stops and starts whole gangs.

### Migration

When replicas need to migrate due to node draining, entire gangs are migrated atomically rather than individual replicas, preserving the atomic scheduling guarantees.

## Constraints and limitations

- `max_replicas_per_node` cannot be used together with gang scheduling since gang scheduling uses placement groups.
