---
myst:
  html_meta:
    description: "Co-schedule groups of Serve replicas atomically with gang scheduling, using placement group PACK or SPREAD strategies and gang context."
---

(serve-gang-scheduling)=

# Gang scheduling

:::{note}
Gang scheduling is an alpha feature. The API might change in future releases.
:::

Gang scheduling co-schedules groups of deployment replicas atomically. A *gang* is a set of replicas that Serve reserves and starts together using a single [Ray placement group](ray-placement-group-doc-ref). If the cluster doesn't have enough resources for the entire gang, Serve starts none of the replicas in that gang.

Gang scheduling helps with workloads where a partial set of replicas is useless, such as the following:
- **Data parallel attention deployment**: In WideEP deployments, the ranks of a data parallel attention and expert parallelism group must coordinate with each other to perform dispatch-combine collective communication. Any rank failure hangs the dispatch-combine collectives, and the entire group needs to fail over to re-establish them.
- **Coordinated startup**: Any workload where replicas need to discover each other's identities and establish communication before serving traffic.

## Configure gang scheduling

Configure gang scheduling by passing a `GangSchedulingConfig` to the `@serve.deployment` decorator:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __basic_gang_start__
:end-before: __basic_gang_end__
:language: python
```

This example creates two gangs of four replicas each, for a total of eight replicas. Serve doesn't allow partial gangs, so `num_replicas` must be a multiple of `gang_size`.

(how-resources-are-reserved-within-a-gang)=
### How does Serve reserve resources within a gang?

The `ray_actor_options` field defines the resource requirements for each replica actor, such as CPU, GPU, and memory. When you enable gang scheduling without setting `placement_group_bundles`, Ray uses the resources from `ray_actor_options` as the bundle template for each replica's slot in the gang placement group. For example, with `ray_actor_options={"num_cpus": 0.25}` and `gang_size=4`, the gang placement group contains four bundles of `{"CPU": 0.25}` each.

When you also set `placement_group_bundles`, each replica occupies multiple consecutive bundles in the gang placement group instead of a single flat bundle. The replica actor runs in the first bundle, so the resources in `ray_actor_options` must fit within that first bundle. The remaining bundles are available for child actors or tasks that the replica spawns. For details, see [Combine gang scheduling with placement group bundles](#combine-gang-scheduling-with-placement-group-bundles).

You can also configure gang scheduling through `.options()`:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __options_start__
:end-before: __options_end__
:language: python
```

In production, you often configure gang scheduling in a declarative Serve config file:

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

(accessing-gang-context)=
### Access the gang context

Each replica in a gang can access a `GangContext` through the replica context. The `GangContext` holds the information that replicas need to discover each other and coordinate:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __gang_context_start__
:end-before: __gang_context_end__
:language: python
```

For the full interface, see {py:class}`~ray.serve.context.GangContext`.

Replicas can use `rank` and `world_size` to set up distributed communication, such as initializing NCCL process groups. They can use `member_replica_ids` to discover and connect to their peers.

## Placement group strategies

Gang scheduling supports two placement group strategies, which control how Serve distributes the replicas in a gang across nodes.

(pack-default)=
### PACK

PACK is the default strategy. It places all replicas in a gang on as few nodes as possible. It works best for workloads that benefit from locality, such as the data parallel ranks of a data parallel attention and expert parallelism deployment for mixture-of-experts (MoE) LLMs.

Although PACK colocates as many replicas per node as possible, it doesn't guarantee that ranks are contiguous within a node. For example, a gang of eight replicas split across two nodes might get ranks `{0, 3, 5, 7}` on one node and `{1, 2, 4, 6}` on the other, rather than `{0, 1, 2, 3}` and `{4, 5, 6, 7}`. Don't write application code that assumes consecutive ranks share a node. This matters most when a single replica owns multiple bundles, such as a tensor-parallel group. The bundles that belong to one replica might land on different nodes, which splits collective communication across the network. To ensure that the bundles for a single replica land on the same node, sort the gang placement group's bundle indices by node IP and assign each replica a contiguous slice of the sorted order.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __pack_strategy_start__
:end-before: __pack_strategy_end__
:language: python
```

### SPREAD

SPREAD places the replicas in a gang across as many distinct nodes as possible. It helps with fault isolation, when you want to minimize the impact of a single node failure.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __spread_strategy_start__
:end-before: __spread_strategy_end__
:language: python
```

Both strategies are best effort. PACK tries to colocate replicas but might spread them across nodes if a single node lacks capacity. SPREAD tries to distribute replicas but might colocate them if there aren't enough nodes.

(combining-with-placement-group-bundles)=
### Combine gang scheduling with placement group bundles

You can combine gang scheduling with `placement_group_bundles` to reserve additional resources per replica within the gang. When you set both, the gang placement group contains the flattened bundles for all replicas in the gang. Each replica occupies `len(placement_group_bundles)` consecutive bundles, and the replica actor runs in the first bundle.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __placement_group_bundles_start__
:end-before: __placement_group_bundles_end__
:language: python
```

```{image} images/gang-single-pg.png
:alt: Gang scheduling with a single bundle per replica
:width: 600px
```

In this example, scheduling each gang of two replicas creates a single gang placement group with two bundles, one `{"CPU": 1, "GPU": 1}` bundle per replica. The example sets `ray_actor_options={"num_cpus": 0}` so the replica actor doesn't request resources outside the placement group. All resource reservation goes through the bundles.

If each replica needs multiple bundles, such as one for the replica actor and one for a worker, the gang placement group contains `gang_size * len(placement_group_bundles)` bundles in total. With two bundles per replica, replica 0 occupies bundle indices 0 and 1, and replica 1 occupies indices 2 and 3.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __multi_placement_group_bundles_start__
:end-before: __multi_placement_group_bundles_end__
:language: python
```

```{image} images/gang-multi-pg.png
:alt: Gang scheduling with multiple bundles per replica
:width: 600px
```

You can also use `placement_group_bundle_label_selector` to control which nodes host the gang's bundles. Serve applies the per-replica label selector to every replica in the gang, so every replica goes to nodes that match the selector. The following example schedules all gang members on nodes with A100 GPUs:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __label_selector_start__
:end-before: __label_selector_end__
:language: python
```

## Autoscaling

Gang scheduling works with Ray Serve autoscaling, such as a deployment with `num_replicas="auto"`. With autoscaling enabled, the Serve autoscaler always rounds the replica count that the base autoscaling policy recommends up to the next multiple of `gang_size`, so the deployment never runs below the capacity that the base policy requested.

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __autoscaling_start__
:end-before: __autoscaling_end__
:language: python
```

When using autoscaling with gang scheduling, `min_replicas`, `max_replicas`, and `initial_replicas` must all be multiples of `gang_size`.

To scale to zero, set `min_replicas=0`. The deployment scales up and down in complete gangs.

The Serve autoscaler handles gang quantization automatically by wrapping the base autoscaling policy in a `GangSchedulingAutoscalingPolicy`.

For example, with `gang_size=4`, if the base autoscaling policy recommends 5 replicas, the `GangSchedulingAutoscalingPolicy` rounds up to 8. If the policy recommends 10 replicas, the gang-aware policy rounds up to 12. Always rounding up makes the output deterministic. The same desired count produces the same replica target regardless of the current replica count, which prevents oscillation between two gang-aligned values.

## Fault tolerance

This section describes how Serve handles replica failures and controller restarts in a gang.

### RESTART_GANG policy

The `runtime_failure_policy` controls what happens when a replica in a running gang fails a health check. The default policy is `RESTART_GANG`:

```{literalinclude} ../doc_code/gang_scheduling.py
:start-after: __fault_tolerance_start__
:end-before: __fault_tolerance_end__
:language: python
```

When any replica in a gang fails its health check, Serve tears down all replicas in that gang and creates a fresh gang. This ensures that gang members always start together and can re-establish coordinated state, such as NCCL communicators.

:::{note}
`RESTART_REPLICA`, which restarts only the failed replica and keeps healthy gang members running, isn't supported yet. If you need this behavior, file a [GitHub issue](https://github.com/ray-project/ray/issues).
:::

### Incomplete gang detection

If a gang member dies while the Serve controller is down, the controller detects the incomplete gang on recovery. It checks each running replica's `gang_context.member_replica_ids` against the set of tracked replicas. If any member is missing, the controller restarts the entire gang.

### Controller recovery

Gang scheduling state survives controller restarts. The `GangContext` is persisted in replica metadata and restored when the controller reconnects to existing replicas.

(how-gang-scheduling-works)=
## How does gang scheduling work?

This section describes how gang scheduling works internally, if you want to understand the system in depth.

### Placement group lifecycle

1. **Reservation**: During each reconciliation loop, the Serve controller's deployment scheduler creates named placement groups for each gang. It builds the bundles in each placement group by repeating the per-replica resource requirements `gang_size` times, or by flattening `placement_group_bundles` across all replicas in the gang.

1. **Atomic startup**: When a gang placement group is ready, the controller schedules all replicas in the gang together. Each replica receives a `rank`, from 0 to `gang_size - 1`, and a `GangContext`. The controller schedules each replica into a specific bundle index within the gang placement group.

1. **Cleanup**: When a deployment is deleted or a gang is replaced, the controller removes the gang's placement group. The controller also runs periodic leak detection to clean up orphaned gang placement groups.

### Scaling

To scale up, the controller reserves gang placement groups, then starts all replicas in each gang together when the placement group is ready. If a gang can't be scheduled because of insufficient resources, the controller retries it on later reconciliation loops, while gangs that scheduled successfully proceed. If any replica fails during startup, the controller stops and retries the entire gang.

To scale down, the controller selects complete gangs to remove rather than individual replicas, so no gang keeps running with only some of its replicas.

### Rolling updates

When a deployment's config or code changes, the rolling update replaces complete gangs atomically. Serve aligns the rollout size to multiples of `gang_size`, so each update wave stops and starts whole gangs.

### Migration

When replicas need to migrate because a node is draining, Serve migrates entire gangs atomically rather than individual replicas, which preserves the atomic scheduling guarantees.

## Constraints and limitations

- You can't use `max_replicas_per_node` together with gang scheduling, because gang scheduling uses placement groups.
