---
myst:
  html_meta:
    description: "Replica ranks give each Serve replica a unique rank and world size, with reconfigure hooks and a defined rank assignment lifecycle."
---

(serve-replica-ranks)=

# Replica ranks

:::{warning}
This API is experimental and may change between Ray minor versions.
:::

Replica ranks identify each replica within a deployment. Each replica receives a `ReplicaRank` object and a `world_size`, the target number of replicas. The rank object includes a global rank, a node rank, and a local rank on the node. Global ranks converge to 0 through N-1, where N is the target replica count. During an in-place update with `max_surge_percent > 0`, a replacement can receive a rank at or above `world_size`.

## Access replica ranks

You can access the rank and world size from within a deployment through the replica context using [`serve.get_replica_context()`](../api/doc/ray.serve.get_replica_context.rst).

The following example shows how to access replica rank information:

```{literalinclude} ../doc_code/replica_rank.py
:start-after: __replica_rank_start__
:end-before: __replica_rank_end__
:language: python
```

```{literalinclude} ../doc_code/replica_rank.py
:start-after: __replica_rank_start_run_main__
:end-before: __replica_rank_end_run_main__
:language: python
```

The [`ReplicaContext`](../api/doc/ray.serve.context.ReplicaContext.rst) provides two key fields:

- `rank`: A [`ReplicaRank`](../api/doc/ray.serve.schema.ReplicaRank.rst) object containing rank information for this replica. Access the integer rank value with `.rank`.
- `world_size`: The target number of replicas for the deployment.

The `ReplicaRank` object contains three fields:
- `rank`: The replica's unique integer rank across all nodes in the deployment. Ranks converge to 0 through N-1 after the deployment reaches `HEALTHY`.
- `node_rank`: The rank of the node this replica runs on (an integer from 0 to M-1 where M is the number of nodes).
- `local_rank`: The rank of this replica on its node (an integer from 0 to K-1 where K is the number of replicas on this node).

:::{note}
**Accessing rank values:**

To use the rank in your code, access the `.rank` attribute to get the integer value:

```python
context = serve.get_replica_context()
my_rank = context.rank.rank  # Get the integer rank value
my_node_rank = context.rank.node_rank  # Get the node rank
my_local_rank = context.rank.local_rank  # Get the local rank on this node
```

Most use cases only need the global `rank` value. The `node_rank` and `local_rank` are useful for advanced scenarios such as coordinating replicas on the same node.
:::

## Handle rank changes with reconfigure

When a replica's rank changes (such as during downscaling), Ray Serve can automatically call the `reconfigure` method on your deployment class to notify it of the new rank. This allows you to update replica-specific state when ranks change.

The following example shows how to implement `reconfigure` to handle rank changes:

```{literalinclude} ../doc_code/replica_rank.py
:start-after: __reconfigure_rank_start__
:end-before: __reconfigure_rank_end__
:language: python
```

```{literalinclude} ../doc_code/replica_rank.py
:start-after: __reconfigure_rank_start_run_main__
:end-before: __reconfigure_rank_end_run_main__
:language: python
```

### When reconfigure is called

Ray Serve automatically calls your `reconfigure` method in the following situations:

1. **At replica startup:** Ray Serve calls `reconfigure` after `__init__` if you provide a `user_config` or the method accepts a `rank` parameter. During an in-place update with `max_surge_percent > 0`, the rank passed at startup can be at or above `world_size`.
2. **When you update user_config:** When you redeploy with a new `user_config`, Ray Serve applies it through `reconfigure`. During an in-place update with `max_surge_percent > 0`, this can wait until replica replacement finishes. If your `reconfigure` method includes `rank` as a parameter, Ray Serve passes both the new `user_config` and the current rank as a `ReplicaRank` object.
3. **When a replica's rank changes:** After downscaling or an in-place update with `max_surge_percent > 0`, the controller may reassign ranks to restore contiguity. If your `reconfigure` method accepts `rank`, Ray Serve passes the existing `user_config` and the updated `ReplicaRank` object.

:::{note}
**Requirements to receive rank updates:**

To get rank changes through `reconfigure`, your deployment needs:
- A class-based deployment (function deployments don't support `reconfigure`)
- A `reconfigure` method with `rank` as a parameter: `def reconfigure(self, user_config, rank: ReplicaRank)`

If you don't provide a `user_config`, the method receives `None` for that argument.
:::

:::{tip}
If you'd like different behavior for when `reconfigure` is called with rank changes, [open a GitHub issue](https://github.com/ray-project/ray/issues/new/choose) to discuss your use case with the Ray Serve team.
:::

## How replica ranks work

:::{note}
**Rank reassignment is eventually consistent**

After downscaling or an in-place update with `max_surge_percent > 0`, rank reassignment to maintain contiguity (0 to N-1) doesn't happen immediately. The controller performs rank consistency checks and reassignment only when the deployment reaches a `HEALTHY` state in its update loop. This means there can be a brief period where ranks are non-contiguous before the controller reassigns them.

This design choice prevents rank reassignment from interfering with ongoing deployment updates and rollouts. If you need immediate rank reassignment or different behavior, [open a GitHub issue](https://github.com/ray-project/ray/issues/new/choose) to discuss your use case with the Ray Serve team.
:::

:::{note}
**In-place updates with surge can exceed the rank range**

During an in-place update with `max_surge_percent > 0`, replacement replicas start while old replicas still hold their ranks. A replacement can initialize with a rank at or above `world_size`. For a single-replica deployment, the replacement starts with rank 1 and `world_size` 1.

Don't use rank to index a structure of size `world_size` during such an in-place update, including in `__init__` or `reconfigure`. After the deployment reaches `HEALTHY`, the controller restores ranks to 0 through N-1. To receive rank changes through `reconfigure`, follow the requirements above. Ranks are scoped to each deployment, so separate deployments in a service rollout don't share rank assignments.
:::

:::{note}
**Ranks don't influence scheduling or eviction decisions**

Replica ranks are independent of scheduling and eviction decisions. The deployment scheduler doesn't consider ranks when placing replicas on nodes, so there's no guarantee that replicas with contiguous ranks (such as rank 0 and rank 1) will be on the same node. Similarly, during downscaling, the autoscaler's eviction decisions don't take replica ranks into account—any replica can be chosen for removal regardless of its rank.

If you need rank-aware scheduling or eviction (for example, to colocate replicas with consecutive ranks), [open a GitHub issue](https://github.com/ray-project/ray/issues/new/choose) to discuss your requirements with the Ray Serve team.
:::

Ray Serve manages replica ranks automatically throughout the deployment lifecycle. The system maintains these invariants:

1. Ranks converge to contiguous integers from 0 to N-1 after the deployment reaches `HEALTHY`. During an in-place update with `max_surge_percent > 0`, ranks can be at or above `world_size`.
2. Each running replica has exactly one rank.
3. No two replicas within the same deployment share a global rank.

### Rank assignment lifecycle

The following table shows how ranks and world size behave during different events:

| Event | Global rank | World size |
|-------|------------|------------|
| Upscaling | No change for existing replicas | Increases to target count |
| Downscaling | Can change to maintain contiguity | Decreases to target count |
| Replica replacement after a crash | Replacement receives the lowest available rank | No change unless the target count changes |
| In-place update with `max_surge_percent > 0` | Replacements can start at or above `world_size`; ranks converge after the update | Target replica count |

:::{note}
World size always reflects the target number of replicas configured for the deployment, not the current number of running replicas.
:::

### Rank lifecycle state machine

```text
┌─────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT LIFECYCLE                     │
└─────────────────────────────────────────────────────────────┘

Initial deployment / upscaling:
┌──────────┐      assign      ┌──────────────────┐
│ No rank  │ ───────────────> │ Lowest available │
└──────────┘                  │ rank             │
                              └──────────────────┘

Replica crash:
┌──────────────┐   release    ┌──────────┐   assign    ┌──────────────────┐
│ Rank: K      │ ───────────> │ Released │ ──────────> │ Lowest available │
│ Stopped      │              │          │             │ rank (may be K)  │
└──────────────┘              └──────────┘             └──────────────────┘

In-place update with max_surge_percent > 0:
Example: one replica, world_size = 1 throughout the update.

┌──────────────┐  start replacement   ┌──────────────┬──────────────┐
│ Old replica  │ ───────────────────> │ Old replica  │ Replacement  │
│ Rank: 0      │                      │ Rank: 0      │ Rank: 1      │
└──────────────┘                      └──────────────┴──────────────┘
                                                     │
                                Replacement ready; old replica stops
                                                     │
                                                     v
                                     ┌─────────────────────────────┐
                                     │ Replacement retains rank 1  │
                                     │ until rank reassignment     │
                                     └─────────────────────────────┘
                                                     │
                                   Deployment HEALTHY; reassign rank
                                                     │
                                                     v
                                     ┌─────────────────────────────┐
                                     │ Same replacement, rank 0    │
                                     │ No replica restart          │
                                     └─────────────────────────────┘

Downscaling:
┌──────────────┐   release    ┌──────────┐
│ Rank: K      │ ───────────> │ Released │
│ Stopped      │              │          │
└──────────────┘              └──────────┘
          │
          └──> Once HEALTHY, remaining ranks converge to 0 through M-1,
               where M is the reduced target replica count.

Controller recovery:
┌──────────┐   query ranks   ┌──────────────────┐
│ Running  │ ──────────────> │ Rank assignments │
│ replicas │                 │ restored         │
└──────────┘                 └──────────────────┘
```

### Detailed lifecycle events

1. **Rank assignment on startup**: Ranks are assigned when replicas start, such as during initial deployment, cold starts, or upscaling. The controller assigns ranks and propagates them to replicas during initialization. New replicas receive the lowest available rank.

2. **Rank release on shutdown**: Ranks are released only after a replica fully stops, which occurs during graceful shutdown or downscaling. Ray Serve preserves existing rank assignments as much as possible to minimize disruption.

3. **Handling replica crashes**: After a crashed replica fully stops, the controller releases its rank. A replacement receives the lowest available rank, which can be the crashed replica's rank. Don't depend on a replacement retaining a specific rank.

4. **Controller crash and recovery**: When the controller recovers from a crash, it reconstructs the rank state by querying all running replicas for their assigned ranks. Ranks aren't checkpointed; the system re-learns them directly from replicas during recovery.

5. **Maintaining rank contiguity**: After downscaling or an in-place update with `max_surge_percent > 0`, the controller may reassign ranks once the deployment reaches `HEALTHY` to restore contiguity (0 to N-1). Ray Serve minimizes reassignments by only changing ranks when necessary.
