(serve-replica-ranks)=

# Replica ranks

:::{warning}
This API is experimental and may change between Ray minor versions.
:::

Replica ranks provide a unique identifier for **each replica within a deployment**. Each replica receives a **`ReplicaRank` object** containing rank information and **a world size (the total number of replicas)**. The rank object includes a global rank (an integer from 0 to N-1), a node rank, and a local rank on the node.

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
- `rank`: The global rank (an integer from 0 to N-1) representing this replica's unique identifier across all nodes.
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

1. **At replica startup:** When a replica starts, if your deployment has both a `reconfigure` method and a `user_config`, Ray Serve calls `reconfigure` after running `__init__`. This lets you initialize rank-aware state without duplicating code between `__init__` and `reconfigure`.
2. **When you update user_config:** When you redeploy with a new `user_config`, Ray Serve calls `reconfigure` on all running replicas. If your `reconfigure` method includes `rank` as a parameter, Ray Serve passes both the new `user_config` and the current rank as a `ReplicaRank` object.
3. **When a replica's rank changes:** During downscaling, ranks may be reassigned to maintain contiguity (0 to N-1). If your `reconfigure` method includes `rank` as a parameter and your deployment has a `user_config`, Ray Serve calls `reconfigure` with the existing `user_config` and the new rank as a `ReplicaRank` object.

:::{note}
**Requirements to receive rank updates:**

To get rank changes through `reconfigure`, your deployment needs:
- A class-based deployment (function deployments don't support `reconfigure`)
- A `reconfigure` method with `rank` as a parameter: `def reconfigure(self, user_config, rank: ReplicaRank)`
- A `user_config` in your deployment (even if it's just an empty dict: `user_config={}`)

Without a `user_config`, Ray Serve won't call `reconfigure` for rank changes.
:::

:::{tip}
If you'd like different behavior for when `reconfigure` is called with rank changes, [open a GitHub issue](https://github.com/ray-project/ray/issues/new/choose) to discuss your use case with the Ray Serve team.
:::

## How replica ranks work

:::{note}
**Rank reassignment is eventually consistent**

When replicas are removed during downscaling, rank reassignment to maintain contiguity (0 to N-1) doesn't happen immediately. The controller performs rank consistency checks and reassignment only when the deployment reaches a `HEALTHY` state in its update loop. This means there can be a brief period after downscaling where ranks are non-contiguous before the controller reassigns them.

This design choice prevents rank reassignment from interfering with ongoing deployment updates and rollouts. If you need immediate rank reassignment or different behavior, [open a GitHub issue](https://github.com/ray-project/ray/issues/new/choose) to discuss your use case with the Ray Serve team.
:::

:::{note}
**Ranks don't influence scheduling or eviction decisions**

Replica ranks are independent of scheduling and eviction decisions. The deployment scheduler doesn't consider ranks when placing replicas on nodes, so there's no guarantee that replicas with contiguous ranks (such as rank 0 and rank 1) will be on the same node. Similarly, during downscaling, the autoscaler's eviction decisions don't take replica ranks into account—any replica can be chosen for removal regardless of its rank.

If you need rank-aware scheduling or eviction (for example, to colocate replicas with consecutive ranks), [open a GitHub issue](https://github.com/ray-project/ray/issues/new/choose) to discuss your requirements with the Ray Serve team.
:::

Ray Serve manages replica ranks automatically throughout the deployment lifecycle. The system maintains these invariants:

1. Ranks are contiguous integers from 0 to N-1.
2. Each running replica has exactly one rank.
3. No two replicas share the same rank.

### Rank assignment lifecycle

The following table shows how ranks and world size behave during different events:

| Event | Local Rank | World Size |
|-------|------------|------------|
| Upscaling | No change for existing replicas | Increases to target count |
| Downscaling | Can change to maintain contiguity | Decreases to target count |
| Other replica dies(will be restarted) | No change | No change |
| Self replica dies | No change | No change |

:::{note}
World size always reflects the target number of replicas configured for the deployment, not the current number of running replicas. During scaling operations, the world size updates immediately to the new target, even while replicas are still starting or stopping.
:::

### Rank lifecycle state machine

```
┌─────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT LIFECYCLE                      │
└─────────────────────────────────────────────────────────────┘

Initial Deployment / Upscaling:
┌──────────┐      assign      ┌──────────┐
│ No Rank  │ ───────────────> │ Rank: N-1│
└──────────┘                  └──────────┘
                              (Contiguous: 0, 1, 2, ..., N-1)

Replica Crash:
┌──────────┐     release      ┌──────────┐     assign    ┌──────────┐
│ Rank: K  │ ───────────────> │ Released │ ────────────> │ Rank: K  │
│ (Dead)   │                  │          │               │ (New)    │
└──────────┘                  └──────────┘               └──────────┘
(K can be any rank from 0 to N-1)

:::{note}
When a replica crashes, Ray Serve automatically starts a replacement replica and assigns it the **same rank** as the crashed replica. This ensures rank contiguity is maintained without reassigning other replicas.
:::

Downscaling:
┌──────────┐     release      ┌──────────┐
│ Rank: K  │ ───────────────> │ Released │
│ (Stopped)│                  │          │
└──────────┘                  └──────────┘
          │
          └──> Remaining replicas may be reassigned to maintain
               contiguity: [0, 1, 2, ..., M-1] where M < N
(K can be any rank from 0 to N-1)

Controller Recovery:
┌──────────┐     recover      ┌──────────┐
│ Running  │ ───────────────> │ Rank: N  │
│ Replicas │                  │(Restored)│
└──────────┘                  └──────────┘
(Controller queries replicas to reconstruct rank state)
```

### Detailed lifecycle events

1. **Rank assignment on startup**: Ranks are assigned when replicas start, such as during initial deployment, cold starts, or upscaling. The controller assigns ranks and propagates them to replicas during initialization. New replicas receive the lowest available rank.

2. **Rank release on shutdown**: Ranks are released only after a replica fully stops, which occurs during graceful shutdown or downscaling. Ray Serve preserves existing rank assignments as much as possible to minimize disruption.

3. **Handling replica crashes**: If a replica crashes unexpectedly, the system releases its rank and assigns the **same rank** to the replacement replica. This means if replica with rank 3 crashes, the new replacement replica will also receive rank 3. The replacement receives its rank during initialization, and other replicas keep their existing ranks unchanged.

4. **Controller crash and recovery**: When the controller recovers from a crash, it reconstructs the rank state by querying all running replicas for their assigned ranks. Ranks aren't checkpointed; the system re-learns them directly from replicas during recovery.

5. **Maintaining rank contiguity**: After downscaling, the system may reassign ranks to remaining replicas to maintain contiguity (0 to N-1). Ray Serve minimizes reassignments by only changing ranks when necessary.
