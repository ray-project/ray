---
myst:
  html_meta:
    description: "How Ray schedules tasks and actors onto nodes: labels, resources, and the DEFAULT, SPREAD, placement group, and node affinity strategies."
---

(ray-scheduling)=

# Scheduling

This page provides an overview of how Ray decides to schedule tasks and actors to nodes.

## Scheduling at a glance

Ray schedules every task and actor without any configuration from you. Each control that follows has a default that applies until you override it, so read this section as a map of the available controls rather than a list of required settings.

Ray places a task or actor in two steps. First it narrows the cluster to the nodes that can run the work, using the resource requirements and label selectors you declare. Then it picks one of those nodes using the scheduling strategy. For tasks under the `"DEFAULT"` strategy, data locality takes precedence over utilization, so Ray prefers a node that already holds the task's large arguments.

```{list-table}
:header-rows: 1
:widths: 22 30 48

* - Control
  - Default
  - Where it's documented
* - Node logical resources
  - Auto-detected from the machine's physical CPU, GPU, and memory
  - {ref}`Resources <ray-scheduling-resources>`
* - Task resource requirements
  - 1 logical CPU
  - {ref}`Specifying resource requirements <resource-requirements>`
* - Actor resource requirements
  - 1 logical CPU to schedule, 0 to run
  - {ref}`Specifying resource requirements <resource-requirements>`
* - Node labels
  - `ray.io/node-id` on every node, plus `ray.io/accelerator-type` on accelerator nodes
  - {doc}`./labels`
* - Scheduling strategy
  - `"DEFAULT"`
  - {ref}`Scheduling strategies <ray-scheduling-strategies>`
* - Data locality
  - Enabled for tasks, ignored for actors and when you set a strategy
  - {ref}`Locality-aware scheduling <ray-scheduling-locality>`
* - Gang placement
  - None. Ray schedules each task and actor independently unless you create a placement group.
  - {doc}`./placement-group`
```

Because the actor scheduling default is non-zero while its running default is zero, an actor that declares no resource requirements still needs a node with at least one free CPU to start, and any number of them can then run there. A node with `num_cpus=0` runs neither tasks nor actors by default.

The `"DEFAULT"` strategy's node selection is tunable through environment variables, though most clusters never need to change them:

```{list-table}
:header-rows: 1
:widths: 40 12 48

* - Environment variable
  - Default
  - Effect
* - `RAY_scheduler_spread_threshold`
  - `0.5`
  - Utilization below this scores a node as 0, making it equally preferred with other lightly loaded nodes.
* - `RAY_scheduler_top_k_fraction`
  - `0.2`
  - Sizes the candidate set Ray randomly picks from, as a fraction of total cluster nodes.
* - `RAY_scheduler_top_k_absolute`
  - `1`
  - Sets a floor on that candidate set, so `k` never drops below this in a small cluster.
```

See {ref}`"DEFAULT" <ray-scheduling-strategies>` for how Ray combines these into a score.

## Labels

Use default and custom labels to control where Ray schedules tasks, actors, and placement group bundles. See {doc}`./labels`.

Labels are a beta feature. As this feature becomes stable, the Ray team recommends using labels to replace the following patterns:

- `NodeAffinitySchedulingStrategy` when `soft=false`. Use the default `ray.io/node-id` label instead.
- The `accelerator_type` option for tasks and actors. Use the default `ray.io/accelerator-type` label instead.

:::{note}
Using custom resources for label-based scheduling is a legacy pattern. Use custom resources only when you need to manage scheduling with numeric values.
:::

(ray-scheduling-resources)=

## Resources

Each task or actor has {ref}`specified resource requirements <resource-requirements>`. Relative to those requirements, a node can be in one of the following two states:

- **Feasible**: the node has the required resources to run the task or actor. Depending on whether those resources are free, a feasible node is in one of two sub-states:

  - **Available**: the node has the required resources and they're free.
  - **Unavailable**: the node has the required resources but other tasks or actors are using them.

- **Infeasible**: the node doesn't have the required resources. For example, a CPU-only node is infeasible for a GPU task.

Resource requirements are hard requirements, so only feasible nodes are eligible to run the task or actor. If feasible nodes exist, Ray either chooses an available node or waits until an unavailable node becomes available, depending on other factors that the following sections describe. If all nodes are infeasible, Ray can't schedule the task or actor until feasible nodes are added to the cluster.

(ray-scheduling-strategies)=

## Scheduling strategies

Set the {func}`scheduling_strategy <ray.remote>` option on a task or actor to choose the strategy Ray uses to decide the best node among feasible nodes. The following sections describe the supported strategies.

### "DEFAULT"

`"DEFAULT"` is Ray's default strategy. Ray schedules tasks or actors onto a group of the top k nodes. To rank the nodes, Ray first favors nodes that already have tasks or actors scheduled, for locality, and then favors nodes with low resource utilization, for load balancing. Within the top k group, Ray picks a node at random to further improve load balancing and reduce cold-start delays in large clusters.

Internally, Ray calculates a score for each node in a cluster based on the utilization of its logical resources. If the utilization is below a threshold, the score is 0. Otherwise, the score is the resource utilization itself, where a score of 1 means the node is fully used. The `RAY_scheduler_spread_threshold` environment variable sets the threshold, which defaults to 0.5. Ray selects the best node for scheduling by randomly picking from the top k nodes with the lowest scores. To compute `k`, Ray multiplies the number of nodes in the cluster by the `RAY_scheduler_top_k_fraction` environment variable, then takes the larger of that product and the `RAY_scheduler_top_k_absolute` environment variable. By default, `k` is 20% of the total number of nodes.

Ray currently handles actors that don't require any resources, meaning `num_cpus=0` with no other resources, as a special case. For these actors, Ray randomly chooses a node in the cluster without considering resource utilization. Because Ray chooses the node at random, these actors are effectively spread across the cluster.

```{literalinclude} ../doc_code/scheduling.py
:language: python
:start-after: __default_scheduling_strategy_start__
:end-before: __default_scheduling_strategy_end__
```

### "SPREAD"

The `"SPREAD"` strategy tries to spread tasks or actors among available nodes.

```{literalinclude} ../doc_code/scheduling.py
:language: python
:start-after: __spread_scheduling_strategy_start__
:end-before: __spread_scheduling_strategy_end__
```

### PlacementGroupSchedulingStrategy

{py:class}`~ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy` schedules the task or actor where the placement group is located. This strategy is useful for actor gang scheduling. See {ref}`Placement groups <ray-placement-group-doc-ref>`.

### NodeAffinitySchedulingStrategy

{py:class}`~ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy` is a low-level strategy that schedules a task or actor onto a particular node, which you specify by its node ID. The `soft` flag specifies whether the task or actor can run somewhere else if the specified node doesn't exist, such as after the node dies, or is infeasible because it doesn't have the resources required to run the task or actor. In these cases, if `soft` is `True`, Ray schedules the task or actor onto a different feasible node. Otherwise, the task or actor fails with {py:class}`~ray.exceptions.TaskUnschedulableError` or {py:class}`~ray.exceptions.ActorUnschedulableError`. As long as the specified node is alive and feasible, the task or actor only runs there, regardless of the `soft` flag. So if the node has no available resources, the task or actor waits until resources become available.

Use this strategy *only* if other high-level scheduling strategies, such as a {ref}`placement group <ray-placement-group-doc-ref>`, can't give you the task or actor placement you want. This strategy has the following known limitations:

- It's a low-level strategy that prevents optimizations by a smart scheduler.
- It can't fully use an autoscaling cluster because you must know the node IDs when you create the tasks or actors.
- It can be difficult to make the best static placement decision, especially in a multi-tenant cluster. For example, an application doesn't know what else is being scheduled onto the same nodes.

```{literalinclude} ../doc_code/scheduling.py
:language: python
:start-after: __node_affinity_scheduling_strategy_start__
:end-before: __node_affinity_scheduling_strategy_end__
```

(ray-scheduling-locality)=

## Locality-aware scheduling

By default, Ray prefers available nodes that have a task's large arguments stored locally, to avoid transferring data over the network. If a task has multiple large arguments, Ray prefers the node with the most object bytes local. This preference takes precedence over the `"DEFAULT"` scheduling strategy, so Ray tries to run the task on the locality-preferred node regardless of that node's resource utilization. However, if the locality-preferred node isn't available, Ray might run the task somewhere else. When you specify another scheduling strategy, that strategy takes precedence and Ray doesn't consider data locality.

:::{note}
Locality-aware scheduling applies only to tasks, not actors.
:::

```{literalinclude} ../doc_code/scheduling.py
:language: python
:start-after: __locality_aware_scheduling_start__
:end-before: __locality_aware_scheduling_end__
```

## More about Ray scheduling

```{toctree}
:maxdepth: 1

labels
resources
accelerators
placement-group
memory-management
ray-oom-prevention
```
