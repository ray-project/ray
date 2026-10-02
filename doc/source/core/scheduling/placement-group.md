---
myst:
  html_meta:
    description: "Atomically reserve resources across nodes with placement groups, Ray's gang scheduling primitive, using bundles and placement strategies."
---

# Placement groups

(ray-placement-group-doc-ref)=

Use placement groups to atomically reserve groups of resources across multiple nodes, a concept commonly known as gang scheduling. After Ray reserves the resources, you can use placement groups to schedule Ray tasks and actors either packed together for locality with the PACK strategy or spread apart with the SPREAD strategy. You typically use placement groups to gang-schedule actors, but they also support tasks.

Some real-world use cases include the following:

- **Distributed machine learning training**: Distributed training, such as in {ref}`Ray Train <train-docs>` and {ref}`Ray Tune <tune-main>`, uses the placement group APIs for gang scheduling. In these settings, all resources for a trial must be available at the same time. Gang scheduling is a critical technique for all-or-nothing scheduling in deep learning training.
- **Fault tolerance in distributed training**: You can use placement groups to configure fault tolerance. In Ray Tune, packing the related resources from a single trial together can be beneficial, so that a node failure affects fewer trials. In libraries that support elastic training, such as XGBoost-Ray, spreading the resources across multiple nodes can help ensure that training continues even when a node dies.

## Key concepts

### Bundles

A *bundle* is a collection of resources. It can be a single resource, such as `{"CPU": 1}`, or a group of resources, such as `{"CPU": 1, "GPU": 4}`. A bundle is a unit of reservation for placement groups. Scheduling a bundle means that Ray finds a node that fits the bundle and reserves the resources that the bundle specifies. A bundle must fit on a single node in the Ray cluster. For example, if you have an 8-CPU node and a 1-CPU node and want to schedule a bundle that requires `{"CPU": 9}`, Ray can't schedule the `{"CPU": 9}` bundle, because no single node has 9 CPUs.

### Placement group

A *placement group* reserves resources from the cluster. To use the reserved resources, tasks or actors must use the {ref}`PlacementGroupSchedulingStrategy <ray-placement-group-schedule-tasks-actors-ref>`.

- Ray represents placement groups with a list of bundles. For example, `{"CPU": 1} * 4` means you want to reserve four bundles, each with 1 CPU.
- Ray then places the bundles across nodes in the cluster according to the {ref}`placement strategies <pgroup-strategy>`.
- After Ray creates the placement group, you can schedule tasks or actors according to the placement group, and even on individual bundles.

## Create a placement group (reserve resources)

Create a placement group with {func}`ray.util.placement_group`. Placement groups take a list of bundles and a {ref}`placement strategy <pgroup-strategy>`.

Bundles are specified by a list of dictionaries, e.g., `[{"CPU": 1}, {"CPU": 1, "GPU": 1}]`).

- `CPU` corresponds to `num_cpus` as used in {func}`ray.remote <ray.remote>`.
- `GPU` corresponds to `num_gpus` as used in {func}`ray.remote <ray.remote>`.
- `memory` corresponds to `memory` as used in {func}`ray.remote <ray.remote>`
- Other resources corresponds to `resources` as used in {func}`ray.remote <ray.remote>` (E.g., `ray.init(resources={"disk": 1})` can have a bundle of `{"disk": 1}`).

Placement group scheduling is asynchronous. `ray.util.placement_group` returns immediately.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __create_pg_start__
:end-before: __create_pg_end__
```
:::


:::{tab-item} Java
```java
// Initialize Ray.
Ray.init();

// Construct a list of bundles.
Map<String, Double> bundle = ImmutableMap.of("CPU", 1.0);
List<Map<String, Double>> bundles = ImmutableList.of(bundle);

// Make a creation option with bundles and strategy.
PlacementGroupCreationOptions options =
  new PlacementGroupCreationOptions.Builder()
    .setBundles(bundles)
    .setStrategy(PlacementStrategy.STRICT_SPREAD)
    .build();

PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
```
:::

:::{tab-item} C++
```c++
// Initialize Ray.
ray::Init();

// Construct a list of bundles.
std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1.0}}};

// Make a creation option with bundles and strategy.
ray::internal::PlacementGroupCreationOptions options{
    false, "my_pg", bundles, ray::internal::PlacementStrategy::PACK};

ray::PlacementGroup pg = ray::CreatePlacementGroup(options);
```
:::
::::

To block your program until the placement group is ready, use one of the following two APIs:

- {func}`ready <ray.util.placement_group.PlacementGroup.ready>`, which is compatible with `ray.get`.
- {func}`wait <ray.util.placement_group.PlacementGroup.wait>`, which blocks the program until the placement group is ready.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __ready_pg_start__
:end-before: __ready_pg_end__
```
:::

:::{tab-item} Java
```java
// Wait for the placement group to be ready within the specified time(unit is seconds).
boolean ready = pg.wait(60);
Assert.assertTrue(ready);

// You can look at placement group states using this API.
List<PlacementGroup> allPlacementGroup = PlacementGroups.getAllPlacementGroups();
for (PlacementGroup group: allPlacementGroup) {
  System.out.println(group);
}
```
:::

:::{tab-item} C++
```c++
// Wait for the placement group to be ready within the specified time(unit is seconds).
bool ready = pg.Wait(60);
assert(ready);

// You can look at placement group states using this API.
std::vector<ray::PlacementGroup> all_placement_group = ray::GetAllPlacementGroups();
for (const ray::PlacementGroup &group : all_placement_group) {
  std::cout << group.GetName() << std::endl;
}
```
:::
::::

Verify that Ray successfully created the placement group.

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list placement-groups
```

```bash
======== List: 2023-04-07 01:15:05.682519 ========
Stats:
------------------------------
Total: 1

Table:
------------------------------
    PLACEMENT_GROUP_ID                    NAME      CREATOR_JOB_ID  STATE
0  3cd6174711f47c14132155039c0501000000                  01000000  CREATED
```

Ray successfully created the placement group. Out of the `{"CPU": 2, "GPU": 2}` resources, the placement group reserves `{"CPU": 1, "GPU": 1}`. You can use the reserved resources only when you schedule tasks or actors with a placement group. The following diagram shows the bundle of 1 CPU and 1 GPU that the placement group reserved.

```{image} ../images/pg_image_1.png
:align: center
```

Ray creates placement groups atomically. If a bundle can't fit in any of the current nodes, Ray reserves no resources for the placement group. To illustrate this, create another placement group with the two bundles `{"CPU":1}, {"GPU": 2}`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __create_pg_failed_start__
:end-before: __create_pg_failed_end__
```
:::
::::

Verify that the new placement group is pending creation.

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list placement-groups
```

```bash
======== List: 2023-04-07 01:16:23.733410 ========
Stats:
------------------------------
Total: 2

Table:
------------------------------
    PLACEMENT_GROUP_ID                    NAME      CREATOR_JOB_ID  STATE
0  3cd6174711f47c14132155039c0501000000                  01000000  CREATED
1  e1b043bebc751c3081bddc24834d01000000                  01000000  PENDING <---- the new placement group.
```

You can also use the `ray status` CLI command to verify that Ray can't allocate the `{"CPU": 1, "GPU": 2}` bundles.

```bash
ray status
```

```bash
Resources
---------------------------------------------------------------
Usage:
0.0/2.0 CPU (0.0 used of 1.0 reserved in placement groups)
0.0/2.0 GPU (0.0 used of 1.0 reserved in placement groups)
0B/3.46GiB memory
0B/1.73GiB object_store_memory

Demands:
{'CPU': 1.0} * 1, {'GPU': 2.0} * 1 (PACK): 1+ pending placement groups <--- 1 placement group is pending creation.
```

This cluster has `{"CPU": 2, "GPU": 2}`. You already created a `{"CPU": 1, "GPU": 1}` bundle, so the cluster has only 1 CPU and 1 GPU left. If you try to schedule a placement group with the two bundles `{"CPU": 1}, {"GPU": 2}`, Ray doesn't create the placement group and doesn't reserve any resources, including the `{"CPU": 1}` bundle.

```{image} ../images/pg_image_2.png
:align: center
```

A placement group that Ray can't schedule in any way is *infeasible*. For example, suppose you schedule a `{"CPU": 4}` bundle, but you have only a single node with 2 CPUs. There's no way to create this bundle in your cluster. The Ray autoscaler is aware of placement groups, and autoscales the cluster to ensure that pending groups can be placed as needed.

If the autoscaler can't provide resources to schedule a placement group, Ray doesn't print a warning about infeasible groups or about the tasks and actors that use them. You can observe the scheduling state of the placement group from the {ref}`dashboard or state APIs <ray-placement-group-observability-ref>`.

:::{note}
When Ray successfully reserves a placement group with GPUs, the bundles aren't necessarily ordered by physical GPU rank. Adjacent bundles don't necessarily map to adjacent physical GPUs.
:::

(ray-placement-group-schedule-tasks-actors-ref)=

## Schedule tasks and actors to placement groups (use reserved resources)

In the previous section, you created a placement group that reserved `{"CPU": 1, "GPU": 1}` from a node with 2 CPUs and 2 GPUs.

Next, schedule an actor to the placement group. To schedule actors or tasks to a placement group, use {class}`options(scheduling_strategy=PlacementGroupSchedulingStrategy(...)) <ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy>`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __schedule_pg_start__
:end-before: __schedule_pg_end__
```
:::

:::{tab-item} Java
```java
public static class Counter {
  private int value;

  public Counter(int initValue) {
    this.value = initValue;
  }

  public int getValue() {
    return value;
  }

  public static String ping() {
    return "pong";
  }
}

// Create GPU actors on a gpu bundle.
for (int index = 0; index < 1; index++) {
  Ray.actor(Counter::new, 1)
    .setPlacementGroup(pg, 0)
    .remote();
}
```
:::

:::{tab-item} C++
```c++
class Counter {
public:
  Counter(int init_value) : value(init_value){}
  int GetValue() {return value;}
  std::string Ping() {
    return "pong";
  }
private:
  int value;
};

// Factory function of Counter class.
static Counter *CreateCounter() {
  return new Counter();
};

RAY_REMOTE(&Counter::Ping, &Counter::GetValue, CreateCounter);

// Create GPU actors on a gpu bundle.
for (int index = 0; index < 1; index++) {
  ray::Actor(CreateCounter)
    .SetPlacementGroup(pg, 0)
    .Remote(1);
}
```
:::
::::

:::{note}
By default, Ray actors require 1 logical CPU at schedule time, but after Ray schedules them, they don't acquire any CPU resources. In other words, by default, Ray can't schedule actors on a zero-CPU node, but an infinite number of them can run on any non-zero CPU node. When you schedule an actor with the default resource requirements and a placement group, you must create the placement group with a bundle that contains at least 1 CPU, because the actor requires 1 CPU for scheduling. After Ray creates the actor, the actor doesn't consume any placement group resources.

To avoid surprises, always specify resource requirements explicitly for actors. If you specify resources explicitly, they're required both at schedule time and at execution time.
:::

Ray schedules the actor. Multiple tasks and actors can use one bundle, so a bundle has a one-to-many relationship with tasks and actors. In this case, because the actor uses 1 CPU, 1 GPU remains from the bundle. Verify this with the `ray status` CLI command. The output shows that the placement group reserves 1 CPU and that the actor you created uses 1.0 of it.

```bash
ray status
```

```bash
Resources
---------------------------------------------------------------
Usage:
1.0/2.0 CPU (1.0 used of 1.0 reserved in placement groups) <---
0.0/2.0 GPU (0.0 used of 1.0 reserved in placement groups)
0B/4.29GiB memory
0B/2.00GiB object_store_memory

Demands:
(no resource demands)
```

You can also verify that Ray created the actor by using `ray list actors`.

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list actors --detail
```

```bash
-   actor_id: b5c990f135a7b32bfbb05e1701000000
    class_name: Actor
    death_cause: null
    is_detached: false
    job_id: '01000000'
    name: ''
    node_id: b552ca3009081c9de857a31e529d248ba051a4d3aeece7135dde8427
    pid: 8795
    placement_group_id: d2e660ac256db230dbe516127c4a01000000 <------
    ray_namespace: e5b19111-306c-4cd8-9e4f-4b13d42dff86
    repr_name: ''
    required_resources:
        CPU_group_d2e660ac256db230dbe516127c4a01000000: 1.0
    serialized_runtime_env: '{}'
    state: ALIVE
```

Because 1 GPU remains, create a new actor that requires 1 GPU. This time, also specify the `placement_group_bundle_index`. Each bundle has an index within the placement group. For example, a placement group of two bundles `[{"CPU": 1}, {"GPU": 1}]` has bundle `{"CPU": 1}` at index 0 and bundle `{"GPU": 1}` at index 1. The placement group you created earlier has only one bundle, so it has only index 0. If you don't specify a bundle, Ray schedules the actor or task on a random bundle that has unallocated reserved resources.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __schedule_pg_3_start__
:end-before: __schedule_pg_3_end__
```
:::
::::

Ray successfully schedules the GPU actor. The following image shows the two actors scheduled into the placement group.

```{image} ../images/pg_image_3.png
:align: center
```

You can also use the `ray status` command to verify that all the reserved resources are in use.

```bash
ray status
```

```bash
Resources
---------------------------------------------------------------
Usage:
1.0/2.0 CPU (1.0 used of 1.0 reserved in placement groups)
1.0/2.0 GPU (1.0 used of 1.0 reserved in placement groups) <----
0B/4.29GiB memory
0B/2.00GiB object_store_memory
```

(pgroup-strategy)=

## Placement strategy

Placement groups can add placement constraints among bundles.

For example, you might want to pack your bundles onto the same node, or spread them out across multiple nodes as much as possible. Specify the strategy with the `strategy` argument. This way, you can make sure that Ray schedules your actors and tasks with certain placement constraints.

The following example creates a placement group of two bundles with a PACK strategy, so both bundles have to be created on the same node. PACK is a soft policy. If Ray can't pack the bundles onto a single node, it spreads them to other nodes. To avoid this problem, use the `STRICT_PACK` policy instead, which fails to create the placement group if Ray can't satisfy the placement requirements.

```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __strategy_pg_start__
:end-before: __strategy_pg_end__
```

The following image shows the PACK policy. Three of the `{"CPU": 2}` bundles are on the same node.

```{image} ../images/pg_image_4.png
:align: center
```

The following image shows the SPREAD policy. Each of the three `{"CPU": 2}` bundles is on a different node.

```{image} ../images/pg_image_5.png
:align: center
```

Ray supports four placement group strategies. The default scheduling policy is `PACK`.

**STRICT_PACK**

All bundles must be placed on a single node in the cluster. Use this strategy when you want to maximize locality.

**PACK**

Ray packs all provided bundles onto a single node on a best-effort basis. If strict packing isn't feasible because some bundles don't fit on the node, Ray can place bundles on other nodes.

**STRICT_SPREAD**

Each bundle must be scheduled on a separate node.

**SPREAD**

Ray spreads the bundles onto separate nodes on a best-effort basis. If strict spreading isn't feasible, Ray can place bundles on overlapping nodes.

## Remove placement groups (free reserved resources)

By default, a placement group's lifetime is scoped to the driver that creates it, unless you make it a {ref}`detached placement group <placement-group-detached>`. When a {ref}`detached actor <actor-lifetimes>` creates the placement group, the lifetime is scoped to the detached actor. In Ray, the driver is the Python script that calls `ray.init`.

Ray automatically frees the placement group's reserved resources, or bundles, when the driver or detached actor that created the placement group exits. To free the reserved resources manually, remove the placement group with the {func}`remove_placement_group <ray.util.remove_placement_group>` API, which is also asynchronous.

:::{note}
When you remove the placement group, Ray forcefully kills the actors or tasks that still use the reserved resources.
:::

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __remove_pg_start__
:end-before: __remove_pg_end__
```
:::

:::{tab-item} Java
```java
PlacementGroups.removePlacementGroup(placementGroup.getId());

PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(placementGroup.getId());
Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);
```
:::

:::{tab-item} C++
```c++
ray::RemovePlacementGroup(placement_group.GetID());

ray::PlacementGroup removed_placement_group = ray::GetPlacementGroup(placement_group.GetID());
assert(removed_placement_group.GetState(), ray::PlacementGroupState::REMOVED);
```
:::
::::

(ray-placement-group-observability-ref)=

## Observe and debug placement groups

Use the following tools to inspect placement group states and resource usage:

- `ray status` is a CLI tool for viewing the resource usage and scheduling resource requirements of placement groups.
- The Ray dashboard is a UI tool for inspecting placement group states.
- The Ray state API is a CLI for inspecting placement group states.

:::::{tab-set}
:::{tab-item} ray status (CLI)
The `ray status` CLI command shows the autoscaling status of the cluster. It shows the resource demands from unscheduled placement groups and the resource reservation status.

```bash
Resources
---------------------------------------------------------------
Usage:
1.0/2.0 CPU (1.0 used of 1.0 reserved in placement groups)
0.0/2.0 GPU (0.0 used of 1.0 reserved in placement groups)
0B/4.29GiB memory
0B/2.00GiB object_store_memory
```
:::

::::{tab-item} Dashboard
The {ref}`dashboard job view <dash-jobs-view>` has a placement group table that displays the scheduling state and metadata of placement groups.

:::{note}
The Ray dashboard is available only when you install Ray with `pip install "ray[default]"`.
:::
::::

::::{tab-item} Ray State API
The {ref}`Ray state API <state-api-overview-ref>` is a CLI tool for inspecting the state of Ray resources, such as tasks, actors, and placement groups.

`ray list placement-groups` shows the metadata and scheduling state of placement groups. `ray list placement-groups --detail` shows statistics and scheduling state in greater detail.

:::{note}
The state API is available only when you install Ray with `pip install "ray[default]"`.
:::
::::
:::::

### Inspect placement group scheduling state

With the preceding tools, you can see the state of the placement group. The following files define the states:

- [High-level state](https://github.com/ray-project/ray/blob/03a9d2166988b16b7cbf51dac0e6e586455b28d8/src/ray/protobuf/gcs.proto#L579)
- [Details](https://github.com/ray-project/ray/blob/03a9d2166988b16b7cbf51dac0e6e586455b28d8/src/ray/protobuf/gcs.proto#L524)

```{image} ../images/pg_image_6.png
:align: center
```

## [Advanced] Child tasks and actors

By default, child actors and tasks don't use the parent's placement group. To automatically schedule child actors or tasks to the same placement group, set `placement_group_capture_child_tasks` to `True`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_capture_child_tasks_example.py
:language: python
:start-after: __child_capture_pg_start__
:end-before: __child_capture_pg_end__
```
:::

:::{tab-item} Java
It's not implemented for Java APIs yet.
:::
::::

If `placement_group_capture_child_tasks` is `True` but you don't want to schedule child tasks and actors to the same placement group, specify `PlacementGroupSchedulingStrategy(placement_group=None)`.

```{literalinclude} ../doc_code/placement_group_capture_child_tasks_example.py
:language: python
:start-after: __child_capture_disable_pg_start__
:end-before: __child_capture_disable_pg_end__
```


## [Advanced] Named placement group

Within a {ref}`namespace <namespaces-guide>`, you can *name* a placement group. Use the name to retrieve the placement group from any job in the Ray cluster, as long as the job is in the same namespace. Naming is useful if you can't pass the placement group handle directly to the actor or task that needs it, or if you're trying to access a placement group that another driver launched.

If a placement group's lifetime isn't `detached`, Ray destroys the placement group when the job that created it completes. To avoid this, use a {ref}`detached placement group <placement-group-detached>`.

This feature requires that you specify a {ref}`namespace <namespaces-guide>` associated with the placement group. Otherwise, you can't retrieve the placement group across jobs.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __get_pg_start__
:end-before: __get_pg_end__
```
:::

:::{tab-item} Java
```java
// Create a placement group with a unique name.
Map<String, Double> bundle = ImmutableMap.of("CPU", 1.0);
List<Map<String, Double>> bundles = ImmutableList.of(bundle);

PlacementGroupCreationOptions options =
  new PlacementGroupCreationOptions.Builder()
    .setBundles(bundles)
    .setStrategy(PlacementStrategy.STRICT_SPREAD)
    .setName("global_name")
    .build();

PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
pg.wait(60);

...

// Retrieve the placement group later somewhere.
PlacementGroup group = PlacementGroups.getPlacementGroup("global_name");
Assert.assertNotNull(group);
```
:::

:::{tab-item} C++
```c++
// Create a placement group with a globally unique name.
std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1.0}}};

ray::PlacementGroupCreationOptions options{
    true/*global*/, "global_name", bundles, ray::PlacementStrategy::STRICT_SPREAD};

ray::PlacementGroup pg = ray::CreatePlacementGroup(options);
pg.Wait(60);

...

// Retrieve the placement group later somewhere.
ray::PlacementGroup group = ray::GetGlobalPlacementGroup("global_name");
assert(!group.Empty());
```

The C++ API also supports non-global named placement groups. A non-global placement group name is valid only within its job, and you can't access the placement group from another job.

```c++
// Create a placement group with a job-scope-unique name.
std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1.0}}};

ray::PlacementGroupCreationOptions options{
    false/*non-global*/, "non_global_name", bundles, ray::PlacementStrategy::STRICT_SPREAD};

ray::PlacementGroup pg = ray::CreatePlacementGroup(options);
pg.Wait(60);

...

// Retrieve the placement group later somewhere in the same job.
ray::PlacementGroup group = ray::GetPlacementGroup("non_global_name");
assert(!group.Empty());
```
:::
::::

(placement-group-detached)=

## [Advanced] Detached placement group

By default, a placement group's lifetime belongs to the driver or actor that creates it:

- If a driver creates the placement group, the placement group is destroyed when the driver terminates.
- If a detached actor creates the placement group, the placement group is killed when the detached actor is killed.

To keep the placement group alive regardless of its job or detached actor, specify `lifetime="detached"`, as in the following example:

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/placement_group_example.py
:language: python
:start-after: __detached_pg_start__
:end-before: __detached_pg_end__
```
:::

:::{tab-item} Java
The lifetime argument isn't implemented for Java APIs yet.
:::
::::

Stop the script and start a new Python script. Call `ray list placement-groups`, and you can see that the placement group isn't removed.

Ray decouples the lifetime option from the name option. If you specify only the name without `lifetime="detached"`, you can retrieve the placement group only while the driver that created it is still running. Always specify a name when you create a detached placement group. Otherwise, there's no way to retrieve the placement group from another process, and no way to kill it after you exit the driver script that created it.


(ray-placement-group-ft-ref)=

## [Advanced] Fault tolerance

### Rescheduling bundles on a dead node

If nodes that contain some bundles of a placement group die, Ray tries to reschedule the lost bundles on different nodes. The initial creation of a placement group is atomic, but after the initial creation, there could be partial placement groups. Actors or tasks running on bundles on the remaining live nodes continue to run. Ray gives the bundles it reschedules higher scheduling priority than other placement group scheduling.

### Provide resources for partially lost bundles

If there aren't enough resources to schedule the partially lost bundles, the placement group waits, assuming the Ray autoscaler starts a new node to satisfy the resource requirements. If the autoscaler can't provide additional resources or if you're not using the autoscaler, the placement group remains in the partially created state indefinitely.

### Fault tolerance of actors and tasks that use the bundle

After Ray recovers the bundle, it reschedules the actors and tasks that use the bundle's reserved resources, based on their {ref}`fault tolerance policy <fault-tolerance>`.

(pgroup-topology-strategy)=

## [Alpha] Topology strategy scheduling

:::{warning}
Topology strategy scheduling is an alpha feature. It is under active development, and the API surface might change. Ray currently supports defining only one topology label and one node-level strategy, as the following sections describe. For topology labels, Ray currently supports only `STRICT_PACK`. Support for additional strategies and multi-level topologies is planned.
:::

### Why topology strategy scheduling?

The preceding placement strategies, PACK, STRICT_PACK, SPREAD, and STRICT_SPREAD, operate purely on a per-node basis. For multi-node GPU domains, such as GB200 or GB300 NVL racks where nodes share fast interconnects, there's no native way to ensure that all bundles land within the same GPU domain.

For example, consider a cluster with two racks of 18 nodes each, where each node has `{"GPU": 4, "CPU": 2}`. You want to schedule `[{"GPU": 4, "CPU": 2}] * 18` within a single rack:

- **STRICT_PACK** tries to place all 18 bundles onto a single *node*, which is infeasible because a single node has only 4 GPUs and 2 CPUs.
- **PACK** spreads bundles across nodes, but it has no concept of racks, and bundles might land on nodes across *both* racks.

You could work around this with static {ref}`label selectors <labels>`, such as `bundle_label_selector=[{"my_custom_gpu_domain_label": "rack-1"}] * 18`, but that approach doesn't support fault tolerance. If all nodes in `rack-1` go down, the placement group can't automatically move to a different rack. You also have to specify a domain manually when you want any domain, which becomes cumbersome if you have many GPU domains.

Topology strategy scheduling solves this problem. You express a topology strategy for the placement group, which specifies a topology label within the cluster. Ray picks a value for this topology label that can satisfy all bundles, such as a specific rack, and then applies your node-level strategy within that value.

### How it works

To use topology strategy placement, pass `topology_strategy=` to {func}`ray.util.placement_group`. The argument is a dict that maps a label key to the placement strategy for that level.

Currently, `topology_strategy` is a dictionary that can contain up to two keys:

- The special key `ray.io/node-id` sets the **node-level** strategy and accepts any value in `{"PACK", "STRICT_PACK", "SPREAD", "STRICT_SPREAD"}`. If you omit it, the node-level strategy defaults to `PACK`.
- Any other key is a **topology label** that you set on nodes through `ray start --labels` or your cluster configuration. Ray currently supports only `STRICT_PACK` for these labels.

```python
from ray.util.placement_group import placement_group

bundles = [{"GPU": 4, "CPU": 2}] * 18

pg = placement_group(
    bundles=bundles,
    topology_strategy={"ray.io/gpu-domain": "STRICT_PACK"},
)

ray.get(pg.ready())
```

With this configuration, Ray does the following:

1. Groups candidate nodes by the value of the topology label you named, which is `ray.io/gpu-domain` in the preceding example.
1. Selects a value for that label that can satisfy all bundles.
1. Applies the node-level scheduling strategy within the selected value.

The following example uses STRICT_SPREAD to spread bundles across distinct nodes and STRICT_PACK to keep them on a single rack, where `ray.io/gpu-domain` is each rack's topology label:

```python
pg = placement_group(
    bundles=[{"CPU": 1}] * 4,
    topology_strategy={
        "ray.io/node-id": "STRICT_SPREAD",
        "ray.io/gpu-domain": "STRICT_PACK",
    },
)
```

`topology_strategy` is mutually exclusive with the `strategy=` parameter, and passing both raises `ValueError`. To override the default node-level strategy alongside a topology label, put the node-level strategy under `ray.io/node-id` in the same dict, as the preceding example shows.

:::{note}
Ray doesn't automatically set topology labels such as `ray.io/gpu-domain` on nodes. Configure these labels through `ray start --labels` or your cluster configuration, as in the following example:

```bash
ray start --labels="ray.io/gpu-domain=rack-1"
```

**Using with Kubernetes**

Many GB200 and GB300 clusters use Kubernetes as their scheduler. The NVIDIA GPU Operator exposes an identifier for each NVLink domain with the node label `nvidia.com/gpu.clique` from GPU Feature Discovery.

If your Ray workers run in Pods, you can use the Kubernetes Downward API to set an environment variable such as `NVIDIA_GPU_CLIQUE` to the value of the `nvidia.com/gpu.clique` node label, which enables the NVLink domain-aware placement groups feature.

For example, a Ray worker's start command might look like this:

```bash
# Inside each Pod, start the worker using the label value
ray start \
  --address="${RAY_HEAD_ADDRESS}" \
  --num-gpus=4 \
  --labels="ray.io/accelerator-type=GB300,ray.io/gpu-domain=${NVIDIA_GPU_CLIQUE}"
```
:::

### Fault tolerance

Topology strategy scheduling improves on static label selectors by providing automatic fault tolerance at the topology-label level:

- **Partial failure**: Some nodes within the selected value of the topology label die. Ray reschedules the lost bundles onto surviving nodes within the same value, such as the same rack. Actors and tasks on the remaining bundles keep running. If the selected value doesn't have enough resources to reschedule the lost bundles, those bundles stay infeasible and queued until resources free up in the same value. To force the placement group onto a different value, call {func}`ray.util.remove_placement_group <ray.util.remove_placement_group>` and create a new one. Removing the placement group forcefully kills every actor and task still using its bundles and doesn't restart them, so you must re-create them yourself on the new placement group.
- **Total failure**: All nodes with the selected value die. Ray clears the topology assignment and reschedules the entire placement group onto a different value.

### Observability

You can inspect topology strategy placement groups using the existing placement group observability tools:

- **Dashboard**: The placement group table shows a `Topology` column, which displays the strategy you requested and the value Ray selected for each topology label.
- **State API**: `ray list placement-groups --detail` returns the requested strategy in `topology_strategy` and the value Ray selected in `topology_assignments`.

The following `ray list placement-groups --detail` output shows the two topology fields, `topology_strategy` and `topology_assignments`, populated for a placement group that packs onto a single `ray.io/gpu-domain`:

```yaml
- placement_group_id: 237f47c3235ac1a96ad423c3f74501000000
  name: gpu-domain-pg
  state: CREATED
  bundles:
  - bundle_id:
      placement_group_id: 237f47c3235ac1a96ad423c3f74501000000
      bundle_index: 0
    unit_resources:
      CPU: 1.0
    node_id: 0fd7eecf6335633ba39ab66f5a26b18eeb35c70c15a9563a29ee2bce
  - bundle_id:
      placement_group_id: 237f47c3235ac1a96ad423c3f74501000000
      bundle_index: 1
    unit_resources:
      CPU: 1.0
    node_id: 0fd7eecf6335633ba39ab66f5a26b18eeb35c70c15a9563a29ee2bce
  is_detached: false
  stats: ...
  topology_strategy:
  - entries:
      ray.io/gpu-domain: STRICT_PACK
  topology_assignments:
  - assignments:
      ray.io/gpu-domain: rack-2
```

For placement groups that don't use a topology strategy, `topology_strategy` and `topology_assignments` are both empty lists. Both fields appear only when you pass `--detail`.

## API reference

See the {ref}`placement group API reference <ray-placement-group-ref>`.
