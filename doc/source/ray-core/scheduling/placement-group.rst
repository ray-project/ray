Placement Groups
================

.. _ray-placement-group-doc-ref:

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling).
They can be then used to schedule Ray tasks and actors packed as close as possible for locality (PACK), or spread apart 
(SPREAD). Placement groups are generally used for gang-scheduling actors, but also support tasks.

Here are some real-world use cases:

- **Distributed Machine Learning Training**: Distributed Training (e.g., :ref:`Ray Train <train-docs>`) uses the placement group APIs enables gang scheduling by reserving a group of resources across Ray cluster. Gang scheduling is a critical technique to enable all-or-nothing scheduling for deep learning training. 
- **Hyperparameter Tuning**: Hyperparameter tuning (e.g., :ref:`Ray Tune <tune-main>`) uses the placement group APIs to reserve a group of resources for each trial, which mitigates the risk of resource deadlock among trials (In a given machine learning training task, each trial only has a partial schedule of training workers allocated to it. As a result, none of the trials can fully schedule all of the available workers for training).

Key Concepts
------------

Bundles
~~~~~~~

A **bundle** is a collection of "resources". It could be a single resource, ``{"CPU": 1}``, or a group of resources, ``{"CPU": 1, "GPU": 4}``. 
A bundle is a unit of reservation for placement groups. "Scheduling a bundle" means we find a node that fits the bundle and reserve the resources specified by the bundle. 
A bundle must be able to fit on a single node on the Ray cluster. For example, if you only have an 8 CPU node, and if you have a bundle that requires ``{"CPU": 9}``, this bundle cannot be scheduled.

Placement Group
~~~~~~~~~~~~~~~

A **placement group** reserves the resources from the cluster. The reserved resources can only be used by tasks or actors that use the :ref:`PlacementGroupSchedulingStrategy <ray-placement-group-schedule-tasks-actors-ref>`.

- Placement groups are represented by a list of bundles. For example, ``{"CPU": 1} * 4`` means you'd like to reserve 4 bundles of 1 CPU (i.e., it reserves 4 CPUs).
- Bundles are then placed according to the :ref:`placement strategies <pgroup-strategy>` across nodes on the cluster.
- After the placement group is created, tasks or actors can be then scheduled according to the placement group and even on individual bundles.

Create a Placement Group (Reserve Resources)
--------------------------------------------

You can create a placement group using :func:`ray.util.placement_group() <ray.util.placement_group.placement_group>`. 
Placement groups take in a list of bundles and a :ref:`placement strategy <pgroup-strategy>`. 
Note that each bundle must be able to fit on a single node on the Ray cluster.
For example, if you only have a 8 CPU node, and if you have a bundle that requires ``{"CPU": 9}``,
this bundle cannot be scheduled.

Bundles are specified by a list of dictionaries, e.g., ``[{"CPU": 1}, {"CPU": 1, "GPU": 1}]``).

- ``CPU`` corresponds to ``num_cpus`` as used in :func:`ray.remote <ray.remote>`.
- ``GPU`` corresponds to ``num_gpus`` as used in :func:`ray.remote <ray.remote>`.
- ``memory`` corresponds to ``memory`` as used in :func:`ray.remote <ray.remote>`
- Other resources corresponds to ``resources`` as used in :func:`ray.remote <ray.remote>` (E.g., ``ray.init(resources={"disk": 1})`` can have a bundle of ``{"disk": 1}``).

Placement group scheduling is asynchronous. The `ray.util.placement_group` returns immediately.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __create_pg_start__
        :end-before: __create_pg_end__


.. tabbed:: Java

    .. code-block:: java

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

.. tabbed:: C++

    .. code-block:: c++

      // Initialize Ray.
      ray::Init();

      // Construct a list of bundles.
      std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1.0}}};

      // Make a creation option with bundles and strategy.
      ray::internal::PlacementGroupCreationOptions options{
          false, "my_pg", bundles, ray::internal::PlacementStrategy::PACK};

      ray::PlacementGroup pg = ray::CreatePlacementGroup(options);

You can block your program until the placement group is ready using the `ready` (compatible with ``ray.get``) or `wait` (block the program until the placement group is ready) API. 
**It is recommended to verify placement groups are ready** before using them to schedule tasks and actors. 

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __ready_pg_start__
        :end-before: __ready_pg_end__

.. tabbed:: Java

    .. code-block:: java

      // Wait for the placement group to be ready within the specified time(unit is seconds).
      boolean ready = pg.wait(60);
      Assert.assertTrue(ready);

      // You can look at placement group states using this API.
      List<PlacementGroup> allPlacementGroup = PlacementGroups.getAllPlacementGroups();
      for (PlacementGroup group: allPlacementGroup) {
        System.out.println(group);
      }

.. tabbed:: C++

    .. code-block:: c++

      // Wait for the placement group to be ready within the specified time(unit is seconds).
      bool ready = pg.Wait(60);
      assert(ready);

      // You can look at placement group states using this API.
      std::vector<ray::PlacementGroup> all_placement_group = ray::GetAllPlacementGroups();
      for (const ray::PlacementGroup &group : all_placement_group) {
        std::cout << group.GetName() << std::endl;
      }

SANG-TODO image 1

Placement groups are atomically created - meaning that if there exists a bundle that cannot fit in any of the current nodes, 
then the entire placement group will not be ready and no resources are reserved.
To see what this means, let's create another placement group that requires ``{"CPU":1}, {"GPU": 2}`` (2 bundles). The current cluster has 
``{"CPU": 2, "GPU": 2}``. We already created a ``{"CPU": 1, "GPU": 1}`` bundle, so there's only ``{"CPU": 1, "GPU": 1}`` left in the cluster.
If we create 2 bundles ``{"CPU": 1}, {"GPU": 2}``, we can create a first bundle successfully, but can't schedule the second bundle.
Since we cannot create every bundle to the cluster, placement group won't be created including the ``{"CPU": 1}`` bundle.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __create_pg_failed_start__
        :end-before: __create_pg_failed_end__

SANG-TODO images 2

If there are not enough resources to create a placement group, it is in the pending state.

When the placement group cannot be scheduled in any way, it is called "infeasible". 
Imagine you schedule ``{"CPU": 4}`` bundle, but you only have a single node with 2 CPUs. There's no way to create this bundle in your cluster.
The Ray Autoscaler is aware of placement groups, and auto-scale the cluster to ensure pending groups can be placed as needed. 

If Ray Autoscaler cannot provide resources to schedule a placement group, Ray does *not* print a warning about infeasible groups and tasks and actors that use the groups. 
You can observe the scheduling state of the placement group from the :ref:`dashboard or state APIs <ray-placement-group-observability-ref>`.

.. _ray-placement-group-schedule-tasks-actors-ref:

Schedule Tasks and Actors to Placement Groups (Use Reserved Resources)
----------------------------------------------------------------------

In the previous section, we created a placement group that reserves ``{"CPU": 1, "GPU: 1"}`` from a 2 CPU and 2 GPU node.

Now let's schedule an actor to the placement group. 
You can schedule actors/tasks to the placement group using
:class:`options(scheduling_strategy=PlacementGroupSchedulingStrategy(...)) <ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy>`.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __schedule_pg_start__
        :end-before: __schedule_pg_end__

.. tabbed:: Java

    .. code-block:: java

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

.. tabbed:: C++

    .. code-block:: c++

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

In Ray, actor requires 1 CPU to be scheduled, and once it is created, it occupies 0 CPU.
Since the placement group has a reserved ``{"CPU": 1, "GPU" 1}`` bundle, the actor can be scheduled onto this bundle.
After the previous actor is created, we have remaining ``{"CPU": 1, "GPU": 1}`` from this bundle because the actor uses 0 CPU.
Let's create another actor to this bundle. This time we explicitly specify actor requires 1 CPU.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __schedule_pg_2_start__
        :end-before: __schedule_pg_2_end__

Actor is scheduled now! Each bundle can be used by multiple tasks and actors. 
In this case, since the actor uses 1 CPU, there's remaining 1 GPU from the bundle. 
You can verify this from the CLI command ``ray status``.

.. code-block:: bash

  ray status

.. code-block:: bash

  SANG-TODO.

Since we have a remaining 1 GPU, let's create a new actor that requires 1 GPU.
This time, we will also specify the ``placement_group_bundle_index``. Each bundle is given an "index" within the placement group.
E.g., A placement group of 2 bundles ``[{"CPU": 1}, {"GPU": 1}]`` will have index 0 bundle ``{"CPU": 1}`` 
and index 1 bundle ``{"GPU": 1}``. Since we only have 1 bundle, we will only have index 0. If you don't specify a bundle, the actor/task
will be scheduled on a random bundle that have the unallocated reserved resources.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __schedule_pg_3_start__
        :end-before: __schedule_pg_3_end__

We succeed to schedule the GPU actor! The below image describes 3 actors scheduled into the placement group. 

SANG-TODO image 3

You can also verify the reserved resources are all used from the ``ray status`` command.

.. code-block:: bash

  ray status

.. code-block:: bash

  SANG-TODO.

.. note::

  By default, Ray task requires 1 CPU and Ray actor uses 0 CPU. But an actor temporarily uses 1 CPU to be "placed", meaning it uses 1 CPU until it is scheduled.

Remove Placement Groups (Free Reserved Resources)
-------------------------------------------------

By default, a placement group's lifetime is scoped to a driver that creates a placement group 
(unless you make it a :ref:`detached placement group <placement-group-detached>`). When the placement group is created from
a :ref:`detached actor <actor-lifetimes>`, the lifetime is scoped to a detached actor.
In Ray, driver means the Python script that calls ``ray.init``.

Reserved resources (bundles) from the placement group is automatically freed when a driver or detached actor
that creates placement group exits. If you'd like to free the reserved resources manually, you can also remove the placement
group using :func:`remove_placement_group <ray.util.remove_placement_group>` API (note that it is also an asynchronous API).

.. note::

  When you remove the placement group, actors or tasks that still use the reserved resources will be
  forcefully killed.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __remove_pg_start__
        :end-before: __remove_pg_end__

.. tabbed:: Java

    .. code-block:: java

      PlacementGroups.removePlacementGroup(placementGroup.getId());

      PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(placementGroup.getId());
      Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);

.. tabbed:: C++

    .. code-block:: c++

      ray::RemovePlacementGroup(placement_group.GetID());

      ray::PlacementGroup removed_placement_group = ray::GetPlacementGroup(placement_group.GetID());
      assert(removed_placement_group.GetState(), ray::PlacementGroupState::REMOVED);

.. _pgroup-strategy:

Placement Strategy
------------------

Often, you'd like to reserve bundles with placement constraints. For example, you'd like to pack your bundles to the same
node or spread out to multiple nodes as much as possible. You can specify the strategy via ``strategy`` argument. The below
example create a placement group with 2 bundles with a STRICT_PACK strategy, meaning both bundle has to be created in the
same node, otherwise the placement group cannot be created.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __strategy_pg_start__
        :end-before: __strategy_pg_end__

Ray currently supports the following placement group strategies. The default scheduling policy is ``PACK``:

**STRICT_PACK**

All bundles must be placed into a single node on the cluster. It is useful when you want to maximize the locality.

**PACK**

All provided bundles are packed onto a single node on a best-effort basis.
If strict packing is not feasible (i.e., some bundles do not fit on the node), bundles can be placed onto other nodes nodes.

**STRICT_SPREAD**

Each bundle must be scheduled in a separate node.

**SPREAD**

Each bundle will be spread onto separate nodes on a best effort basis.
If strict spreading is not feasible, bundles can be placed overlapping nodes.

.. _ray-placement-group-observability-ref:

Observe and Debug Placement Groups
----------------------------------

Ray provides several useful tools to inspect the placement group states and resource usage.

- **Ray Status** is a CLI tool to see the resource usage and the scheduling resource requirement of the placement groups.
- **Ray Dashboard** is a UI tool to inspect placement group states.
- **Ray State API** is a CLI to inspect placement group states.

.. tabbed:: ray status (CLI)

  The CLI command ``ray status`` provides the autoscaling status of the cluster. 
  It provides the "resource demands" from unscheduled placement groups as well as the resource reservation status.

  SANG-TODO images

.. tabbed:: Dashboard

  :ref:`The dashboard job view <dash-jobs-view>` provides the placement group table that displays the scheduling state and metadata of the placement group.

  .. note::

    Ray dashboard is only available when Ray is installed with ``pip install "ray[default]"``.

.. tabbed:: Ray State API

  :ref:`Ray state API <state-api-overview-ref>` is a CLI tool to inspect the state of Ray resources (tasks, actors, placement groups, etc.). 

  ``ray list placement-groups`` provides the metadata and the scheduling state of the placement group.
  ``ray list placement-groups --detail`` provides stats and scheduling state in a greater detail.

  .. note::

    State API is only available when Ray is installed with ``pip install "ray[default]"``

Inspect Placement Group Scheduling State
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

From the above tools, you can see the state of the placement group. The definition of states are specified in the following files.

- `High level state <https://github.com/ray-project/ray/blob/03a9d2166988b16b7cbf51dac0e6e586455b28d8/src/ray/protobuf/gcs.proto#L579>`_
- `Details <https://github.com/ray-project/ray/blob/03a9d2166988b16b7cbf51dac0e6e586455b28d8/src/ray/protobuf/gcs.proto#L524>`_

SANG-TODO Diagrams

[Advanced] Child Tasks and Actors
---------------------------------

By default, child actors and tasks don't share the same placement group that the parent uses.
If you'd like to automatically schedule child actors/tasks to the same placement group,
set ``placement_group_capture_child_tasks`` to True.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_capture_child_tasks_example.py
      :language: python
      :start-after: __child_capture_pg_start__
      :end-before: __child_capture_pg_end__

.. tabbed:: Java

    It's not implemented for Java APIs yet.

When ``placement_group_capture_child_tasks`` is True, but you still don't want to schedule
your child tasks and actors to the same placement group, you should specify ``PlacementGroupSchedulingStrategy(placement_group=None)``.

.. literalinclude:: ../doc_code/placement_group_capture_child_tasks_example.py
  :language: python
  :start-after: __child_capture_disable_pg_start__
  :end-before: __child_capture_disable_pg_end__

[Advanced] Named Placement Group
--------------------------------

A placement group can be given a globally unique name.
This allows you to retrieve the placement group from any job in the Ray cluster.
This can be useful if you cannot directly pass the placement group handle to
the actor or task that needs it, or if you are trying to
access a placement group launched by another driver.
Note that the placement group will still be destroyed if it's lifetime isn't `detached`.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __get_pg_start__
        :end-before: __get_pg_end__

.. tabbed:: Java

    .. code-block:: java

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

.. tabbed:: C++

    .. code-block:: c++

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

    We also support non-global named placement group in C++, which means that the placement group name is only valid within the job and cannot be accessed from another job.

    .. code-block:: c++

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

.. _placement-group-detached:

[Advanced] Detached Placement Group
-----------------------------------

By default, the lifetimes of placement groups belong to the driver and actor.

- If the placement group is created from a driver, it will be destroyed when the driver is terminated.
- If it is created from a detached actor, it is killed when the detached actor is killed.

If you'd like to keep the placement group alive regardless of its job or detached actor, you should specify
`lifetime="detached"`. For example:

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_example.py
        :language: python
        :start-after: __detached_pg_start__
        :end-before: __detached_pg_end__

.. tabbed:: Java

    The lifetime argument is not implemented for Java APIs yet.

Let's terminate the current script and start a new Python script. Call ``ray list placement-groups``, and you can see the placement group is not removed.

Note that the lifetime option is decoupled from the name. If we only specified
the name without specifying ``lifetime="detached"``, then the placement group can
only be retrieved as long as the original driver is still running.
It is recommended to always specify the name when creating the detached placement group.

[Advanced] Fault Tolerance
--------------------------

.. _ray-placement-group-ft-ref:

Rescheduling Bundles on a Dead Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If nodes that contain some bundles of a placement group die, all the bundles will be rescheduled on different nodes by 
GCS (i.e., we try reserving resources again). This means that the initial creation of placement group is "atomic", 
but once it is created, there could be partial placement groups. 
Rescheduling bundles will have the higher scheduling priority than other placement group scheduling.

Provide Resources for Partially Lost Bundles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If there are not enough resources to schedule the partially lost bundles, 
the placement group waits assuming Ray autoscaler will start a new node to satisfy the resource requirements. 
If the additinoal resources cannot be provided (e.g., you don't use the autoscaler or the autoscaler hits the resource limit), 
the placement group remains the partially created state indefinitely.

Fault Tolerance of Actors and Tasks that Use the Bundle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Actors/tasks that use the bundle (reserved resources) will be rescheduled based on their :ref:`fault tolerant policy <fault-tolerance>` once the
bundle is recovered.

API Reference
-------------
:ref:`Placement Group API reference <ray-placement-group-ref>`
