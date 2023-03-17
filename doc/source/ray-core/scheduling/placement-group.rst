Placement Groups
================

.. _ray-placement-group-doc-ref:

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling).
They can be then used to schedule Ray tasks and actors packed as close as possible for locality (PACK), or spread apart 
(SPREAD). Placement groups are generally used for gang-scheduling actors, but also support tasks.

Here are some real-world use cases:

- **Machine Learning Training**: :ref:`Ray Train <train-docs>` uses the placement group APIs enables gang scheduling by reserving a group of resources across Ray cluster. Gang scheduling is a critical technique to enable all-or-nothing scheduling for deep learning training. 
- **Hyperparameter Tuning**: :ref:`Ray Tune <tune-main>` uses the placement group APIs to reserve a group of resources for each trial, which mitigates the risk of resource deadlock among trials (In a given machine learning training task, each trial only has a partial schedule of training workers allocated to it. As a result, none of the trials can fully schedule all of the available workers for training).

Key Concepts
------------

Bundles
~~~~~~~

A **bundle** is a collection of "resources". It could be a single resource, ``{"CPU": 1}``, or a group of resources, ``{"CPU": 1, "GPU": 4}``. 
A bundle is a unit of reservation for placement groups. "Scheduling a bundle" means we find a node that fits the bundle and reserve the resources specified by the bundle. 
A bundle must be able to fit on a single node on the Ray cluster. For example, if you only have an 8 CPU node, and if you have a bundle that requires ``{"CPU": 9}``, this bundle cannot be scheduled.

Placement Group
~~~~~~~~~~~~~~~

A **placement group** reserves the resources from the cluster. The reserved resources can only be used by tasks or actors that use the :ref:`scheduling API <ray-placement-group-schedule-tasks-actors-ref>`, ``PlacementGroupSchedulingStrategy``.

- Placement groups are represented by a list of bundles. For example, ``{"CPU": 1} * 4`` means you'd like to reserve 4 bundles of 1 CPU (i.e., it reserves 4 CPUs).
- Bundles are then placed according to the :ref:`placement strategies <pgroup-strategy>` across nodes on the cluster.
- After the placement group is created, tasks or actors can be then scheduled according to the placement group and even on individual bundles.

Schedule a Placement Group (Reserve Resources)
----------------------------------------------

Ray placement group can be created by the ``ray.util.placement_group`` API. Placement groups take in a list of bundles and a :ref:`placement strategy <pgroup-strategy>`. 
Note that each bundle must be able to fit on a single node on the Ray cluster.
For example, if you only have a 8 CPU node, and if you have a bundle that requires ``{"CPU": 9}``,
this bundle cannot be scheduled.

Bundles are specified by a list of dictionaries, e.g., ``[{"CPU": 1}, {"CPU": 1, "GPU": 1}]``).

- ``CPU`` will correspond with ``num_cpus`` as used in ``ray.remote``
- ``GPU`` will correspond with ``num_gpus`` as used in ``ray.remote``
- Other resources will correspond with ``resources`` as used in ``ray.remote`` (E.g., ``ray.init(resources={"disk": 1})`` can have a bundle of ``{"disk": 1}``).

.. tabbed:: Python

    .. code-block:: python

      # Import placement group APIs.
      from ray.util.placement_group import (
          placement_group,
          placement_group_table,
          remove_placement_group
      )

      # Initialize Ray.
      import ray
      # 2 CPUs and 2 GPUs bundles.
      ray.init(num_cpus=2, num_gpus=2)

      # Reserve a placement group of 1 bundle that reserves 1 CPU and 1 GPU.
      pg = placement_group([{"CPU": 1, "GPU": 1}])

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

Placement group scheduling is asynchronous. The `ray.util.placement_group` returns immediately. You can block your program until
the placement group is ready using the `ready` (compatible with ``ray.get``) or `wait` (block the program until the placement group is ready) API. 
**It is recommended to verify placement groups are ready** before using it to schedule tasks and actors. 

.. tabbed:: Python

    .. code-block:: python

      # Wait until placement group is created.
      ray.get(pg.ready(), timeout=10)

      # You can also use ray.wait.
      ready, unready = ray.wait([pg.ready()], timeout=10)

      # You can look at placement group states using this API.
      print(placement_group_table(pg))

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

Placement groups are atomically created - meaning that if there exists a bundle that cannot fit in any of the current nodes, then the entire placement group will not be ready. 

.. tabbed:: Python

    .. code-block:: python

      import ray
      # 2 CPUs and 2 GPUs bundles.
      ray.init(num_cpus=1, num_gpus=1)

      # The second bundle {GPU: 2} cannot be satisfied. Since the placement group
      # scheduling is atomic this won't be ready until there will be other 
      # node that has more than 2 GPUs.
      pg = placement_group([{"CPU": 1}, {"GPU": 2}], strategy="STRICT_PACK")
      # This will raise the timeout exception!
      ray.get(pg.ready(), timeout=5)

If there's not enough resources to create a placement group, it is in the pending state.

When the placement group cannot be scheduled in any way, it is called "infeasible". 
For example, you'd like to schedule ``{"CPU": 4}`` bundle, but you only have a single node with 2 CPUs.
Infeasible placement groups will be pending until resources are available. 
The Ray Autoscaler will be aware of placement groups, and auto-scale the cluster to ensure pending groups can be placed as needed. 

If Ray Autoscaler cannot provide resources to schedule a placement group, Ray does *not* print a warning about infeasible tasks. 
You can observe the scheduling state of the placement group from the :ref:`dashboard or state APIs <ray-placement-group-observability-ref>`.

.. _ray-placement-group-schedule-tasks-actors-ref:

Schedule Tasks and Actors to Placement Groups (Use Reserved Resources)
----------------------------------------------------------------------

In the previous section, we created a placement group that reserves ``{"CPU": 1, "GPU: 1"}`` from a 2 CPU and 2 GPU node.

Now let's schedule an actor to the placement group. 
You can schedule actors/tasks on the placement group using
:class:`options(scheduling_strategy=PlacementGroupSchedulingStrategy(...)) <ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy>`.

.. tabbed:: Python

    .. code-block:: python

      @ray.remote
      class Actor:
        def __init__(self):
          pass

        def ready(self):
          pass

      # Create an actor to a placement group.
      actor = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
          placement_group=pg,
        )
      ).remote()

      # Verify the actor is scheduled.
      ray.get(actor.ready.remote(), timeout=10)

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
After the actor is created, we have remaining ``{"CPU": 1, "GPU": 1}`` from this bundle because actor uses 0 CPU.
Let's create another actor to this bundle. This time we explicitly specify actor requires 1 CPU.

.. tabbed:: Python

    .. code-block:: python

      @ray.remote(num_cpus=1)
      class Actor:
        def __init__(self):
          pass

        def ready(self):
          pass

      # Create an actor with 1 CPU to a placement group.
      actor = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
          placement_group=pg,
        )
      ).remote()

      # Verify the actor is scheduled.
      ray.get(actor.ready.remote(), timeout=10)

SANG-TODO images to explain.

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

    .. code-block:: python

      @ray.remote(num_cpus=0, num_gpus=1)
      class Actor:
        def __init__(self):
          pass

        def ready(self):
          pass

      # Create a GPU actor on the first bundle of index 0.
      actor = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
          placement_group=pg,
          placement_group_bundle_index=0,
        )
      ).remote()

      # Verify gpu actor is scheduled.
      ray.get(actor.ready.remote(), timeout=10)

We succeeds to schedule the GPU actor! You can verify the reserved resources are all used from the ``ray status`` command.

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
that creates placement group exits. If you'd like to free the reserved resources, you can also remove the placement
group using ``remove_placement_group`` API (note that it is also an asynchronous API).

.. note::

  When you remove the placement group, actors or tasks that still use the reserved resources will be
  forcefully killed.

.. tabbed:: Python

    .. code-block:: python

      # This API is asynchronous.
      remove_placement_group(pg)

      # Wait until placement group is killed.
      import time
      time.sleep(1)
      # Check the placement group has died.
      pprint(placement_group_table(pg))

      """
      {'bundles': {0: {'GPU': 1.0}, 1: {'CPU': 1.0}},
      'name': 'unnamed_group',
      'placement_group_id': '40816b6ad474a6942b0edb45809b39c3',
      'state': 'REMOVED',
      'strategy': 'PACK'}
      """

      ray.shutdown()

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

.. _ray-placement-group-observability-ref:

Observe and Debug Placement Groups
----------------------------------

Ray provides several useful tools to inspect the placement group states and resource usage.

- **Ray Status** is a CLI tool to see the resource usage and the scheduling resource requirement of the placement groups.
- **Ray Dashboard** is a UI tool to inspect placement group states.
- **Ray State API** is a CLI to inspect placement group states.

.. tabbed:: Ray Status

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

.. _pgroup-strategy:

[Advanced] Placment Strategy
----------------------------

Often, you'd like to reserve bundles with placement constraints. For example, you'd like to pack your bundles to the same
node or spread out to multiple nodes as much as possible.

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

.. tabbed:: Python

    .. code-block:: python

      # Reserve a placement group of 2 bundles
      # that have to be packed on the same node.
      pg = placement_group([{"CPU": 1}, {"GPU": 1}], strategy="STRICT_PACK")

[Advanced] Nested Placement Groups
----------------------------------

By default, child actors/tasks don't share the same placement group that the parent uses.
If you'd like to automatically schedule child actors/tasks to the same placement group,
set ``placement_group_capture_child_tasks`` to True.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/placement_group_capture_child_tasks_example.py
      :language: python

.. tabbed:: Java

    It's not implemented for Java APIs yet.

When ``placement_group_capture_child_tasks`` is True, and if you'd like to avoid scheduling
child tasks/actors, you should specify the below option when you call child tasks/actors.

.. code-block:: python

  @ray.remote
  def parent():
      # In this case, the child task won't be
      # scheduled with the parent's placement group.
      ray.get(child.options(
          scheduling_strategy=PlacementGroupSchedulingStrategy(
              placement_group=None)).remote())

.. _placement-group-detached:

[Advanced] Placement Group Lifetimes Control
--------------------------------------------

.. tabbed:: Python

    By default, the lifetimes of placement groups are not detached and will be destroyed
    when the driver is terminated (but, if it is created from a detached actor, it is
    killed when the detached actor is killed). If you'd like to keep the placement group
    alive regardless of its job or detached actor, you should specify
    `lifetime="detached"`. For example:

    .. code-block:: python

      # first_driver.py
      pg = placement_group([{"CPU": 2}, {"CPU": 2}], strategy="STRICT_SPREAD", lifetime="detached")
      ray.get(pg.ready())

    The placement group's lifetime will be independent of the driver now. This means it
    is possible to retrieve the placement group from other drivers regardless of when
    the current driver exits. Let's see an example:

    .. code-block:: python

      # second_driver.py
      table = ray.util.placement_group_table()
      print(len(table))

    Note that the lifetime option is decoupled from the name. If we only specified
    the name without specifying ``lifetime="detached"``, then the placement group can
    only be retrieved as long as the original driver is still running.

.. tabbed:: Java

    The lifetime argument is not implemented for Java APIs yet.

A placement group can be given a globally unique name.
This allows you to retrieve the placement group from any job in the Ray cluster.
This can be useful if you cannot directly pass the placement group handle to
the actor or task that needs it, or if you are trying to
access a placement group launched by another driver.
Note that the placement group will still be destroyed if it's lifetime isn't `detached`.

.. tabbed:: Python

    .. code-block:: python

      # first_driver.py
      # Create a placement group with a global name.
      pg = placement_group([{"CPU": 2}, {"CPU": 2}], strategy="STRICT_SPREAD", lifetime="detached", name="global_name")
      ray.get(pg.ready())

    Then, we can retrieve the actor later somewhere.

    .. code-block:: python

      # second_driver.py
      # Retrieve a placement group with a global name.
      pg = ray.util.get_placement_group("global_name")

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
