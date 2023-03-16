Placement Groups
================

.. _ray-placement-group-doc-ref:

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling). They can be then used to schedule Ray tasks and actors packed as close as possible for locality (PACK), or spread apart (SPREAD). Placement groups are generally used for gang-scheduling actors, but also support tasks.

Here are some real-world use cases:
- **Machine Learning Training**: Placement group enables gang scheduling by reserving a group of resources across Ray cluster. Gang scheduling is a critical technique to enable all-or-nothing scheduling for deep learning training. 
- **Hyperparameter Tuning**: Hyperparameter tuning involves training multiple versions of a model (trial) with different hyperparameters, 
such as learning rates, batch sizes, and regularization parameters. Each version of the model is trained on a separate 
compute node or GPU, and the results are compared to determine the optimal set of hyperparameters.
When hyperparemeter tuning the deep learning models, each trial should ensure to schedule every worker involved in training.
Placement groups reserve a group of resources for each trial, which mitigates the risk of deadlock
(In a given machine learning training task, each trial only has a partial schedule of training workers allocated to it. As a result, none of the trials can fully schedule all of the available workers for training).

Key Concepts
------------

A **bundle** is a collection of "resources", It could be a single resource (``{"CPU": 1}``) or a group of resources ``{"CPU": 1, "GPU": 4}``. 
A bundle is a unit of reservation for placement groups. A bundle must be able to fit on a single node on the Ray cluster. For example, if you only have 8 CPU nodes, and if you have a bundle that requires ``{"CPU": 9}``, there's no node that can reserve this bundle.

A **placement group** is a collection of bundles.

Placement groups are represented by a list of bundles. For example, ``{"CPU": 1} * 4`` means you'd like to reserve 4 bundles of 1 CPU 
(i.e., it reserves 4 CPUs). 

- Each bundle is given an "index" within the placement group. e.g., A placement group of 2 bundles ``[{"CPU": 1}, {"GPU": 1}]`` will have index 0 bundle ``{"CPU": 1}`` and index 1 bundle ``{"GPU": 1}``.
- Bundles are then placed according to the "placement group strategy" across nodes on the cluster.
- After the placement group is created, tasks or actors can be then scheduled according to the placement group and even on individual bundles.

A **placement group strategy** is an algorithm for selecting nodes for bundle placement. See :ref:`placement strategies <pgroup-strategy>` for more details.


Schedule a Placement Group
--------------------------

Ray placement group can be created via the ``ray.util.placement_group`` (Python). Placement groups take in a list of bundles and a :ref:`placement strategy <pgroup-strategy>`.
Note that each bundle must be able to fit on a single node on the Ray cluster.
For example, if you only have 8 CPU nodes, and if you have a bundle that requires ``{"CPU": 9}``,
there's no node that can reserve this bundle.

When specifying bundles (as a dictionary, e.g., ``{"CPU": 1}``),

- ``CPU`` will correspond with ``num_cpus`` as used in ``ray.remote``
- ``GPU`` will correspond with ``num_gpus`` as used in ``ray.remote``
- Other resources will correspond with ``resources`` as used in ``ray.remote``. E.g., ``ray.init(resources={"disk": 1})`` can have a bundle of ``{"disk": 1}``.

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
      ray.init(num_cpus=1, num_gpus=1)

      # Reserve a placement group of 1 bundle that reserves 1 CPU and 1 GPU.
      pg = placement_group([{"CPU": 1, "GPU": 1}], strategy="STRICT_PACK")

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
**It is recommended to verify that their placement groups are ready** before using it. 
Ray assumes that the placement group will be properly created and does *not* print a warning about infeasible tasks.

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
      # This will hang!
      ray.get(pg.ready(), timeout=5)

When the placement group cannot be scheduled in any way, it is called "infeasible". 
Infeasible placement groups will be pending until resources are available. 
The Ray Autoscaler will be aware of placement groups, and auto-scale the cluster to ensure pending groups can be placed as needed. 
If Ray Autoscaler cannot provide resources to schedule a placement group, 


SANG-TODO 

Schedule Tasks and Actors to Placement Groups
---------------------------------------------

tasks and actors can use the placement groups (reserved resources) when they are scheduled with ``PlacementGroupSchedulingStrategy``.
To demonstrate it, let's initialize a new Ray instance.

.. code-block:: python

  import ray
  from pprint import pprint

  # Import placement group APIs.
  from ray.util.placement_group import (
      placement_group,
      placement_group_table,
      remove_placement_group
  )
  from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

  ray.init(num_cpus=2, num_gpus=2)

Now let's define an actor that uses GPU. We'll also define a task that use ``extra_resources``.
You can schedule actors/tasks on the placement group using
:class:`options(scheduling_strategy=PlacementGroupSchedulingStrategy(...)) <ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy>`.

.. tabbed:: Python

    .. code-block:: python

      gpu_bundle = {"CPU":2, "GPU": 2}
      extra_resource_bundle = {"CPU": 2, "extra_resource": 2}

      # Reserve bundles with strict pack strategy.
      # It means Ray will reserve 2 "GPU" and 2 "extra_resource" on the same node (strict pack) within a Ray cluster.
      # Using this placement group for scheduling actors or tasks will guarantee that they will
      # be colocated on the same node.
      pg = placement_group([gpu_bundle, extra_resource_bundle], strategy="STRICT_PACK")

      # Wait until placement group is created.
      ray.get(pg.ready())

      @ray.remote(num_gpus=1)
      class GPUActor:
        def __init__(self):
          pass

      @ray.remote(resources={"extra_resource": 1})
      def extra_resource_task():
        import time
        # simulate long-running task.
        time.sleep(10)

      # Create GPU actors on a gpu bundle.
      gpu_actors = [
        GPUActor.options(
          scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            # This is the index from the original list.
            # This index is set to -1 by default, which means any available bundle.
            placement_group_bundle_index=0 # Index of gpu_bundle is 0.
          )
        ).remote() for _ in range(2)
      ]

      # Create extra_resource actors on a extra_resource bundle.
      extra_resource_actors = [
        extra_resource_task.options(
          scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
              # This is the index from the original list.
              # This index is set to -1 by default, which means any available bundle.
              placement_group_bundle_index=1 # Index of extra_resource_bundle is 1.
          )
        ).remote() for _ in range(2)
      ]

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
      for (int index = 0; index < 2; index++) {
        Ray.actor(Counter::new, 1)
          .setResource("GPU", 1.0)
          .setPlacementGroup(pg, 0)
          .remote();
      }

      // Create extra_resource actors on a extra_resource bundle.
      for (int index = 0; index < 2; index++) {
        Ray.task(Counter::ping)
          .setPlacementGroup(pg, 1)
          .setResource("extra_resource", 1.0)
          .remote().get();
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
      for (int index = 0; index < 2; index++) {
        ray::Actor(CreateCounter)
          .SetResource("GPU", 1.0)
          .SetPlacementGroup(pg, 0)
          .Remote(1);
      }

      // Create extra_resource actors on a extra_resource bundle.
      for (int index = 0; index < 2; index++) {
        ray::Task(&Counter::Ping)
          .SetPlacementGroup(pg, 1)
          .SetResource("extra_resource", 1.0)
          .Remote().Get();
      }


Now, you can guarantee all gpu actors and extra_resource tasks are located on the same node
because they are scheduled on a placement group with the STRICT_PACK strategy.

.. note::

  Child actors/tasks don't share the same placement group that the parent uses.
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

You can remove a placement group at any time to free its allocated resources.

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
      {'bundles': {0: {'GPU': 2.0}, 1: {'extra_resource': 2.0}},
      'name': 'unnamed_group',
      'placement_group_id': '40816b6ad474a6942b0edb45809b39c3',
      'state': 'REMOVED',
      'strategy': 'STRICT_PACK'}
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

Remove Placement Groups
-----------------------

Observe Placement Groups
------------------------

[Advanced] Nested Placement Groups
----------------------------------

.. _pgroup-strategy:

Strategy types
--------------

Often, you'd like to reserve bundles with placement constraints. 

Ray currently supports the following placement group strategies:

**STRICT_PACK**

All bundles must be placed into a single node on the cluster.

**PACK**

All provided bundles are packed onto a single node on a best-effort basis.
If strict packing is not feasible (i.e., some bundles do not fit on the node), bundles can be placed onto other nodes nodes.

**STRICT_SPREAD**

Each bundle must be scheduled in a separate node.

**SPREAD**

Each bundle will be spread onto separate nodes on a best effort basis.
If strict spreading is not feasible, bundles can be placed overlapping nodes.

.. _placement-group-lifetimes:

Change the Placement Group Lifetimes
------------------------------------

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

Lifecycle
---------

.. _ray-placement-group-lifecycle-ref:

**Creation**: When placement groups are first created, the request is sent to the GCS. The GCS sends resource reservation requests to nodes based on its scheduling strategy. Ray guarantees placement groups are placed atomically.

**Autoscaling**: Placement groups are pending creation if there are no nodes that can satisfy resource requirements for a given strategy. The Ray Autoscaler will be aware of placement groups, and auto-scale the cluster to ensure pending groups can be placed as needed.

**Cleanup**: Placement groups are automatically removed when the job that created the placement group is finished. The only exception is that it is created by detached actors. In this case, placement groups fate-share with the detached actors.


Fault Tolerance
---------------

.. _ray-placement-group-ft-ref:

If nodes that contain some bundles of a placement group die, all the bundles will be rescheduled on different nodes by GCS. This means that the initial creation of placement group is "atomic", but once it is created, there could be partial placement groups.

Placement groups are tolerant to worker nodes failures (bundles on dead nodes are rescheduled). However, placement groups are currently unable to tolerate head node failures (GCS failures), which is a single point of failure of Ray.

API Reference
-------------
:ref:`Placement Group API reference <ray-placement-group-ref>`
