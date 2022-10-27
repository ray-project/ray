Placement Groups
================

.. _ray-placement-group-doc-ref:

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling). They can be then used to schedule Ray tasks and actors packed as close as possible for locality (PACK), or spread apart (SPREAD). Placement groups are generally used for gang-scheduling actors, but also support tasks.

Java demo code in this documentation can be found here `<https://github.com/ray-project/ray/blob/master/java/test/src/main/java/io/ray/docdemo/PlacementGroupDemo.java>`__.

Here are some use cases:

- **Gang Scheduling**: Your application requires all tasks/actors to be scheduled and start at the same time.
- **Maximizing data locality**: You'd like to place or schedule your actors close to your data to avoid object transfer overheads.

Key Concepts
------------

A **bundle** is a collection of "resources", i.e. `{"GPU": 4}`.

- A bundle must be able to fit on a single node on the Ray cluster.
- Bundles are then placed according to the "placement group strategy" across nodes on the cluster.


A **placement group** is a collection of bundles.

- Each bundle is given an "index" within the placement group
- Bundles are then placed according to the "placement group strategy" across nodes on the cluster.
- After the placement group is created, tasks or actors can be then scheduled according to the placement group and even on individual bundles.


A **placement group strategy** is an algorithm for selecting nodes for bundle placement. Read more about :ref:`placement strategies <pgroup-strategy>`.


Starting a placement group
--------------------------

Ray placement group can be created via the ``ray.util.placement_group`` (Python) or ``PlacementGroups.createPlacementGroup`` (Java) API. Placement groups take in a list of bundles and a :ref:`placement strategy <pgroup-strategy>`:

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
      ray.init(num_gpus=2, resources={"extra_resource": 2})

      bundle1 = {"GPU": 2}
      bundle2 = {"extra_resource": 2}

      pg = placement_group([bundle1, bundle2], strategy="STRICT_PACK")

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

.. important:: Each bundle must be able to fit on a single node on the Ray cluster.

Placement groups are atomically created - meaning that if there exists a bundle that cannot fit in any of the current nodes, then the entire placement group will not be ready.

.. tabbed:: Python

    .. code-block:: python

      # Wait until placement group is created.
      ray.get(pg.ready())

      # You can also use ray.wait.
      ready, unready = ray.wait([pg.ready()], timeout=0)

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

Infeasible placement groups will be pending until resources are available. The Ray Autoscaler will be aware of placement groups, and auto-scale the cluster to ensure pending groups can be placed as needed.

.. _pgroup-strategy:

Strategy types
--------------

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

Quick Start
-----------

Let's see an example of using placement group. Note that this example is done within a single node.

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

  ray.init(num_cpus=4, num_gpus=2, resources={"extra_resource": 2})

Let's create a placement group. Recall that each bundle is a collection of resources, and tasks or actors can be scheduled on each bundle.

.. note::

  When specifying bundles,

  - "CPU" will correspond with `num_cpus` as used in `ray.remote`
  - "GPU" will correspond with `num_gpus` as used in `ray.remote`
  - Other resources will correspond with `resources` as used in `ray.remote`.

  Once the placement group reserves resources, original resources are unavailable until the placement group is removed. For example:

  .. tabbed:: Python

      .. literalinclude:: ../doc_code/original_resource_unavailable_example.py
        :language: python

  .. tabbed:: Java

      .. code-block:: java

        System.setProperty("ray.head-args.0", "--num-cpus=2");
        Ray.init();

        public static class Counter {
          public static String ping() {
            return "pong";
          }
        }

        // Construct a list of bundles.
        Map<String, Double> bundle = ImmutableMap.of("CPU", 2.0);
        List<Map<String, Double>> bundles = ImmutableList.of(bundle);

        // Create a placement group and make sure its creation is successful.
        PlacementGroupCreationOptions options =
          new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_SPREAD)
            .build();

        PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
        boolean isCreated = pg.wait(60);
        Assert.assertTrue(isCreated);

        // Won't be scheduled because there are no 2 cpus now.
        ObjectRef<String> obj = Ray.task(Counter::ping)
          .setResource("CPU", 2.0)
          .remote();

        List<ObjectRef<String>> waitList = ImmutableList.of(obj);
        WaitResult<String> waitResult = Ray.wait(waitList, 1, 5 * 1000);
        Assert.assertEquals(1, waitResult.getUnready().size());

        // Will be scheduled because 2 cpus are reserved by the placement group.
        obj = Ray.task(Counter::ping)
          .setPlacementGroup(pg, 0)
          .setResource("CPU", 2.0)
          .remote();
        Assert.assertEquals(obj.get(), "pong");

  .. tabbed:: C++

      .. code-block:: c++

        RayConfig config;
        config.num_cpus = 2;
        ray::Init(config);

        class Counter {
        public:
          std::string Ping() {
            return "pong";
          }
        };

        // Factory function of Counter class.
        static Counter *CreateCounter() {
          return new Counter();
        };

        RAY_REMOTE(&Counter::Ping, CreateCounter);

        // Construct a list of bundles.
        std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 2.0}}};

        // Create a placement group and make sure its creation is successful.
        ray::PlacementGroupCreationOptions options{
            false, name, bundles, ray::PlacementStrategy::STRICT_SPREAD};


        ray::PlacementGroup pg = ray::CreatePlacementGroup(options);
        bool is_created = pg.Wait(60);
        assert(is_created);

        // Won't be scheduled because there are no 2 cpus now.
        ray::ObjectRef<std::string> obj = ray::Task(&Counter::Ping)
          .SetResource("CPU", 2.0)
          .Remote();

        std::vector<ray::ObjectRef<std::string>> wait_list = {obj};
        auto wait_result = ray::Wait(wait_list, 1, 5 * 1000);
        assert(wait_result.unready.size() == 1);

        // Will be scheduled because 2 cpus are reserved by the placement group.
        obj = ray::Task(&Counter::Ping)
          .SetPlacementGroup(pg, 0)
          .SetResource("CPU", 2.0)
          .Remote();
        assert(*obj.get() == "pong");

.. note::

  When using placement groups, it is recommended to verify that their placement groups are ready (by calling ``ray.get(pg.ready())``)
  and have the proper resources. Ray assumes that the placement group will be properly created and does *not*
  print a warning about infeasible tasks.

  .. tabbed:: Python

      .. code-block:: python

        gpu_bundle = {"GPU": 2}
        extra_resource_bundle = {"extra_resource": 2}

        # Reserve bundles with strict pack strategy.
        # It means Ray will reserve 2 "GPU" and 2 "extra_resource" on the same node (strict pack) within a Ray cluster.
        # Using this placement group for scheduling actors or tasks will guarantee that they will
        # be colocated on the same node.
        pg = placement_group([gpu_bundle, extra_resource_bundle], strategy="STRICT_PACK")

        # Wait until placement group is created.
        ray.get(pg.ready())

  .. tabbed:: Java

      .. code-block:: java

        Map<String, Double> bundle1 = ImmutableMap.of("GPU", 2.0);
        Map<String, Double> bundle2 = ImmutableMap.of("extra_resource", 2.0);
        List<Map<String, Double>> bundles = ImmutableList.of(bundle1, bundle2);

        /**
         * Reserve bundles with strict pack strategy.
         * It means Ray will reserve 2 "GPU" and 2 "extra_resource" on the same node (strict pack) within a Ray cluster.
         * Using this placement group for scheduling actors or tasks will guarantee that they will
         * be colocated on the same node.
         */
        PlacementGroupCreationOptions options =
          new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_PACK)
            .build();

        PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
        boolean isCreated = pg.wait(60);
        Assert.assertTrue(isCreated);

  .. tabbed:: C++

      .. code-block:: c++

        std::vector<std::unordered_map<std::string, double>> bundles{{{"GPU", 2.0}, {"extra_resource", 2.0}}};

        // Reserve bundles with strict pack strategy.
        // It means Ray will reserve 2 "GPU" and 2 "extra_resource" on the same node (strict pack) within a Ray cluster.
        // Using this placement group for scheduling actors or tasks will guarantee that they will
        // be colocated on the same node.
        ray::PlacementGroupCreationOptions options{
            false, "my_pg", bundles, ray::PlacementStrategy::STRICT_PACK};

        ray::PlacementGroup pg = ray::CreatePlacementGroup(options);
        bool is_created = pg.Wait(60);
        assert(is_created);

Now let's define an actor that uses GPU. We'll also define a task that use ``extra_resources``.
You can schedule actors/tasks on the placement group using
:ref:`options(scheduling_strategy=PlacementGroupSchedulingStrategy(...)) <scheduling-strategy-ref>`.

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

Named Placement Groups
----------------------

A placement group can be given a globally unique name.
This allows you to retrieve the placement group from any job in the Ray cluster.
This can be useful if you cannot directly pass the placement group handle to
the actor or task that needs it, or if you are trying to
access a placement group launched by another driver.
Note that the placement group will still be destroyed if it's lifetime isn't `detached`.
See :ref:`placement-group-lifetimes` for more details.

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

.. _placement-group-lifetimes:

Placement Group Lifetimes
-------------------------

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

Tips for Using Placement Groups
-------------------------------
- Learn the :ref:`lifecycle <ray-placement-group-lifecycle-ref>` of placement groups.
- Learn the :ref:`fault tolerance <ray-placement-group-ft-ref>` of placement groups.


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
