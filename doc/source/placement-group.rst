Placement Groups
================

.. _ray-placement-group-doc-ref:

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling). They can be then used to schedule Ray tasks and actors to be packed as close as possible for locality (PACK), or spread apart (SPREAD).

Here are some use cases:

- **Gang Scheduling**: Your application requires all tasks/actors to be scheduled and start at the same time.
- **Maximizing data locality**: You'd like to place or schedule your tasks and actors close to your data to avoid object transfer overheads.
- **Load balancing**: To improve application availability and avoid resource overload, you'd like to place your actors or tasks into different physical machines as much as possible.

To learn more about production use cases, check out the :ref:`examples <ray-placement-group-examples-ref>`.

Key Concepts
------------

A **bundle** is a collection of "resources", i.e. {"GPU": 4}.

- A bundle must be able to fit on a single node on the Ray cluster.
- Bundles are then placed according to the "placement group strategy" across nodes on the cluster.


A **placement group** is a collection of bundles.

- Each bundle is given an "index" within the placement group
- Bundles are then placed according to the "placement group strategy" across nodes on the cluster.
- After the placement group is created, tasks or actors can be then scheduled according to the placement group and even on individual bundles.


A **placement group strategy** is an algorithm for selecting nodes for bundle placement. Read more about :ref:`placement strategies <pgroup-strategy>`.


Starting a placement group
--------------------------

Ray placement group can be created via the ``ray.util.placement_group`` API. Placement groups take in a list of bundles and a :ref:`placement strategy <pgroup-strategy>`:

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

.. important:: Each bundle must be able to fit on a single node on the Ray cluster.

Placement groups are atomically created - meaning that if there exists a bundle that cannot fit in any of the current nodes, then the entire placement group will not be ready.

.. code-block:: python

  # Wait until placement group is created.
  ray.get(pg.ready())

  # You can also use ray.wait.
  ready, unready = ray.wait([pg.ready()], timeout=0)

  # You can look at placement group states using this API.
  print(placement_group_table(pg))

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

  ray.init(num_gpus=2, resources={"extra_resource": 2})

Let's create a placement group. Recall that each bundle is a collection of resources, and tasks or actors can be scheduled on each bundle.

.. note::

  When specifying bundles,

  - "CPU" will correspond with `num_cpus` as used in `ray.remote`
  - "GPU" will correspond with `num_gpus` as used in `ray.remote`
  - Other resources will correspond with `resources` as used in `ray.remote`.

  Once the placement group reserves resources, original resources are unavailable until the placement group is removed. For example:

  .. code-block:: python

    # Two "CPU"s are available.
    ray.init(num_cpus=2)

    # Create a placement group.
    pg = placement_group([{"CPU": 2}])
    ray.get(pg.ready())

    # Now, 2 CPUs are not available anymore because they are pre-reserved by the placement group.
    @ray.remote(num_cpus=2)
    def f():
        return True

    # Won't be scheduled because there are no 2 cpus.
    f.remote()

    # Will be scheduled because 2 cpus are reserved by the placement group.
    f.options(placement_group=pg).remote()

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

Now let's define an actor that uses GPU. We'll also define a task that use ``extra_resources``.

.. code-block:: python

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
  gpu_actors = [GPUActor.options(
          placement_group=pg,
          # This is the index from the original list.
          # This index is set to -1 by default, which means any available bundle.
          placement_group_bundle_index=0) # Index of gpu_bundle is 0.
      .remote() for _ in range(2)]

  # Create extra_resource actors on a extra_resource bundle.
  extra_resource_actors = [extra_resource_task.options(
          placement_group=pg,
          # This is the index from the original list.
          # This index is set to -1 by default, which means any available bundle.
          placement_group_bundle_index=1) # Index of extra_resource_bundle is 1.
      .remote() for _ in range(2)]

Now, you can guarantee all gpu actors and extra_resource tasks are located on the same node
because they are scheduled on a placement group with the STRICT_PACK strategy.

.. note::

  In order to fully utilize resources pre-reserved by the placement group,
  Ray automatically schedules children tasks/actors to the same placement group as its parent.

  .. code-block:: python

    # Create a placement group with the STRICT_SPREAD strategy.
    pg = placement_group([{"CPU": 2}, {"CPU": 2}], strategy="STRICT_SPREAD")
    ray.get(pg.ready())

    @ray.remote
    def child():
        pass

    @ray.remote
    def parent():
        # The child task is scheduled with the same placement group as its parent
        # although child.options(placement_group=pg).remote() wasn't called.
        ray.get(child.remote())

    ray.get(parent.options(placement_group=pg).remote())

  To avoid it, you should specify `options(placement_group=None)` in a child task/actor remote call.

  .. code-block:: python

    @ray.remote
    def parent():
        # In this case, the child task won't be
        # scheduled with the parent's placement group.
        ray.get(child.options(placement_group=None).remote())

Note that you can anytime remove the placement group to clean up resources.

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

Tips for Using Placement Groups
-------------------------------
- Learn the :ref:`lifecycle <ray-placement-group-lifecycle-ref>` of placement groups.
- Learn the :ref:`fault tolerance <ray-placement-group-ft-ref>` of placement groups.
- See more :ref:`examples <ray-placement-group-examples-ref>` to learn real world use cases of placement group APIs.

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
