Placement Groups
================

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling). They can be then used to schedule Ray tasks and actors to be packed as close as possible for locality (PACK), or spread apart (SPREAD).

Here are some use cases:

- **Gang Scheduling**: Your application requires all tasks to be scheduled and start at the same time.
- **Maximizing data locality**: You'd like to place or schedule your tasks and actors close to your data to avoid object transfer overheads.
- **Load balancing**: To improve application availability and avoid resource overload, you'd like to place your actors or tasks into different physical machines as much as possible.


Key Concepts
------------

A **bundle** is a collection of "resources", i.e. {"GPU": 4}.
* A bundle must be able to fit on a single node on the Ray cluster.
* Bundles are then placed according to the "placement group strategy" across nodes on the cluster.


A **placement group** is a collection of bundles.
* Each bundle is given an "index" within the placement group
* Bundles are then placed according to the "placement group strategy" across nodes on the cluster.
* After the placement group is created, tasks or actors can be then scheduled according to the placement group and even on individual bundles.


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

  bundle1 = {"GPU": 2}
  bundle2 = {"extra_resource": 2}

  pg = placement_group([bundle1, bundle2, ...], strategy="STRICT_PACK")

.. important:: Each bundle must be able to fit on a single node on the Ray cluster.

Placement groups are atomically created - meaning that if there exists a bundle that cannot fit in any of the current nodes, then the entire placement group will not be ready.

.. code-block:: python

  # Wait until placement group is created.
  ray.get(pg.ready())

  # You can also use ray.wait.
  ready, unready = ray.wait([pg.ready()], timeout=0)

  # You can look at placement group states using this API.
  pprint(placement_group_table(pg))

.. The Ray Autoscaler will be aware of placement groups, and auto-scale the cluster to ensure pending groups can be placed as needed.

.. _pgroup-strategy:

Strategy types
--------------

Ray currently supports the following placement group strategies:

**STRICT_PACK**

All bundles must be placed into a single node on the cluster.
Infeasible placement groups will wait for the Ray Autoscaler to add
the requested resources to the cluster (i.e., a larger node).

**PACK**

All provided bundles are packed onto a single node on a best-effort basis.
If strict packing is not feasible (i.e., some bundles do not fit on the node), bundles can be placed onto other nodes nodes.

**STRICT_SPREAD**

Each bundle must be scheduled in a separate node.
Infeasible placement groups will wait for the Ray Autoscaler to add
the requested resources to the cluster.

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

.. code-block:: python

  gpu_bundle = {"GPU": 2}
  tpu_bundle = {"TPU": 2}

  # Reserve 2 of GPU bundles with strict pack strategy.
  # It means Ray will reserve 2 GPU resources on the same node (strict pack) within a Ray cluster.
  # Using this placement group for scheduling actors or tasks will guarantee that they will
  # collocate in the same node.
  pg = placement_group([gpu_bundle, tpu_bundle], strategy="STRICT_PACK")

  # Wait until placement group is created.
  ray.get(pg.ready())

Now let's define actors that uses GPUs. We'll also define actors that use ``extra_resources``.

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor:
      def __init__(self):
          pass

  @ray.remote(resources={"extra_resource": 1})
  class ExtraResourceActor:
      def __init__(self):
          pass

  # Create GPU actors on a gpu bundle.
  gpu_actors = [GPUActor.options(
          placement_group=pg,
          # This is the index from the original list.
          placement_group_bundle_index=0) # Index of gpu_bundle is 0.
      .remote() for _ in range(2)]

  # Create extra_resource actors on a extra_resource bundle.
  extra_resource_actors = [ExtraResourceActor.options(
          placement_group=pg,
          # This is the index from the original list.
          placement_group_bundle_index=1) # Index of gpu_bundle is 1.
      .remote() for _ in range(2)]

Now, you can guarantee all gpu and extra_resource actors are located on the same node
because they are scheduled on a placement group that has STRICT_PACK strategy.
You can see this by checking all actors have the same node id.

.. code-block:: python

  pprint(ray.actors())

Note that you must remove the placement group once you are finished with your application:

.. code-block:: python

  # This API is asynchronous.
  remove_placement_group(pg)

  # Wait until placement group is killed.
  import time
  time.sleep(1)
  # Check the placement group has died.
  pprint(placement_group_table(pg))

  ray.shutdown()

Lifecycle
---------

When placement group is first created, the request is sent to GCS. GCS reserve resources to nodes based on scheduling strategy. Ray guarantees atomic creation of placement group.

Placement groups are pending creation if there are no nodes that can satisfy resource requirements for a given strategy.

If nodes that contain some bundles of a placement group die, bundles will be rescheduled on different nodes by GCS.

Unlike actors and tasks, placement group is currently not fault tolerant yet. It is in progress.

