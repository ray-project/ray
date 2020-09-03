Placement Group
===============

Ray is great at scheduling tasks and actors based on its resource requirements.
requirements, it is also important to specify which "place" they should be scheduled. Ray's placement group APIs allow you to place your actors or tasks according to your strategies.

Let's see some scenarios it is useful.

- **Gang scheduling**: Let's say you'd like to place exact number of actors on each GPU node to maximize bandwidth.
- **Maximizing data locality**: To avoid Ray objects transfer cost, you'd like to pack all actors or tasks as much as possible.
- **Improving availability**: To improve availability, you'd like to place your actors or tasks into different physical machines as much as possible.
- **Resource reservation in multi-tenant cluster**: You can reserve resources for your job ahead of time, so that you won't need to worry that resource competition slows down your job.

.. note::

  Ray's placement group is still under active development. Please try out and give us feedback and report bugs!
  APIs are subject to change in the future.

Quick Start
-----------

Let's see an example of using placement group. Note that this example is done within a single node.

.. code-block:: python
  
  import ray
  from pprint import pprint
  # Import placement group APIs.
  from ray.experimental.placement_group import (
      placement_group,
      placement_group_table,
      remove_placement_group
  )

  ray.init(num_gpus=2, resources={"TPU": 2})

  # Bundle is a dictionary of resource requirements.
  # Bundles are reserved on Ray clusters according to placement strategy.
  # Tasks or actors can be scheduled on each bundle.
  gpu_bundle = {"GPU": 2}
  tpu_bundle = {"TPU": 2}
  
  # Reserve 2 of GPU bundles with strict pack strategy.
  # It means Ray will reserve 2 GPU resources on the same node (strict pack) within a Ray cluster.
  # Using this placement group for scheduling actors or tasks will guarantee that they will
  # collocate in the same node.
  pg = placement_group([gpu_bundle, tpu_bundle], strategy="STRICT_PACK")
  
  # Wait until placement group is created.
  ray.get(pg.ready())

  # You can also use ray.wait.
  ready, unready = ray.wait([pg.ready()], timeout=0)

  # You can look at placement group states using this API.
  pprint(placement_group_table(pg))

  # Now let's define actors that uses GPUs.
  @ray.remote(num_gpus=1)
  class GPUActor:
      def __init__(self):
          pass

  # Now let's define actors that uses TPUs.
  @ray.remote(resources={"TPU": 1})
  class TPUActor:
      def __init__(self):
          pass

  # Create GPU actors on a gpu bundle.
  gpu_actors = [GPUActor.options(
          placement_group=pg,
          placement_group_bundle_index=0) # Index of gpu_bundle is 0.
      .remote() for _ in range(2)]

  # Create TPU actors on a tpu bundle.
  tpu_actors = [GPUActor.options(
          placement_group=pg,
          placement_group_bundle_index=0) # Index of gpu_bundle is 0.
      .remote() for _ in range(2)]

  # Now, you can guarantee all gpu and tpu actors are located on the same node
  # because they are scheduled on a placement group that has STRICT_PACK strategy.
  # You can see this by checking all actors have the same node id.
  pprint(ray.actors())

  # Remove the placement group since we don't use it anymore.
  # This API is asynchronous.
  remove_placement_group(pg)

  # Wait until placement group is killed.
  import time
  time.sleep(1)
  # Check the placement group has died.
  pprint(placement_group_table(pg))

  ray.shutdown()

Strategies
----------

Currently, 4 strategies are provided.

- **STRICT_PACK**: Bundles should be packed into a single node.
- **PACK**: Bundles are packed as much as possible, but it can be scheduled on multiple nodes.
- **STRICT_SPREAD**: Each bundle should be scheduled in a separate node.
- **SPREAD**: Each bundle is trying to be scheduled in a separate node, but it is okay that some of bundles are scheduled in the same node.

Lifecycle
---------

Creating
~~~~~~~~
When placement group is first created, the request is sent to GCS. GCS reserve resources to nodes based on scheduling strategy.
you don't need to worry about that there are partilly reserved resources for each placement group because Ray guarantees atomic creation of placement group.

Pending
~~~~~~~
Placement groups are pending creation if there are no nodes that can satisfy resource requirements for a given strategy.

Rescheduling
~~~~~~~~~~~~
If nodes that contain some bundles of a placement group die, bundles will be rescheduled on different nodes by GCS.

Fault Tolerance
---------------

Unlike actors and tasks, placement group is currently not fault tolerant yet. It is in progress.

Package Reference
-----------------

.. autofunction:: ray.experimental.placement_group.placement_group

.. autoclass:: ray.experimental.placement_group.PlacementGroup
   :members:

.. autofunction:: ray.experimental.placement_group.placement_group_table

.. autofunction:: ray.experimental.placement_group.remove_placement_group
