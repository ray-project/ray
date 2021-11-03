Placement Group Examples
========================

.. _ray-placement-group-examples-ref:

Ray placement groups are an advanced feature that helps you to optimize the performance and improve the resiliency (fault tolerance) of your distributed applications.

This page assumes you already have basic knowledge about the placement group APIs. To learn more about APIs, go to the :ref:`placement group document <ray-placement-group-doc-ref>`. 

Colocation
----------
**Recommended Strategy**: `STRICT_PACK`.

Colocation helps you to maximize data locality and minimize communication costs.

When a Ray application runs in a multi-node cluster, inter-node communication for tasks/actors is much more expensive than inter-process communication within the same node.
Here are possible system overheads when your tasks and actors are located in a different node. Imagine you have two Ray primitives (task or actor) A (in a node NA) and B (in a node NB).

- **Object transfer cost**: When A accesses an object created by B (O), the O is not immediately available to A. Here, Ray fetches the O from NB to NA. That says, there will be a network cost to transfer the object and latency overhead to wait for the object to be fetched. The overhead linearly grows to the object size that needs to be transferred.
- **Network cost**: When A and B communicate with each other, there is additional networking overhead since A and B are located in a different node.

In some cases, this type of overhead can have a significant impact on your application's performance. For example, imagine you'd like to read a 
large size dataset using S3 and distribute it to multiple tasks or actors. 

You can minimize the overhead using the placement group's `STRICT_PACK` strategy. `STRICT_PACK` guarantees that all the tasks and actors are located in the same node.

Gang Scheduling
---------------
**Recommended Strategy**: `STRICT_SPREAD`.

Sometimes, you'd like to schedule multiple tasks/actors in a separate physical machine (node) "at the same time". For example, you may use a collective communication library like NCCL and form a communication group with a set of actors. In creating this set of actors, you may want to ensure that all actors are separated and placed evenly across available nodes (creating a homogenous cluster).

You can use placement groups' `STRICT_SPREAD` strategy to achieve it. `STRICT_SPREAD` ensures that all actors and tasks scheduled with the placement group will be located in a separate node.
Also, since the placement group creation is atomic, you can always guarantee that tasks and actors are scheduled at the same time.

Improve Fault tolerance
-----------------------
**Recommended Strategy**: `SPREAD`, `STRICT_SPREAD`.

Imagine you have a set of stateless actors that are used for serving requests. Ray Serve is one of the examples.

In this case, it is common to have multiple replicas of the serving actor to improve the throughput of the applications. But what if every actor is located in the same node?
It means that if that specific node fails, the application will be crashed, or the throughput becomes much lower until all of the dead actors are backed up.

To avoid this, you can use either the `SPREAD` or `STRICT_SPREAD` strategy. 
It ensures the most replica actors are located in a different node, which improves the fault tolerance of your applications.

.. note::

  If `STRICT` strategies are not absolutely necessary, it is encouraged to use just `SPREAD` or `PACK` strategies. Strict strategies can lead to lower resource utilization and is harder to schedule.

Load Balancing
--------------
**Recommended Strategy**: `SPREAD`, `STRICT_SPREAD`.

Imagine you have a set of actors that read a large dataset from S3. It means all these actors' performance is bound to the network bandwidth of physical machines.
At this time, if you start multiple actors in the same physical node, it won't improve the read/write throughput.

Instead, you can use the placement group's `SPREAD` or `STRICT_SPREAD` strategy to ensure all S3 reader actors are located in a different physical node.
