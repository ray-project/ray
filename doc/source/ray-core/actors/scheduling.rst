.. _ray-actor-scheduling:

Scheduling
==========

For each actor, Ray will choose a node to run it and the scheduling decision is based on the following factors in order.

Resources
---------
Each actor has the :ref:`specified resource requirements <resource-requirements>`.
By default, actors require 1 CPU for scheduling and 0 CPU for running.
This means they cannot get scheduled on a zero-cpu node, but an infinite number of them
can run on any non-zero cpu node. If resources are specified explicitly, they are required
for both scheduling and running.
Given the specified resource requirements, a node is available (has the available resources to run the actor now),
feasible (has the resources but they are not available now)
or infeasible (doesn't have the resources). If there are available nodes, Ray will choose one based on other factors discussed below.
If there are no available nodes but only feasible ones, Ray will wait until resources are freed up and nodes become available.
If all nodes are infeasible, the actor cannot be scheduled until feasible nodes are added to the cluster.

Placement Group
---------------
If ``placement_group`` option is set then the actor will be scheduled where the placement group is located.
See :ref:`Placement Group <ray-placement-group-doc-ref>` for more details.

Scheduling Strategy
-------------------
Actors support ``scheduling_strategy`` option to specify the strategy used to decide the best node among available nodes.
Currently the only supported strategy for actors is "DEFAULT".
"DEFAULT" is the default strategy used by Ray. With the current implementation, Ray will try to pack actors on nodes
until the resource utilization is beyond a certain threshold and spread actors afterwards.
Currently Ray handles actors that don't require any resources (i.e., ``num_cpus=0`` with no other resources) specially by randomly choosing a node in the cluster without considering resource utilization.
Since nodes are randomly chosen, actors that don't require any resources are effectively SPREAD across the cluster.

.. tabbed:: Python

    .. code-block:: python

        @ray.remote
        class Actor:
            pass

        # "DEFAULT" scheduling strategy is used (packed onto nodes until reaching a threshold and then spread).
        a1 = Actor.remote()

        # Zero-CPU (and no other resources) actors are randomly assigned to nodes.
        a2 = Actor.options(num_cpus=0).remote()
