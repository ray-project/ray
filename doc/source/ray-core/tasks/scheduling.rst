.. _ray-scheduling:

Scheduling
==========

For each task, Ray will choose a node to run it and the scheduling decision is based on the following factors in order.

Resources
---------
Each task has the :ref:`specified resource requirements <resource-requirements>`.
Given the specified resource requirements, a node is available (has the available resources to run the task now),
feasible (has the resources but they are not available now)
or infeasible (doesn't have the resources). If there are available nodes, Ray will choose one based on other factors discussed below.
If there are no available nodes but only feasible ones, Ray will wait until resoruces are freed up and nodes become available.
If all nodes are infeasible, the task cannot be scheduled until feasible nodes are added to the cluster.

Placement Group
---------------
If ``placement_group`` option is set then the task will be scheduled where the placement group is located.
See :ref:`Placement Group <ray-placement-group-doc-ref>` for more details.

Scheduling Strategy
-------------------
Task supports ``scheduling_strategy`` option to specify the strategy used to decide the best node among available nodes.
Currently the supported strategies are "DEFAULT" and "SPREAD".
"DEFAULT" is the default strategy used by Ray. With the current implementation, Ray will try to pack tasks on nodes
until the resource utilization is beyond a certain threshold and spread tasks afterwards.
"SPREAD" strategy will try to spread the tasks among available nodes.

.. tabbed:: Python

    .. code-block:: python

        @ray.remote
        def default_function():
            return 1

        # If unspecified, "DEFAULT" scheduling strategy is used.
        default_function.remote()
        # Explicitly set scheduling strategy to "DEFAULT".
        default_function.options(scheduling_strategy="DEFAULT").remote()

        @ray.remote(scheduling_strategy="SPREAD")
        def spread_function():
            return 2
        # Spread tasks across the cluster.
        [spread_function.remote() for i in range(100)]

Locality-Aware Scheduling
-------------------------
When the scheduling strategy is "DEFAULT", Ray also prefers nodes that have task inputs locally
to avoid transferring data over the network. Note: Locality-aware scheduling is only for tasks not actors.