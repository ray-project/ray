.. _ray-task-scheduling:

Scheduling
==========

For each task, Ray will choose a node to run it and the scheduling decision is based on the following factors in order.

Resources
---------
Each task has the :ref:`specified resource requirements <resource-requirements>` and requires 1 CPU by default.
Given the specified resource requirements, a node is available (has the available resources to run the task now),
feasible (has the resources but they are not available now)
or infeasible (doesn't have the resources). If there are available nodes, Ray will choose one based on other factors discussed below.
If there are no available nodes but only feasible ones, Ray will wait until resources are freed up and nodes become available.
If all nodes are infeasible, the task cannot be scheduled until feasible nodes are added to the cluster.

Placement Group
---------------
If ``placement_group`` option is set then the task will be scheduled where the placement group is located.
See :ref:`Placement Group <ray-placement-group-doc-ref>` for more details.

Scheduling Strategy
-------------------
Tasks support a ``scheduling_strategy`` option to specify the strategy used to decide the best node among available nodes.
Currently the supported strategies are ``"DEFAULT"``, ``"SPREAD"`` and
``ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(node_id, soft: bool)``.

"DEFAULT" is the default strategy used by Ray. With the current implementation, Ray will try to pack tasks on nodes
until the resource utilization is beyond a certain threshold and spread tasks afterwards.

"SPREAD" strategy will try to spread the tasks among available nodes.

NodeAffinitySchedulingStrategy is a low-level strategy that allows a task to be scheduled onto a particular node specified by its node id.
The ``soft`` flag specifies whether the task is allowed to run somewhere else if the specified node doesn't exist (e.g. if the node dies)
or is infeasible because it does not have the resources required to run the task. In these cases, if ``soft`` is True, the task will be scheduled onto a different feasible node.
Otherwise, the task will fail with ``TaskUnschedulableError``.
As long as the specified node is alive and feasible, the task will only run there
regardless of the ``soft`` flag. This means if the node currently has no available resources, the task will wait until resources
become available.
This strategy should *only* be used if other high level scheduling strategies (e.g. :ref:`placement group <ray-placement-group-doc-ref>`) cannot give the
desired task placements. It has the following known limitations:
1. It's a low-level strategy which prevents optimizations by a smart scheduler.
2. It cannot fully utilize an autoscaling cluster since node ids must be known when the tasks are created.
3. It can be difficult to make the best static task placement decision
especially in a multi-tenant cluster: for example, an application won't know what else is being scheduled onto the same nodes.

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

        @ray.remote
        def node_affinity_function():
            return ray.get_runtime_context().node_id

        # Only run the task on the local node.
        node_affinity_function.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id = ray.get_runtime_context().node_id,
                soft = False,
            )
        ).remote()

        # Run the two node_affinity_function tasks on the same node if possible.
        node_affinity_function.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id = ray.get(node_affinity_function.remote()),
                soft = True,
            )
        ).remote()

Locality-Aware Scheduling
-------------------------
By default, Ray prefers available nodes that have large task arguments local
to avoid transferring data over the network. If there are multiple large task arguments,
the node with most object bytes local is preferred.
This takes precedence over the ``"DEFAULT"`` scheduling strategy,
which means we will try to run the task on the locality preferred node regardless of the node resource utilization.
However, if the locality preferred node is not available, we may run the task somewhere else.
When ``"SPREAD"`` and ``NodeAffinitySchedulingStrategy`` scheduling strategies are specified,
they have higher precedence and data locality is no longer considered.
Note: Locality-aware scheduling is only for tasks not actors.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/task_locality_aware_scheduling.py
