.. _ray-scheduling:

Scheduling
==========

For each task or actor, Ray will choose a node to run it and the scheduling decision is based on the following factors.

.. _ray-scheduling-resources:

Resources
---------

Each task or actor has the :ref:`specified resource requirements <resource-requirements>`.
Given that, a node can be in one of the following states:

- Feasible: the node has the required resources to run the task or actor.
  Depending on the current availability of these resources, there are two sub-states:

  - Available: the node has the required resources and they are free now.
  - Unavailable: the node has the required resources but they are currently being used by other tasks or actors.

- Infeasible: the node doesn't have the required resources. For example a CPU-only node is infeasible for a GPU task.

Resource requirements are **hard** requirements meaning that only feasible nodes are eligible to run the task or actor.
If there are feasible nodes, Ray will either choose an avaialbe node or wait until a unavailable node to become available
depending on other factors discussed below.
If all nodes are infeasible, the task or actor cannot be scheduled until feasible nodes are added to the cluster.

.. _ray-scheduling-strategies:

Scheduling Strategies
---------------------

Tasks or actors support a :ref:`scheduling_strategy <ray-remote-ref>` option to specify the strategy used to decide the best node among feasible nodes.
Currently the supported strategies are the followings.

"DEFAULT"
~~~~~~~~~

``"DEFAULT"`` is the default strategy used by Ray. With the current implementation, Ray will try to pack tasks or actors on nodes
until the resource utilization is beyond a certain threshold and spread them afterwards.

Currently Ray handles actors that don't require any resources (i.e., ``num_cpus=0`` with no other resources) specially by randomly choosing a node in the cluster without considering resource utilization.
Since nodes are randomly chosen, actors that don't require any resources are effectively SPREAD across the cluster.

.. literalinclude:: ../doc_code/scheduling.py
    :language: python
    :start-after: __default_scheduling_strategy_start__
    :end-before: __default_scheduling_strategy_end__

"SPREAD"
~~~~~~~~

``"SPREAD"`` strategy will try to spread the tasks or actors among available nodes.

.. literalinclude:: ../doc_code/scheduling.py
    :language: python
    :start-after: __spread_scheduling_strategy_start__
    :end-before: __spread_scheduling_strategy_end__

PlacementGroupSchedulingStrategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy` will schedule the task or actor to where the placement group is located.
This is useful for actor gang scheduling. See :ref:`Placement Group <ray-placement-group-doc-ref>` for more details.

NodeAffinitySchedulingStrategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy` is a low-level strategy that allows a task or actor to be scheduled onto a particular node specified by its node id.
The ``soft`` flag specifies whether the task or actor is allowed to run somewhere else if the specified node doesn't exist (e.g. if the node dies)
or is infeasible because it does not have the resources required to run the task or actor.
In these cases, if ``soft`` is True, the task or actor will be scheduled onto a different feasible node.
Otherwise, the task or actor will fail with :py:class:`~ray.exceptions.TaskUnschedulableError` or :py:class:`~ray.exceptions.ActorUnschedulableError`.
As long as the specified node is alive and feasible, the task or actor will only run there
regardless of the ``soft`` flag. This means if the node currently has no available resources, the task or actor will wait until resources
become available.
This strategy should *only* be used if other high level scheduling strategies (e.g. :ref:`placement group <ray-placement-group-doc-ref>`) cannot give the
desired task or actor placements. It has the following known limitations:

- It's a low-level strategy which prevents optimizations by a smart scheduler.
- It cannot fully utilize an autoscaling cluster since node ids must be known when the tasks or actors are created.
- It can be difficult to make the best static placement decision
  especially in a multi-tenant cluster: for example, an application won't know what else is being scheduled onto the same nodes.

.. literalinclude:: ../doc_code/scheduling.py
    :language: python
    :start-after: __node_affinity_scheduling_strategy_start__
    :end-before: __node_affinity_scheduling_strategy_end__

.. _ray-scheduling-locality:

Locality-Aware Scheduling
-------------------------

By default, Ray prefers available nodes that have large task arguments local
to avoid transferring data over the network. If there are multiple large task arguments,
the node with most object bytes local is preferred.
This takes precedence over the ``"DEFAULT"`` scheduling strategy,
which means Ray will try to run the task on the locality preferred node regardless of the node resource utilization.
However, if the locality preferred node is not available, Ray may run the task somewhere else.
When other scheduling strategies are specified,
they have higher precedence and data locality is no longer considered.

.. note::

  Locality-aware scheduling is only for tasks not actors.

.. literalinclude:: ../doc_code/scheduling.py
    :language: python
    :start-after: __locality_aware_scheduling_start__
    :end-before: __locality_aware_scheduling_end__

More about Ray Scheduling
-------------------------

.. toctree::
    :maxdepth: 1

    resources
    ../tasks/using-ray-with-gpus
    placement-group
    memory-management
    ray-oom-prevention
