.. _ray-scheduling:

Scheduling
==========

This page provides an overview of how Ray decides to schedule tasks and actors to nodes.

Scheduling at a glance
----------------------

Ray schedules every task and actor without any configuration from you. Each control below has a default that applies until you override it, so read this section as a map of the available controls rather than a list of required settings.

Ray places a task or actor in two steps. First it narrows the cluster to the nodes that can run the work at all, using the resource requirements and label selectors you declared. Then it picks one of those nodes using the scheduling strategy. For tasks under the ``"DEFAULT"`` strategy, data locality takes precedence over utilization, so Ray prefers a node that already holds the task's large arguments.

.. list-table::
   :header-rows: 1
   :widths: 22 30 48

   * - Control
     - Default
     - Where it's documented
   * - Node logical resources
     - Auto-detected from the machine's physical CPU, GPU, and memory
     - :ref:`Resources <ray-scheduling-resources>`
   * - Task resource requirements
     - 1 logical CPU
     - :ref:`Specifying resource requirements <resource-requirements>`
   * - Actor resource requirements
     - 1 logical CPU to schedule, 0 to run
     - :ref:`Specifying resource requirements <resource-requirements>`
   * - Node labels
     - ``ray.io/node-id`` on every node; ``ray.io/accelerator-type`` on accelerator nodes
     - :doc:`./labels`
   * - Scheduling strategy
     - ``"DEFAULT"``
     - :ref:`Scheduling strategies <ray-scheduling-strategies>`
   * - Data locality
     - Enabled for tasks, ignored for actors and when you set a strategy
     - :ref:`Locality-aware scheduling <ray-scheduling-locality>`
   * - Gang placement
     - None. Ray schedules each task and actor independently unless you create a placement group
     - :doc:`./placement-group`

Because the actor scheduling default is non-zero while its running default is zero, an actor with no arguments still needs a node with at least one free CPU to start, and any number of them can then run there. A node with ``num_cpus=0`` runs neither tasks nor actors by default.

The ``"DEFAULT"`` strategy's node selection is tunable through environment variables, though most clusters never need to change them:

.. list-table::
   :header-rows: 1
   :widths: 40 12 48

   * - Environment variable
     - Default
     - Effect
   * - ``RAY_scheduler_spread_threshold``
     - ``0.5``
     - Utilization below this scores a node as 0, making it equally preferred with other lightly loaded nodes.
   * - ``RAY_scheduler_top_k_fraction``
     - ``0.2``
     - Fraction of cluster nodes forming the candidate set Ray randomly picks from.
   * - ``RAY_scheduler_top_k_absolute``
     - ``1``
     - Floor on that candidate set, so ``k`` never drops below this in a small cluster.

See :ref:`"DEFAULT" <ray-scheduling-strategies>` for how Ray combines these into a score.

Labels
------

Labels provide a simplified solution for controlling scheduling for tasks, actors, and placement group bundles using default and custom labels. See :doc:`./labels`.

Labels are a beta feature. As this feature becomes stable, the Ray team recommends using labels to replace the following patterns:

- NodeAffinitySchedulingStrategy when `soft=false`. Use the default `ray.io/node-id` label instead.
- The `accelerator_type` option for tasks and actors. Use the default `ray.io/accelerator-type` label instead.

.. note:: 

  A legacy pattern recommended using custom resources for label-based scheduling. We now recommend only using custom resources when you need to manage scheduling using numeric values. 

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
If there are feasible nodes, Ray will either choose an available node or wait until an unavailable node to become available
depending on other factors discussed below.
If all nodes are infeasible, the task or actor cannot be scheduled until feasible nodes are added to the cluster.

.. _ray-scheduling-strategies:

Scheduling Strategies
---------------------

Tasks or actors support a :func:`scheduling_strategy <ray.remote>` option to specify the strategy used to decide the best node among feasible nodes.
Currently the supported strategies are the followings.

"DEFAULT"
~~~~~~~~~

``"DEFAULT"`` is the default strategy used by Ray.
Ray schedules tasks or actors onto a group of the top k nodes.
Specifically, the nodes are sorted to first favor those that already have tasks or actors scheduled (for locality),
then to favor those that have low resource utilization (for load balancing).
Within the top k group, nodes are chosen randomly to further improve load-balancing and mitigate delays from cold-start in large clusters.

Implementation-wise, Ray calculates a score for each node in a cluster based on the utilization of its logical resources.
If the utilization is below a threshold (controlled by the OS environment variable ``RAY_scheduler_spread_threshold``, default is 0.5), the score is 0,
otherwise it is the resource utilization itself (score 1 means the node is fully utilized).
Ray selects the best node for scheduling by randomly picking from the top k nodes with the lowest scores.
The value of ``k`` is the max of (number of nodes in the cluster * ``RAY_scheduler_top_k_fraction`` environment variable) and ``RAY_scheduler_top_k_absolute`` environment variable.
By default, it's 20% of the total number of nodes.

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

    labels
    resources
    accelerators
    placement-group
    memory-management
    ray-oom-prevention
