.. include:: /_includes/clusters/we_are_hiring.rst

.. _deployment-guide-autoscaler-under-construction:

Autoscaling with Ray
--------------------

Ray is designed to support highly elastic workloads which are most efficient on
an autoscaling cluster. At a high level, the autoscaler attempts to
launch/terminate nodes in order to ensure that workloads have sufficient
resources to run, while minimizing the idle resources.

It does this by taking into consideration:

* User specified hard limits (min/max workers).
* User specified node types (nodes in a Ray cluster do _not_ have to be
  homogenous).
* Information from the Ray core's scheduling layer about the current resource
  usage/demands of the cluster.
* Programmatic autoscaling hints.

Take a look at :ref:`the cluster reference <cluster-config>` to learn more
about configuring the autoscaler.


How does it work?
^^^^^^^^^^^^^^^^^

The Ray Cluster Launcher will automatically enable a load-based autoscaler. The
autoscaler resource demand scheduler will look at the pending tasks, actors,
and placement groups resource demands from the cluster, and try to add the
minimum list of nodes that can fulfill these demands. Autoscaler uses a simple 
binpacking algorithm to binpack the user demands into
the available cluster resources. The remaining unfulfilled demands are placed
on the smallest list of nodes that satisfies the demand while maximizing
utilization (starting from the smallest node).

**Downscaling**: When worker nodes are
idle (without active Tasks or Actors running on it) 
for more than :ref:`idle_timeout_minutes
<cluster-configuration-idle-timeout-minutes>`, they are subject to
removal from the cluster. But there are two important additional conditions
to note: 

* The head node is never removed unless the cluster is torn down.
* If the Ray Object Store is used, and a Worker node still holds objects (including spilled objects on disk), it won't be removed.



**Here is "A Glimpse into the Ray Autoscaler" and how to debug/monitor your cluster:**

2021-19-01 by Ameer Haj-Ali, Anyscale Inc.

.. youtube:: BJ06eJasdu4


