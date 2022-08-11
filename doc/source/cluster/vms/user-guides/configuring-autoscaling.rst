.. include:: /_includes/clusters/we_are_hiring.rst

.. _deployment-guide-autoscaler-under-construction:

Configuring Autoscaling
=======================

This guide explains how to configure the Ray autoscaler. The Ray autoscaler adjusts
the number of nodes in the cluster based on the resources required by tasks, actors or
placement groups.

Note that the autoscaler only considers logical resource requests for scaling (i.e., those specified
in ``@ray.remote`` and displayed in ``ray status``), not machine utilization.
If a user tries to launch an actor, task, or placement group but there are insufficient resources, the request will be queued.
The autoscaler adds nodes to satisfy resource demands in the queue, and removes nodes when they become idle.

Parameters
==========

The following are the autoscaling parameters that are specified with cluster launch. They can also be modified at runtime by
updating the cluster config.

**max_workers[default_value=2, min_value=0]**: Specify the max number of cluster worker nodes. Note that this excludes the head node.

.. note::

  If this value is modified at runtime, the autoscaler will immediately remove nodes until this constraint
  is satisfied. This may disrupt running workloads.

**upscaling_speed[default_value=1.0, min_value=1.0]**: The number of nodes allowed to be pending as a multiple of the current number of nodes.
For example, if this is set to 1.0, the cluster can grow in size by at most 100% at any time, so if the cluster currently has 20 nodes, at most 20 pending
launches are allowed. The minimum number of pending launches is 5 regardless of this setting.

**idle_timeout_minutes[default_value=5, min_value=0]**: The number of minutes that need to pass before an idle worker node is removed by the
autoscaler. Worker nodes are idle when they hold no active tasks, actors, or referenced objects (either in-memory or spilled to disk). This parameter does not affect the head node.

**available_node_types.<node_type_name>.min_workers[default_value=0, min_value=0]**: The minimum number of worker nodes of a given type to launch. If this number
is set to greater than zero, the autoscaler will maintain the number of nodes regardless of utilization. The sum of the min worker of all the node types
must be less than or equal to the max_workers for the cluster.

**available_node_types.<node_type_name>.max_workers[default_value=cluster max_workers, min_value=0]**: The maximum number of worker nodes of a given type to launch. This must be
greater than or equal to available_node_types.<node_type_name>.min_workers. It must be less than or equal to the max_workers for the cluster.

.. note::
  If this value is modified at runtime, the autoscaler will immediately remove nodes until this constraint
  is satisfied. This may disrupt running workloads.

Autoscaler SDK
==============

For more information on programmatic access to the autoscaler, see :ref:`Autoscaler SDK<ref-autoscaler-sdk-under-construction>`.
