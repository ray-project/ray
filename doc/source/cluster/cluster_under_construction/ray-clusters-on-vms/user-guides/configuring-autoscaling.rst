.. include:: /_includes/clusters/we_are_hiring.rst

.. _deployment-guide-autoscaler-under-construction:

Configuring the Ray Autoscaler
==============================

This page provides an overview of the parameters for the Ray autoscaler. A Ray cluster by default runs an autoscaler that modifies
the number of nodes based on the resources requested by the task, actor, or placement group. If there is insufficient resource to serve
the request they will be put into pending and will not execute. The autoscaler runs a bin-packing algorithm to determine the list of nodes
the cluster should use to satisfy the resource requests, and adds / removes nodes from the cluster to maximize the cluster utilization
while providing sufficient resource to execute the requests.

.. note::
  The autoscaler currently only looks at the amount of cpu and gpu requested and adds / removes nodes accordingly. It does not take into
  the account the memory request nor the disk usage.

Parameters
==========

**max_workers[default_value=2, min_value=0]**: Indicates the maximum number of workers the cluster will have at any given time. This number
excludes the head node where there is always exactly one.

.. note::
  When this value changes and drops below the number of running workers, the autoscaler will remove nodes to satisfy the new constraint even
  if the node is running some workload. Workers that are the least recently used will be removed first.

**upscaling_speed[default_value=1.0, min_value=0.0]**: The number of nodes allowed to be pending as a multiple of the current number of nodes.
For example, if set to 1.0, the cluster can grow in size by at most 100% at any time, so if the cluster currently has 20 nodes, at most 20 pending
launches are allowed. The mininum number of pending launches is 5 regardless of this setting.

**idle_timeout_minutes[default_value=5, min_value=0]**: The number of minutes that need to pass before an idle worker node is removed by the
autoscaler. Worker nodes are idle when there are no active tasks or actors running. This parameter does not affect the head node.

.. note::
  If the Ray object store is used, and a worker node still holds objects (including spilled objects on disk), it won't be removed.

**available_node_types.<node_type_name>.min_workers[default_value=0, min_value=0]**: The minimum number of worker nodes of a given type to launch. If this number
is set to greater than zero, the autoscaler will maintain the number of nodes regardless of utilization. The sum of the min worker of all the node types
must be less than or equal to the max_workers for the cluster.

**available_node_types.<node_type_name>.max_workers[default_value=0, min_value=0]**: The maximum number of worker nodes of a given type to launch. This must be
greater than or equal to available_node_types.<node_type_name>.min_workers. It must be less than or equal to the max_workers for the cluster.

.. note::
  When this value changes and drops below the number of running workers for the given type, the autoscaler will remove nodes to satisfy the new constraint even
  if the node is running some workload. Workers that are the least recently used will be removed first.

