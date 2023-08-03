.. _vms-autoscaling:

Configuring Autoscaling
=======================

This guide explains how to configure the Ray autoscaler using the Ray cluster launcher.
The Ray autoscaler is a Ray cluster process that automatically scales a cluster up and down based on resource demand.
The autoscaler does this by adjusting the number of nodes in the cluster based on the resources required by tasks, actors or placement groups.

Note that the autoscaler only considers logical resource requests for scaling (i.e., those specified in ``@ray.remote`` and displayed in `ray status`), not physical machine utilization. If a user tries to launch an actor, task, or placement group but there are insufficient resources, the request will be queued. The autoscaler adds nodes to satisfy resource demands in this queue.
The autoscaler also removes nodes after they become idle for some time.
A node is considered idle if it has no active tasks, actors, or objects.

.. tip::
  **When to use Autoscaling?**

  Autoscaling can reduce workload costs, but adds node launch overheads and can be tricky to configure.
  We recommend starting with non-autoscaling clusters if you're new to Ray.

Cluster Config Parameters
-------------------------

The following options are available in your cluster config file.
It is recommended that you set these before launching your cluster, but you can also modify them at run-time by updating the cluster config.

`max_workers[default_value=2, min_value=0]`: The max number of cluster worker nodes to launch. Note that this does not include the head node.

`min_workers[default_value=0, min_value=0]`: The min number of cluster worker nodes to launch, regardless of utilization. Note that this does not include the head node. This number must be less than the ``max_workers``.

.. note::

  If `max_workers` is modified at runtime, the autoscaler will immediately remove nodes until this constraint
  is satisfied. This may disrupt running workloads.

If you are using more than one node type, you can also set min and max workers for each individual type:

`available_node_types.<node_type_name>.max_workers[default_value=cluster max_workers, min_value=0]`: The maximum number of worker nodes of a given type to launch. This number must be less than or equal to the `max_workers` for the cluster.


`available_node_types.<node_type_name>.min_workers[default_value=0, min_value=0]`: The minimum number of worker nodes of a given type to launch, regardless of utilization. The sum of `min_workers` across all node types must be less than or equal to the `max_workers` for the cluster.

Upscaling and downscaling speed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If needed, you can also control the rate at which nodes should be added to or removed from the cluster. For applications with many short-lived tasks, you may wish to adjust the upscaling and downscaling speed to be more conservative.

`upscaling_speed[default_value=1.0, min_value=1.0]`: The number of nodes allowed to be pending as a multiple of the current number of nodes. The higher the value, the more aggressive upscaling will be. For example, if this is set to 1.0, the cluster can grow in size by at most 100% at any time, so if the cluster currently has 20 nodes, at most 20 pending
launches are allowed. The minimum number of pending launches is 5 regardless of this setting.

`idle_timeout_minutes[default_value=5, min_value=0]`: The number of minutes that need to pass before an idle worker node is removed by the
autoscaler. The smaller the value, the more aggressive downscaling will be. Worker nodes are considered idle when they hold no active tasks, actors, or referenced objects (either in-memory or spilled to disk). This parameter does not affect the head node.

Programmatic Scaling
--------------------

For more information on programmatic access to the autoscaler, see the :ref:`Programmatic Cluster Scaling Guide <ref-autoscaler-sdk>`.
