.. _ref-autoscaling:

Cluster Autoscaling
===================

Programmatically Scaling a Cluster
----------------------------------

You can from within a Ray program command the autoscaler to scale the cluster up to a desired size with ``request_resources()`` call. The cluster will immediately attempt to scale to accomodate the requested resources, bypassing normal upscaling speed constraints.

.. autofunction:: ray.autoscaler.sdk.request_resources

Manually Adding Nodes without Resources (Unmanaged Nodes)
---------------------------------------------------------

In some cases, adding special nodes without any resources (i.e. `num_cpus=0`) may be desirable. Such nodes can be used as a driver which connects to the cluster to launch jobs.

In order to manually add a node to an autoscaled cluster, the `ray-cluster-name` tag should be set and `ray-node-type` tag should be set to `unmanaged`.

Unmanaged nodes **must have 0 resources**.

If you are using the `available_node_types` field, you should create a custom node type with `resources: {}`, and `max_workers: 0` when configuring the autoscaler.

The autoscaler will not attempt to start, stop, or update unmanaged nodes. The user is responsible for properly setting up and cleaning up unmanaged nodes.
