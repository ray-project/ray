.. _ref-autoscaling:

Cluster Autoscaling
===================

.. tip:: Before you continue, be sure to have read :ref:`cluster-cloud`.

Basics
------

The Ray Cluster Launcher will automatically enable a load-based autoscaler. The scheduler will look at the task, actor, and placement group resource demands from the cluster, and tries to add the minimum set of nodes that can fulfill these demands. When nodes are idle for more than a timeout, they will be removed, down to the ``min_workers`` limit. The head node is never removed.

To avoid launching too many nodes at once, the number of nodes allowed to be pending is limited by the ``upscaling_speed`` setting. By default it is set to ``1.0``, which means the cluster can be growing in size by at most ``100%`` at any time (e.g., if the cluster currently has 20 nodes, at most 20 pending launches are allowed). This fraction can be set to as high as needed, e.g., ``99999`` to allow the cluster to quickly grow to its max size.

In more detail, the autoscaler implements the following control loop:

 1. It calculates the number of nodes required to satisfy all currently pending tasks, actor, and placement group requests.
 2. If the number of nodes required total divided by the number of current nodes exceeds ``1 + upscaling_speed``, then the number of nodes launched will be limited by that threshold.
 3. If a node is idle for a timeout (5 minutes by default), it is removed from the cluster.

The basic autoscaling config settings are as follows:

.. code-block:: yaml

    # An unique identifier for the head node and workers of this cluster.
    cluster_name: default

    # The minimum number of workers nodes to launch in addition to the head
    # node. This number should be >= 0.
    min_workers: 0

    # The autoscaler will scale up the cluster faster with higher upscaling speed.
    # E.g., if the task requires adding more nodes then autoscaler will gradually
    # scale up the cluster in chunks of upscaling_speed*currently_running_nodes.
    # This number should be > 0.
    upscaling_speed: 1.0

    # If a node is idle for this many minutes, it will be removed. A node is
    # considered idle if there are no tasks or actors running on it.
    idle_timeout_minutes: 5

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


Multiple Node Type Autoscaling
------------------------------

Ray supports multiple node types in a single cluster. In this mode of operation, the scheduler will choose the types of nodes to add based on the resource demands, instead of always adding the same kind of node type.

The concept of a cluster node type encompasses both the physical instance type (e.g., AWS p3.8xl GPU nodes vs m4.16xl CPU nodes), as well as other attributes (e.g., IAM role, the machine image, etc). `Custom resources <configure.html>`__ can be specified for each node type so that Ray is aware of the demand for specific node types at the application level (e.g., a task may request to be placed on a machine with a specific role or machine image via custom resource).

An example of configuring multiple node types is as follows `(full example) <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-multi-node-type.yaml>`__:

.. code-block:: yaml

    # Specify the allowed node types and the resources they provide.
    # The key is the name of the node type, which is just for debugging purposes.
    # The node config specifies the launch config and physical instance type.
    available_node_types:
        cpu_4_ondemand:
            node_config:
                InstanceType: m4.xlarge
            # For AWS instances, autoscaler will automatically add the available
            # CPUs/GPUs/accelerator_type ({"CPU": 4} for m4.xlarge) in "resources".
            # resources: {"CPU": 4}
            min_workers: 1
            max_workers: 5
        cpu_16_spot:
            node_config:
                InstanceType: m4.4xlarge
                InstanceMarketOptions:
                    MarketType: spot
            # Autoscaler will auto fill the CPU resources below.
            resources: {"Custom1": 1, "is_spot": 1}
            max_workers: 10
        gpu_1_ondemand:
            node_config:
                InstanceType: p2.xlarge
            # Autoscaler will auto fill the CPU/GPU resources below.
            resources: {"Custom2": 2}
            max_workers: 4
            worker_setup_commands:
                - pip install tensorflow-gpu  # Example command.
        gpu_8_ondemand:
            node_config:
                InstanceType: p3.8xlarge
            # Autoscaler autofills the "resources" below.
            # resources: {"CPU": 32, "GPU": 4, "accelerator_type:V100": 1}
            max_workers: 2
            worker_setup_commands:
                - pip install tensorflow-gpu  # Example command.

    # Specify the node type of the head node (as configured above).
    head_node_type: cpu_4_ondemand


The above config defines two CPU node types (``cpu_4_ondemand`` and ``cpu_16_spot``), and two GPU types (``gpu_1_ondemand`` and ``gpu_8_ondemand``). Each node type has a name (e.g., ``cpu_4_ondemand``), which has no semantic meaning and is only for debugging. Let's look at the inner fields of the ``gpu_1_ondemand`` node type:

The node config tells the underlying Cloud provider how to launch a node of this type. This node config is merged with the top level node config of the YAML and can override fields (i.e., to specify the p2.xlarge instance type here):

.. code-block:: yaml

    node_config:
        InstanceType: p2.xlarge

The resources field tells the autoscaler what kinds of resources this node provides. This can include custom resources as well (e.g., "Custom2"). This field enables the autoscaler to automatically select the right kind of nodes to launch given the resource demands of the application. The resources specified here will be automatically passed to the ``ray start`` command for the node via an environment variable. For more information, see also the `resource demand scheduler <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/resource_demand_scheduler.py>`__:

.. code-block:: yaml

    resources: {"CPU": 4, "GPU": 1, "Custom2": 2}

The ``min_workers`` and ``max_workers`` fields constrain the minimum and maximum number of nodes of this type to launch, respectively:

.. code-block:: yaml

    min_workers: 1
    max_workers: 4

The ``worker_setup_commands`` field (and also the ``initialization_commands`` field, not shown) can be used to override the setup and initialization commands for a node type. Note that you can only override the setup for worker nodes. The head node's setup commands are always configured via the top level field in the cluster YAML:

.. code-block:: yaml

    worker_setup_commands:
        - pip install tensorflow-gpu  # Example command.

Docker Support for Multi-type clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For each node type, you can specify ``worker_image`` and ``pull_before_run`` fields. These will override any top level ``docker`` section values (see :ref:`autoscaler-docker`). The ``worker_run_options`` field is combined with top level ``docker: run_options`` field to produce the docker run command for the given node_type.  Ray will automatically select the Nvidia docker runtime if it is available.

The following configuration is for a GPU enabled node type:

.. code-block:: yaml

    available_node_types:
        gpu_1_ondemand:
            max_workers: 2
            worker_setup_commands:
                - pip install tensorflow-gpu  # Example command.

            # Docker specific commands for gpu_1_ondemand
            pull_before_run: True
            worker_image:
                - rayproject/ray-ml:latest-gpu
            worker_run_options:  # Appended to top-level docker field.
                - "-v /home:/home"
