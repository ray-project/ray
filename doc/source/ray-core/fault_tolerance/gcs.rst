.. _fault-tolerance-gcs:

GCS Fault Tolerance
===================

Global Control Service (GCS) is a server that manages cluster-level metadata.
It also provides a handful of cluster-level operations including :ref:`actor <ray-remote-classes>`, :ref:`placement group <ray-placement-group-doc-ref>` and node management.
By default, GCS is not fault tolerant since all the data is stored in-memory and its failure means that the entire Ray cluster fails.
To make GCS fault tolerant, HA storage is required. Ray supports Redis as the backend storage for durability and high availability.
Then, when GCS restarts, it loads all the data from the Redis instance and resumes regular functions.

During the recovery period, the following functions are not available:

- Actor creation, deletion and reconstruction.
- Placement group creation, deletion and reconstruction.
- Resource management.
- Worker node registration.
- Worker process creation.

But the running Ray tasks and actors remain alive and any existing objects will continue to be available.

Setting up Redis
----------------

.. tabbed:: KubeRay (Recommended)

    If you are using :ref:`KubeRay <kuberay-index>`, please refer to `KubeRay docs on GCS Fault Tolerance <https://ray-project.github.io/kuberay/guidance/gcs-ft/>`_.

.. tabbed:: ray start

    If you are using :ref:`ray start <ray-start-doc>` to start the Ray head node,
    set the OS environment ``RAY_REDIS_ADDRESS`` to
    the Redis address, and supply the ``--redis-password`` flag with the password when calling ``ray start``:

    .. code-block:: shell

      RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD

.. tabbed:: ray up

    If you are using :ref:`ray up <ray-up-doc>` to start the Ray cluster, change :ref:`head_start_ray_commands <cluster-configuration-head-start-ray-commands>` field to add ``RAY_REDIS_ADDRESS`` and ``--redis-password`` to the ``ray start`` command:

    .. code-block:: yaml

      head_start_ray_commands:
        - ray stop
        - ulimit -n 65536; RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --dashboard-host=0.0.0.0

.. tabbed:: Kubernetes

    If you are using Kubernetes but not :ref:`KubeRay <kuberay-index>`, please refer to :ref:`this doc <deploy-a-static-ray-cluster-without-kuberay>`.


Once GCS is backed by Redis, when it restarts, it'll recover the
state by reading from Redis. When the GCS is recovering from its failed state, the raylet
will try to reconnect to the GCS.
If the raylet failes to reconnect to the GCS for more than 60 seconds,
the raylet will exit and the corresponding node fails.
This timeout threshold can be tuned by the OS environment variable ``RAY_gcs_rpc_server_reconnect_timeout_s``.

If the IP address of GCS will change after restarts, it's better to use a qualified domain name
and pass it to all raylets at start time. Raylet will resolve the domain name and connect to
the correct GCS. You need to ensure that at any time, only one GCS is alive.

.. note::

  Unless you are using :ref:`KubeRay <kuberay-index>`,
  you also need to implement a mechanism to detect the failure of GCS or the head node
  and restart it automatically in addition to setting up the external Redis instance.
