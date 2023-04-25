.. _fault-tolerance-gcs:

GCS Fault Tolerance
===================

Global Control Service (GCS) is a server that manages cluster-level metadata.
It also provides a handful of cluster-level operations including :ref:`actor <ray-remote-classes>`, :ref:`placement groups <ray-placement-group-doc-ref>` and node management.
By default, the GCS is not fault tolerant since all the data is stored in-memory and its failure means that the entire Ray cluster fails.
To make the GCS fault tolerant, HA Redis is required.
Then, when the GCS restarts, it loads all the data from the Redis instance and resumes regular functions.

During the recovery period, the following functions are not available:

- Actor creation, deletion and reconstruction.
- Placement group creation, deletion and reconstruction.
- Resource management.
- Worker node registration.
- Worker process creation.

However, running Ray tasks and actors remain alive and any existing objects will continue to be available.

Setting up Redis
----------------

.. tab-set::

    .. tab-item:: KubeRay (officially supported)

        If you are using :ref:`KubeRay <kuberay-index>`, please refer to `KubeRay docs on GCS Fault Tolerance <https://ray-project.github.io/kuberay/guidance/gcs-ft/>`_.

    .. tab-item:: ray start

        If you are using :ref:`ray start <ray-start-doc>` to start the Ray head node,
        set the OS environment ``RAY_REDIS_ADDRESS`` to
        the Redis address, and supply the ``--redis-password`` flag with the password when calling ``ray start``:

        .. code-block:: shell

          RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD

    .. tab-item:: ray up

        If you are using :ref:`ray up <ray-up-doc>` to start the Ray cluster, change :ref:`head_start_ray_commands <cluster-configuration-head-start-ray-commands>` field to add ``RAY_REDIS_ADDRESS`` and ``--redis-password`` to the ``ray start`` command:

        .. code-block:: yaml

          head_start_ray_commands:
            - ray stop
            - ulimit -n 65536; RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --dashboard-host=0.0.0.0

    .. tab-item:: Kubernetes

        If you are using Kubernetes but not :ref:`KubeRay <kuberay-index>`, please refer to :ref:`this doc <deploy-a-static-ray-cluster-without-kuberay>`.


Once the GCS is backed by Redis, when it restarts, it'll recover the
state by reading from Redis. When the GCS is recovering from its failed state, the raylet
will try to reconnect to the GCS.
If the raylet fails to reconnect to the GCS for more than 60 seconds,
the raylet will exit and the corresponding node fails.
This timeout threshold can be tuned by the OS environment variable ``RAY_gcs_rpc_server_reconnect_timeout_s``.

You can also set the OS environment variable ``RAY_external_storage_namespace`` to isolate the data stored in Redis.
This makes sure that there is no data conflicts if multiple Ray clusters share the same Redis instance.

If the IP address of GCS will change after restarts, it's better to use a qualified domain name
and pass it to all raylets at start time. Raylet will resolve the domain name and connect to
the correct GCS. You need to ensure that at any time, only one GCS is alive.

.. note::

  GCS fault tolerance with external Redis is officially supported
  ONLY if you are using :ref:`KubeRay <kuberay-index>` for :ref:`Ray serve fault tolerance <serve-e2e-ft>`.
  For other cases, you can use it at your own risk and
  you need to implement additional mechanisms to detect the failure of GCS or the head node
  and restart it.
