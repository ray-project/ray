.. _fault-tolerance-internal-system:

Advanced topic: Ray system failure model
========================================

Here, we describe the system-level components of Ray and how failures in those
components can affect tasks, objects, and actors.

Worker failure
--------------

When the worker failed, all the objects or actors owned by this worker will be
lost. Workers could fail because of the node failure, raylet failure or the
users code make it crashed.

Raylet failure
--------------

When the raylet failed, all the workers created by this raylet will fail too,
which will cause objects copies lost, actors and objects owner die. Each raylet
is associated with a unique id, so even if the raylet restarts on the same node,
it'll be treated as a new raylet to the Ray cluster. The placement group
scheduled on this node will be rescheduled by GCS and recreated.

Raylet could fail because of a node failure. It can also fail if it failed to
send heartbeats to the GCS within 30 seconds. This could happen if the network
is very bad, or the raylet is too busy to send the heartbeats. It could also
because the GCS is too busy to process these heartbeats. This threshold can be
tuned by OS environment variable ``RAY_num_heartbeats_timeout`` when starting the ray
cluster. There is another OS environment variable
``RAY_gcs_failover_worker_reconnect_timeout`` which is the initial value for all
raylets to reconnect to this GCS after the GCS is restarted. This is useful if
the system needs more time to recover in case of GCS failure.

Raylet could also fail if it failed to connect to the GCS for more than 60
seconds. This could happen either because GCS failed for a long time and never
come back or the network is really bad. This threshold can be tuned by OS
environment ``RAY_gcs_rpc_server_reconnect_timeout_s``.

.. _fault-tolerance-gcs:

GCS failure
-----------

GCS failure usually means the Ray cluster failure and everything will be lost.
To make GCS fault-tolerant, HA storage is needed to be used. Ray support using
Redis as the backend storage. The user needs to set up the storage first and make
GCS connected to the Redis.

To use this feature, the OS environment ``RAY_REDIS_ADDRESS`` needs to be set to
the Redis address and ``--redis-password`` needs to be passed when calling ``ray
start``. For example,
``RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD``.

Once it's backed by persistent storage, when GCS restarts, it'll recover the
status by reading from the DB. When GCS failed and is recovering, the raylet
will try to reconnect to the GCS. If the raylet failed to reconnect to the GCS
for more than 60 seconds, the raylet will crash itself. This threshold can be
tuned by OS environment variable ``RAY_gcs_rpc_server_reconnect_timeout_s``.
If the IP address of the GCS can change, a domain name should be used and passed
to all raylets when start. Raylet will resolve the domain name and connect to
the correct GCS. The user needs to ensure that at the same time, only one GCS is
alive.

.. note::
    KubeRay has supported fault-tolerance GCS. For the users who don't have experience
    setting up this, please refer to `KubeRay GCS FT <https://github.com/ray-project/kuberay/blob/master/docs/guidance/gcs-ft.md>`_
