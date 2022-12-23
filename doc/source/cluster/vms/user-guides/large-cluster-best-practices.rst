.. _vms-large-cluster:

Best practices for deploying large clusters
-------------------------------------------

This section aims to document best practices for deploying Ray clusters at
large scale.

Networking configuration
^^^^^^^^^^^^^^^^^^^^^^^^

End users should only need to directly interact with the head node of the
cluster. In particular, there are 2 services which should be exposed to users:

1. The dashboard
2. The Ray client server

.. note::

  While users only need 2 ports to connect to a cluster, the nodes within a
  cluster require a much wider range of ports to communicate.

  See :ref:`Ray port configuration <Ray-ports>` for a comprehensive list.

  Applications (such as :ref:`Ray Serve <Rayserve>`) may also require
  additional ports to work properly.

System configuration
^^^^^^^^^^^^^^^^^^^^

There are a few system level configurations that should be set when using Ray
at a large scale.

* Make sure ``ulimit -n`` is set to at least 65535. Ray opens many direct
  connections between worker processes to avoid bottlenecks, so it can quickly
  use a large number of file descriptors.
* Make sure ``/dev/shm`` is sufficiently large. Most ML/RL applications rely
  heavily on the plasma store. By default, Ray will try to use ``/dev/shm`` for
  the object store, but if it is not large enough (i.e. ``--object-store-memory``
  > size of ``/dev/shm``), Ray will write the plasma store to disk instead, which
  may cause significant performance problems.
* Use NVMe SSDs (or other high perforfmance storage) if possible. If
  :ref:`object spilling <object-spilling>` is enabled Ray will spill objects to
  disk if necessary. This is most commonly needed for data processing
  workloads.

.. _vms-large-cluster-configure-head-node:

Configuring the head node
^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the above changes, when deploying a large cluster, Ray's
architecture means that the head node will have extra stress due to GCS.

* Make sure the head node has sufficient bandwidth. The most heavily stressed
  resource on the head node is outbound bandwidth. For large clusters (see the
  scalability envelope), we recommend using machines networking characteristics
  at least as good as an r5dn.16xlarge on AWS EC2.
* Set ``resources: {"CPU": 0}`` on the head node.
  (For Ray clusters deployed using KubeRay,
  set ``rayStartParams: {"CPU": "0"}``.
  See the :ref:`configuration guide for KubeRay clusters<kuberay-num-cpus>`.)
  Due to the heavy networking load (and the GCS and dashboard processes), we
  recommend setting the number of CPUs to 0 on the head node to avoid
  scheduling additional tasks on it.

Configuring the autoscaler
^^^^^^^^^^^^^^^^^^^^^^^^^^

For large, long running clusters, there are a few parameters that can be tuned.

* Ensure your quotas for node types are set correctly.
* For long running clusters, set the ``AUTOSCALER_MAX_NUM_FAILURES`` environment
  variable to a large number (or ``inf``) to avoid unexpected autoscaler
  crashes. The variable can be set by prepending \ ``export AUTOSCALER_MAX_NUM_FAILURES=inf;``
  to the head node's Ray start command.
  (Note: you may want a separate mechanism to detect if the autoscaler
  errors too often).
* For large clusters, consider tuning ``upscaling_speed`` for faster
  autoscaling.

Picking nodes
^^^^^^^^^^^^^

Here are some tips for how to set your ``available_node_types`` for a cluster,
using AWS instance types as a concrete example.

General recommendations with AWS instance types:

**When to use GPUs**

* If you’re using some RL/ML framework
* You’re doing something with tensorflow/pytorch/jax (some framework that can
  leverage GPUs well)

**What type of GPU?**

* The latest gen GPU is almost always the best bang for your buck (p3 > p2, g4
  > g3), for most well designed applications the performance outweighs the
  price (the instance price may be higher, but you’ll use the instance for less
  time.
* You may want to consider using older instances if you’re doing dev work and
  won’t actually fully utilize the GPUs though.
* If you’re doing training (ML or RL), you should use a P instance. If you’re
  doing inference, you should use a G instance. The difference is
  processing:VRAM ratio (training requires more memory).

**What type of CPU?**

* Again stick to the latest generation, they’re typically cheaper and faster.
* When in doubt use M instances, they have typically have the highest
  availability.
* If you know your application is memory intensive (memory utilization is full,
  but cpu is not), go with an R instance
* If you know your application is CPU intensive go with a C instance
* If you have a big cluster, make the head node an instance with an n (r5dn or
  c5n)

**How many CPUs/GPUs?**

* Focus on your CPU:GPU ratio first and look at the utilization (Ray dashboard
  should help with this). If your CPU utilization is low add GPUs, or vice
  versa.
* The exact ratio will be very dependent on your workload.
* Once you find a good ratio, you should be able to scale up and and keep the
  same ratio.
* You can’t infinitely scale forever. Eventually, as you add more machines your
  performance improvements will become sub-linear/not worth it. There may not
  be a good one-size fits all strategy at this point.

.. note::

   If you're using RLlib, check out :ref:`the RLlib scaling guide
   <rllib-scaling-guide>` for RLlib specific recommendations.
