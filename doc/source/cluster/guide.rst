.. include:: we_are_hiring.rst

.. _deployment-guide:

Cluster Deployment Guide
========================

This page provides an overview of how to deploy a multi-node Ray cluster, including how to:

* Launch the cluster.
* Set up the autoscaler.
* Deploy a Ray application.
* Monitor a multi-node cluster.
* Best practices for setting up large Ray clusters.

Launching a Ray cluster
-----------------------

There 2 recommended ways of launching a Ray cluster are via:

1. :ref:`The cluster launcher <cluster-cloud>`
2. :ref:`The kubernetes operator <Ray-operator>`

Cluster Launcher
^^^^^^^^^^^^^^^^

The goal of :ref:`the cluster launcher <cluster-cloud>` is to make it easy to deploy a Ray cluster on
any cloud. It will:

* provision a new instance/machine using the cloud provider's SDK.
* execute shell commands to set up Ray with the provided options.
* (optionally) run any custom, user defined setup commands.  This can be useful for setting environment variables and installing packages.  (To dynamically set up environments after the cluster has been deployed, you can use :ref:`Runtime Environments<runtime-environments>`.)
* Initialize the Ray cluster.
* Deploy an autoscaler process.

Kubernetes Operator
^^^^^^^^^^^^^^^^^^^

The goal of the :ref:`Ray Kubernetes Operator <Ray-operator>` is to make it easy
to deploy a Ray cluster on an existing Kubernetes cluster.

To simplify Operator configuration, Ray provides a :ref:`a Helm chart <Ray-helm>`.
Installing the Helm chart will create an Operator Deployment.
The Operator manages autoscaling Ray clusters; each Ray node runs in its own K8s Pod.

.. _deployment-guide-autoscaler:

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


Deploying an application
------------------------

To submit an application to the Ray cluster, use the Ray :ref:`Job submission interface <jobs-overview>`.

.. code:: bash

  export RAY_ADDRESS=<your_cluster_address>:8265
  ray job submit ... -- "python script.py"


To interactively connect to a Ray cluster, connect via the :ref:`Ray Client<ray-client>`.

.. code-block:: python

  # outside python, set the ``RAY_ADDRESS`` environment variable to the address of the Ray client server
  ray.init("ray://<host>:<port>")


:ref:`Learn more about setting up the Ray client server here <Ray-client>`.

You can dynamically specify local files, Python packages, and environment variables for your
application using :ref:`Runtime Environments <runtime-environments>`.

.. note::

  When deploying an application, the job will be killed if the driver
  disconnects.

  :ref:`A detached actor <actor-lifetimes>` can be used to avoid having a long running driver.

Monitoring and observability
----------------------------

Ray comes with 3 main observability features:

1. :ref:`The dashboard <Ray-dashboard>`
2. :ref:`ray status <monitor-cluster>`
3. :ref:`Prometheus metrics <multi-node-metrics>`

Monitoring the cluster via the dashboard
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`The dashboard provides detailed information about the state of the cluster <Ray-dashboard>`,
including the running jobs, actors, workers, nodes, etc.

By default, the cluster launcher and operator will launch the dashboard, but
not publicly expose it.

If you launch your application via the cluster launcher, you can securely
portforward local traffic to the dashboard via the ``ray dashboard`` command
(which establishes an SSH tunnel). The dashboard will now be visible at
``http://localhost:8265``.

The Kubernetes Operator makes the dashboard available via a Service targeting the Ray head pod.
You can :ref:`access the dashboard <ray-k8s-dashboard>` using ``kubectl port-forward``.


Observing the autoscaler
^^^^^^^^^^^^^^^^^^^^^^^^

The autoscaler makes decisions by scheduling information, and programmatic
information from the cluster. This information, along with the status of
starting nodes, can be accessed via the ``ray status`` command.

To dump the current state of a cluster launched via the cluster launcher, you
can run ``ray exec cluster.yaml "Ray status"``.

For a more "live" monitoring experience, it is recommended that you run ``ray
status`` in a watch loop: ``ray exec cluster.yaml "watch -n 1 Ray status"``.

With the kubernetes operator, you should replace ``ray exec cluster.yaml`` with
``kubectl exec <head node pod>``.

Prometheus metrics
^^^^^^^^^^^^^^^^^^

Ray is capable of producing prometheus metrics. When enabled, Ray produces some
metrics about the Ray core, and some internal metrics by default. It also
supports custom, user-defined metrics.

These metrics can be consumed by any metrics infrastructure which can ingest
metrics from the prometheus server on the head node of the cluster.

:ref:`Learn more about setting up prometheus here. <multi-node-metrics>`

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

Configuring the head node
^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the above changes, when deploying a large cluster, Ray's
architecture means that the head node will have extra stress due to GCS.

* Make sure the head node has sufficient bandwidth. The most heavily stressed
  resource on the head node is outbound bandwidth. For large clusters (see the
  scalability envelope), we recommend using machines networking characteristics
  at least as good as an r5dn.16xlarge on AWS EC2.
* Set ``resources: {"CPU": 0}`` on the head node. (For Ray clusters deployed using Helm,
  set ``rayResources: {"CPU": 0}``.) Due to the heavy networking
  load (and the GCS and dashboard processes), we recommend setting the number of
  CPUs to 0 on the head node to avoid scheduling additional tasks on it.

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
