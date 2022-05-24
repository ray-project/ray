.. include:: we_are_hiring.rst

.. _cluster-key-concepts:

Key Concepts
============

Cluster
-------

A Ray cluster is a set of one or more nodes that are running Ray and share the
same :ref:`head node<cluster-node-types>`.

.. _cluster-node-types:

Node types
----------

A Ray cluster consists of a :ref:`head node<cluster-head-node>` and a set of
:ref:`worker nodes<cluster-worker-node>`.

.. image:: ray-cluster.jpg
    :align: center
    :width: 600px

.. _cluster-head-node:

Head node
~~~~~~~~~

The head node is the first node started by the
:ref:`Ray cluster launcher<cluster-launcher>` when trying to launch a Ray
cluster. Among other things, the head node holds the :ref:`Global Control Store
(GCS)<memory>` and runs the :ref:`autoscaler<cluster-autoscaler>`. Once the head
node is started, it will be responsible for launching any additional
:ref:`worker nodes<cluster-worker-node>`. The head node itself will also execute
tasks and actors to utilize its capacity.

.. _cluster-worker-node:

Worker node
~~~~~~~~~~~

A worker node is any node in the Ray cluster that is not functioning as head node.
Therefore, worker nodes are simply responsible for executing tasks and actors.
When a worker node is launched, it will be given the address of the head node to
form a cluster.

.. _cluster-launcher:

Cluster launcher
----------------

The cluster launcher is a process responsible for bootstrapping the Ray cluster
by launching the :ref:`head node<cluster-head-node>`. For more information on how
to use the cluster launcher, refer to
:ref:`cluster launcher CLI commands documentation<cluster-commands>` and the
corresponding :ref:`documentation for the configuration file<cluster-config>`.

.. _cluster-autoscaler:

Autoscaler
----------

The autoscaler is a process that runs on the :ref:`head node<cluster-head-node>`
and is responsible for adding or removing :ref:`worker nodes<cluster-worker-node>`
to meet the needs of the Ray workload while matching the specification in the
:ref:`cluster config file<cluster-config>`. In particular, if the resource
demands of the Ray workload exceed the current capacity of the cluster, the
autoscaler will try to add nodes. Conversely, if a node is idle for long enough,
the autoscaler will remove it from the cluster. To learn more about autoscaling,
refer to the :ref:`Ray cluster deployment guide<deployment-guide-autoscaler>`.

Ray Client
----------
The Ray Client is an API that connects a Python script to a remote Ray cluster.
To learn more about the Ray Client, you can refer to the :ref:`documentation<ray-client>`.

Job submission
--------------

Ray Job submission is a mechanism to submit locally developed and tested applications
to a remote Ray cluster. It simplifies the experience of packaging, deploying,
and managing a Ray application. To learn more about Ray jobs, refer to the
:ref:`documentation<ray-job-submission-api-ref>`.

Cloud clusters
--------------

If you’re using AWS, Azure, GCP or Aliyun, you can use the
:ref:`Ray cluster launcher<cluster-launcher>` to launch cloud clusters, which
greatly simplifies the cluster setup process.

Cluster managers
----------------

You can simplify the process of managing Ray clusters using a number of popular
cluster managers including :ref:`Kubernetes<ray-k8s-deploy>`,
:ref:`YARN<ray-yarn-deploy>`, :ref:`Slurm<ray-slurm-deploy>` and :ref:`LSF<ray-LSF-deploy>`.

Kubernetes (K8s) operator
-------------------------

Deployments of Ray on Kubernetes are managed by the Ray Kubernetes Operator. The
Ray Operator makes it easy to deploy clusters of Ray pods within a Kubernetes
cluster. To learn more about the K8s operator, refer to
the :ref:`documentation<ray-operator>`.
