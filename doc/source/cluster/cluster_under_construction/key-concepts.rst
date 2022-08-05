.. include:: /_includes/clusters/we_are_hiring.rst

Key Concepts
============

.. _cluster-key-concepts-under-construction:


This page introduces the following key concepts concerning Ray clusters:

.. contents::
    :local:

.. _cluster-worker-nodes-under-construction:

Worker nodes
------------
A Ray cluster consists of a set of one or more **worker nodes**. Ray tasks and actors
can be scheduled onto the worker nodes. Each worker node runs helper processes which
implement distributed scheduling and :ref:`memory management<memory>`.

.. figure:: ray-cluster.jpg
    :align: center
    :width: 600px

    A Ray cluster consists of a set of one or more worker nodes, one of which is designated
    as the head node. Each node runs Ray helper processes and Ray application code.

The number of worker nodes in a cluster may change with application demand, according
to your Ray cluster configuration. The :ref:`head node<cluster-head-node-under-construction>`
runs the logic which implements autoscaling.

.. note::
    Here, a *node* means an individual machine. However, :ref:`on Kubernetes<kuberay-index>`, nodes are implemented as pods.

.. _cluster-head-node-under-construction:

Head node
---------
Every Ray cluster has one :ref:`worker node<cluster-worker-nodes-under-construction>`
which is designated as the **head node** of the cluster. The head node runs
important processes such as the :ref:`autoscaler<cluster-autoscaler-under-construction>`
and the Ray driver processes, :ref:`which run the top-level Ray application
<cluster-clients-and-jobs-under-construction>`. Ray may schedule tasks and actors
on the head node just like any other worker node.

.. note::
    :ref:`On Kubernetes<kuberay-index>`, the autoscaler process runs inside a sidecar container in the head pod,
    instead of in the same container (which would be most analogous to how the autoscaler runs inside the head node
    on non-Kubernetes clusters).

.. _cluster-autoscaler-under-construction:

Autoscaler
----------

The autoscaler is a process that runs on the :ref:`head node<cluster-head-node-under-construction>`.
It is responsible for provisioning or deprovisioning :ref:`worker nodes<cluster-worker-nodes-under-construction>`
to meet the needs of the Ray workload. In particular, if the resource demands of the Ray workload exceed the
current capacity of the cluster, the autoscaler will attempt to add more nodes. Conversely, if
a node is idle for long enough, the autoscaler will remove it from the cluster.

To learn more about the autoscaler and how to configure it, refer to the following user guides:

* :ref:`Configuring Autoscaling on VMs<deployment-guide-autoscaler-under-construction>`.
* :ref:`Configuring Autoscaling on Kubernetes<kuberay-index>` (TODO cade@ update this link.)

.. _cluster-clients-and-jobs-under-construction:

Clients and Jobs
----------------
Ray provides two methods for running workloads on a Ray Cluster: the Ray Client, and Ray Job Submission.

* **The Ray Client** enables interactive development by connecting a local Python script or shell to the cluster.
  Developers can scale-out their local programs on the cloud as if it were on their laptop. The Ray Client is used
  by specifying the :ref:`head node<cluster-head-node-under-construction>` address as an argument to `ray.init`.
* **Ray Job Submission** enables users to submit locally developed-and-tested applications to a remote Ray
  Cluster. Ray Job Submission simplifies the experience of packaging, deploying, and managing a Ray application.

To learn how to run workloads on a Ray Cluster, refer to the following user guides:

* :ref:`Running Ray workloads on VMs<ref-cluster-quick-start-vms-under-construction>` (TODO cade@ update this link).
* :ref:`Running Ray workloads on Kubernetes<kuberay-index>` (TODO cade@ update this link).
