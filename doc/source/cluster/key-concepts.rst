.. warning::
    This page is under construction!

.. include:: /_includes/clusters/we_are_hiring.rst

Key Concepts
============

.. _cluster-key-concepts:


This page introduces the following key concepts concerning Ray clusters:

.. contents::
    :local:

Ray cluster
------------
A **Ray cluster** is comprised of a :ref:`head node<cluster-head-node>`
and any number of :ref:`worker nodes<cluster-worker-nodes>`.

.. figure:: images/ray-cluster.svg
    :align: center
    :width: 600px
    
    *A Ray cluster with two worker nodes. Each node runs Ray helper processes to
    facilitate distributed scheduling and memory management. The head node runs
    additional control processes, which are highlighted.*

The number of worker nodes in a cluster may change with application demand, according
to your Ray cluster configuration. This is known as *autoscaling*. The head node runs
the :ref:`autoscaler<cluster-autoscaler>`.

.. note::
    Ray nodes are implemented as pods when :ref:`running on Kubernetes<kuberay-index>`.

Users can submit jobs for execution on the Ray cluster, or can interactively use the
cluster by connecting to the head node and running `ray.init`. See
:ref:`Clients and Jobs<cluster-clients-and-jobs>` for more information.

.. _cluster-worker-nodes:

Worker nodes
~~~~~~~~~~~~
**Worker nodes** execute a Ray application by executing tasks and actors and storing Ray objects. Each worker node runs helper processes which
implement distributed scheduling and :ref:`memory management<memory>`.

.. _cluster-head-node:

Head node
~~~~~~~~~
Every Ray cluster has one node which is designated as the **head node** of the cluster.
The head node is identical to other worker nodes, except that it also runs singleton processes responsible for cluster management such as the
:ref:`autoscaler<cluster-autoscaler>` and the Ray driver processes
:ref:`which run Ray jobs<cluster-clients-and-jobs>`. Ray may schedule
tasks and actors on the head node just like any other worker node, unless configured otherwise.

.. _cluster-autoscaler:

Autoscaler
----------

The **autoscaler** is a process that runs on the :ref:`head node<cluster-head-node>` (or as a sidecar container in the head pod if :ref:`using Kubernetes<kuberay-index>`).
It is responsible for provisioning or deprovisioning :ref:`worker nodes<cluster-worker-nodes>`
to meet the needs of the Ray workload. In particular, if the resource demands of the Ray workload exceed the
current capacity of the cluster, the autoscaler will attempt to add more nodes. Conversely, if
a node is idle for long enough, the autoscaler will remove it from the cluster.

To learn more about the autoscaler and how to configure it, refer to the following user guides:

* :ref:`Configuring Autoscaling on VMs<deployment-guide-autoscaler>`.
* :ref:`Autoscaling on Kubernetes<kuberay-autoscaler-discussion>`.

.. _cluster-clients-and-jobs:

..

Clients and Jobs
----------------

Clients and Jobs
~~~~~~~~~~~~~~~~
Ray provides two methods for running workloads on a Ray Cluster: the Ray Client, and Ray Job Submission.

* **The Ray Client** enables interactive development by connecting a local Python script or shell to the cluster.
  Developers can scale-out their local programs on the cloud as if it were on their laptop. The Ray Client is used
  by specifying the :ref:`head node<cluster-head-node>` address as an argument to `ray.init`.
* **Ray Job Submission** enables users to submit locally developed-and-tested applications to a remote Ray
  Cluster. Ray Job Submission simplifies the experience of packaging, deploying, and managing a Ray application.

To learn how to run workloads on a Ray Cluster, refer to the following user guides:

* :ref:`Running Ray workloads on VMs<ref-deployment-guide>`.
* The :ref:`Ray Job Submission<kuberay-job>` and :ref:`Ray Client<kuberay-client>` sections in :ref:`Getting Started with Ray on Kubernetes<kuberay-quickstart>`.
