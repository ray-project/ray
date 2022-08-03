.. include:: /_includes/clusters/announcement.rst

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-index-under-construction:


Ray Clusters Overview
=====================

What is a Ray cluster?
----------------------

One of Ray's strengths is the ability to leverage multiple machines for
distributed execution. Ray can, of course, be run on a single machine (and is
done so often), but the real power is using Ray on a cluster of machines.

A Ray cluster is a set of one or more nodes that are running Ray and share the same :ref:`head node<cluster-head-node-under-construction>`.
Ray clusters can be a fixed-size number of nodes, or :ref:`autoscale<cluster-autoscaler-under-construction>` the cluster size up and down according to the demand of the Ray workload.

How can I use Ray clusters?
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray clusters are officially supported on the following technology stacks:

* The :ref:`Ray Cluster Launcher on AWS and GCP<ref-cluster-quick-start-vms-under-construction>`. Community-supported Azure and Aliyun integrations also exist.
* :ref:`KubeRay, the official way to run Ray on Kubernetes<kuberay-index>`.

Advanced users may want to :ref:`deploy Ray clusters on-premise<cluster-private-setup-under-construction>` or even onto infrastructure platforms not listed here by :ref:`providing a custom node provider<additional-cloud-providers-under-construction>`.

Where to go from here?
----------------------

.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **I want to run Ray on a cloud provider** 
    ^^^
    This guide helps you take a sample application designed to run on a laptop and
    scale it up in the cloud. Access to an AWS or GCP account is required.

    +++
    .. link-button:: ref-cluster-quick-start-vms-under-construction
        :type: ref
        :text: Getting Started with Ray Clusters on VMs
        :classes: btn-outline-info btn-block
    ---

    **I want to run Ray on Kubernetes**
    ^^^
    This guide helps you deploy a Ray application to a Kubernetes cluster.
    You can run the tutorial on a remote Kubernetes cluster or on your laptop via KinD.

    +++
    .. link-button:: kuberay-quickstart
        :type: ref
        :text: Getting Started with Ray on Kubernetes
        :classes: btn-outline-info btn-block
    ---

    **I want to learn key Ray concepts**
    ^^^
    Understand the key concepts behind Ray Clusters. Learn about the main
    concepts and the different ways to interact with a cluster.

    +++
    .. link-button:: cluster-key-concepts-under-construction
        :type: ref
        :text: Learn Key Concepts
        :classes: btn-outline-info btn-block

.. include:: /_includes/clusters/announcement_bottom.rst
