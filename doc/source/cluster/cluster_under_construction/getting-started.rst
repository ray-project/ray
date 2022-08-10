.. include:: /_includes/clusters/announcement.rst

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-index-under-construction:


Ray Clusters Overview
=====================

What is a Ray cluster?
----------------------

One of Ray's strengths is the ability to leverage multiple machines for
distributed execution. Ray is great for multiprocessing on a single machine.
However, the real power of Ray is the ability to seamlessly scale to a cluster
of machines.

A Ray cluster is a set of one or more nodes that are running Ray and share the same :ref:`head node<cluster-head-node-under-construction>`.
Ray clusters can either be a fixed-size number of nodes or :ref:`can autoscale<cluster-autoscaler-under-construction>` (i.e., automatically provision or deprovision the number of nodes in a cluster) according to the demand of the Ray workload.

How can I use Ray clusters?
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray clusters are officially supported on the following technology stacks:

* The :ref:`Ray cluster launcher on AWS and GCP<ref-cluster-quick-start-vms-under-construction>`. Community-supported Azure and Aliyun integrations also exist.
* :ref:`KubeRay, the official way to run Ray on Kubernetes<kuberay-index>`.

Advanced users may want to [deploy Ray clusters on-premise](/cluster/cluster_under_construction/ray-clusters-on-vms/user-guides/launching-clusters/on-premises.html) or even onto infrastructure platforms not listed here by [providing a custom node provide](/cluster/cluster_under_construction/ray-clusters-on-vms/user-guides/community-supported-cluster-manager/index.html).

Where to go from here?
----------------------

.. panels::
    :container: text-center
    :column: col-lg-6 px-3 py-2
    :card:

    **I want to learn key Ray cluster concepts**
    ^^^
    Understand the key concepts and main ways of interacting with a Ray cluster.

    +++
    .. link-button:: cluster-key-concepts-under-construction
        :type: ref
        :text: Learn Key Concepts
        :classes: btn-outline-info btn-block

    ---

    **I want to run Ray on a cloud provider**
    ^^^
    Take a sample application designed to run on a laptop and scale it up in the
    cloud. Access to an AWS or GCP account is required.

    +++
    .. link-button:: ref-cluster-quick-start-vms-under-construction
        :type: ref
        :text: Getting Started with Ray Clusters on VMs
        :classes: btn-outline-info btn-block
    ---

    **I want to run Ray on Kubernetes**
    ^^^
    Deploy a Ray application to a Kubernetes cluster. You can run the tutorial on a
    remote Kubernetes cluster or on your laptop via KinD.

    +++
    .. link-button:: kuberay-quickstart
        :type: ref
        :text: Getting Started with Ray on Kubernetes
        :classes: btn-outline-info btn-block

.. include:: /_includes/clusters/announcement_bottom.rst
