.. include:: /_includes/clusters/announcement.rst

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-index:


Ray Clusters Overview
=====================

Ray enables seamless scaling of workloads from a laptop to a large cluster. To run Ray
applications on multiple nodes, you must first *deploy a Ray cluster*.

A Ray cluster is a set of worker nodes connected to a common :ref:`Ray head node <cluster-head-node>`.
Ray clusters can be fixed-size, or they may :ref:`autoscale up and down <cluster-autoscaler>` according
to the resources requested by applications running on the cluster.

Where can I deploy Ray clusters?
--------------------------------

Ray provides native cluster deployment support on the following technology stacks:

* On :ref:`AWS and GCP <ref-cluster-quick-start>`. Community-supported Azure and Aliyun integrations also exist.
* On :ref:`Kubernetes, via the KubeRay project <kuberay-index>`.

Advanced users may want to :ref:`deploy Ray clusters manually <on-prem>`
or onto platforms not listed here by :ref:`implementing a custom node provider <ref-cluster-setup>`.

What's next?
------------

.. panels::
    :container: text-center
    :column: col-lg-6 px-3 py-2
    :card:

    **I want to learn key Ray cluster concepts**
    ^^^
    Understand the key concepts and main ways of interacting with a Ray cluster.

    +++
    .. link-button:: cluster-key-concepts
        :type: ref
        :text: Learn Key Concepts
        :classes: btn-outline-info btn-block

    ---

    **I want to run Ray on a cloud provider**
    ^^^
    Take a sample application designed to run on a laptop and scale it up in the
    cloud. Access to an AWS or GCP account is required.

    +++
    .. link-button:: ref-cluster-quick-start
        :type: ref
        :text: Get Started with Ray on VMs
        :classes: btn-outline-info btn-block
    ---

    **I want to run Ray on Kubernetes**
    ^^^
    Deploy a Ray application to a Kubernetes cluster. You can run the tutorial on a
    Kubernetes cluster or on your laptop via KinD.

    +++
    .. link-button:: kuberay-quickstart
        :type: ref
        :text: Get Started with Ray on Kubernetes
        :classes: btn-outline-info btn-block

.. include:: /_includes/clusters/announcement_bottom.rst
