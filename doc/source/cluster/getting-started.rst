.. _cluster-index:

Ray Clusters Overview
=====================

Ray enables seamless scaling of workloads from a laptop to a large cluster. While Ray
works out of the box on single machines with just a call to ``ray.init``, to run Ray
applications on multiple nodes you must first *deploy a Ray cluster*.

A Ray cluster is a set of worker nodes connected to a common :ref:`Ray head node <cluster-head-node>`.
Ray clusters can be fixed-size, or they may :ref:`autoscale up and down <cluster-autoscaler>` according
to the resources requested by applications running on the cluster.

Where can I deploy Ray clusters?
--------------------------------

Ray provides native cluster deployment support on the following technology stacks:

* On :ref:`AWS and GCP <cloud-vm-index>`. Community-supported Azure and Aliyun integrations also exist.
* On :ref:`Kubernetes <kuberay-index>`, via the officially supported KubeRay project.

Advanced users may want to :ref:`deploy Ray manually <on-prem>`
or onto :ref:`platforms not listed here <ref-cluster-setup>`.

.. note::

    Multi-node Ray clusters are only supported on Linux. At your own risk, you
    may deploy Windows and OSX clusters by setting the environment variable
    ``RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1`` during deployment.

What's next?
------------

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::

        **I want to learn key Ray cluster concepts**
        ^^^
        Understand the key concepts and main ways of interacting with a Ray cluster.

        +++
        .. button-ref:: cluster-key-concepts
            :color: primary
            :outline:
            :expand:

            Learn Key Concepts

    .. grid-item-card::

        **I want to run Ray on Kubernetes**
        ^^^
        Deploy a Ray application to a Kubernetes cluster. You can run the tutorial on a
        Kubernetes cluster or on your laptop via KinD.

        +++
        .. button-ref:: kuberay-quickstart
            :color: primary
            :outline:
            :expand:

            Get Started with Ray on Kubernetes

    .. grid-item-card::

        **I want to run Ray on a cloud provider**
        ^^^
        Take a sample application designed to run on a laptop and scale it up in the
        cloud. Access to an AWS or GCP account is required.

        +++
        .. button-ref:: vm-cluster-quick-start
            :color: primary
            :outline:
            :expand:

            Get Started with Ray on VMs

    .. grid-item-card::

        **I want to run my application on an existing Ray cluster**
        ^^^
        Guide to submitting applications as Jobs to existing Ray clusters.

        +++
        .. button-ref:: jobs-quickstart
            :color: primary
            :outline:
            :expand:

            Job Submission

.. include:: /_includes/clusters/announcement_bottom.rst
