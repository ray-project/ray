.. warning::
    This page is under construction!

.. include:: /_includes/clusters/announcement.rst

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-index-under-construction:

..
    TODO(cade)
    Update this to accomplish the following:
        Direct users, based on what they are trying to accomplish, to the
        correct page between "Managing Ray Clusters on Kubernetes",
        "Managing Ray Clusters via `ray up`", and "Using Ray Clusters".
        There should be some discussion on Kubernetes vs. `ray up` for
        those looking to create new Ray clusters for the first time.
    Specifically:
    * Different ways you can launch a cluster. VM or Kubernetes, as well as some community-supported launchers
    * Key concepts
    * API
    * Getting started with Ray on Kubernetes
    * Getting started with Ray on Virtual Machines


Ray Clusters Overview
=====================

What is a Ray cluster?
----------------------

One of Ray's strengths is the ability to leverage multiple machines for
distributed execution. Ray can, of course, be run on a single machine (and is
done so often), but the real power is using Ray on a cluster of machines.

A Ray Cluster can be created from virtual machines using the :ref:`Ray Cluster Launcher<ref-cluster-quick-start-vms-under-construction>`, which is officially supported on AWS and GCP and community supported on Azure and Aliyun, or created from machines managed by Kubernetes via :ref:`KubeRay<kuberay-index>`, which is the officially supported way to deploy Ray on Kubernetes.
Ray can also be :ref:`deployed on-premise<cluster-private-setup-under-construction>` or even onto infrastructure not listed here by :ref:`providing a custom node provider<additional-cloud-providers-under-construction>`.

Your cluster can have a fixed size, or :ref:`leverage autoscaling<cluster-autoscaler-under-construction>` to automatically request additional or release unused nodes depending on the demands of your application.

..
    Ray can automatically interact with the cloud provider to request or release instances. You can specify :ref:`a configuration <cluster-config>` to launch clusters on :ref:`AWS, GCP, Azure (community-maintained), Aliyun (community-maintained), on-premise, or even on your custom node provider <cluster-cloud>`. Ray can also be run on :ref:`Kubernetes <kuberay-index>` infrastructure. Your cluster can have a fixed size or 
    Ray can automatically interact with the cloud provider to request or release
    instances. You can specify :ref:`a configuration <cluster-config>` to launch
    clusters on :ref:`AWS, GCP, Azure (community-maintained), Aliyun (community-maintained), on-premise, or even on
    your custom node provider <cluster-cloud>`. Ray can also be run on :ref:`Kubernetes <kuberay-index>` infrastructure.
    Your cluster can have a fixed size
    or :ref:`automatically scale up and down<cluster-autoscaler>` depending on the
    demands of your application.

..
    TODO(cade)
    Change the titles to be something like "I have a AWS/GCP account and I want to create a Ray clsuter"
    "I have a Kubernetes cluster that I want to create Ray Clusters on"

Where to go from here?
----------------------

.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Getting Started with the Ray Clusters on Virtual Machines** 
    ^^^

    In this quick start tutorial you will take a sample application designed to
    run on a laptop and scale it up in the cloud, using a cloud provider such as
    AWS or GCP.

    +++
    .. link-button:: ref-cluster-quick-start-vms-under-construction
        :type: ref
        :text: Getting Started with Ray Clusters on VMs
        :classes: btn-outline-info btn-block
    ---

    **Getting Started with Ray on Kubernetes**
    ^^^

    In this quick start tutorial you will take a sample application designed to
    run on a laptop and scale it up in the cloud, using a cloud provider such as
    AWS or GCP.

    +++
    .. link-button:: kuberay-quickstart
        :type: ref
        :text: Getting Started with Ray on Kubernetes
        :classes: btn-outline-info btn-block
    ---

    **Key Concepts**
    ^^^

    Understand the key concepts behind Ray Clusters. Learn about the main
    concepts and the different ways to interact with a cluster.

    +++
    .. link-button:: cluster-key-concepts
        :type: ref
        :text: Learn Key Concepts
        :classes: btn-outline-info btn-block

.. include:: /_includes/clusters/announcement_bottom.rst
