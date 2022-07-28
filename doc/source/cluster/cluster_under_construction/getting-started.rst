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

Ray Clusters Overview
=====================

What is a Ray cluster?
----------------------

One of Ray's strengths is the ability to leverage multiple machines for
distributed execution. Ray can, of course, be run on a single machine (and is
done so often), but the real power is using Ray on a cluster of machines.

Ray can automatically interact with the cloud provider to request or release
instances. You can specify :ref:`a configuration <cluster-config>` to launch
clusters on :ref:`AWS, GCP, Azure (community-maintained), Aliyun (community-maintained), on-premise, or even on
your custom node provider <cluster-cloud>`. Ray can also be run on :ref:`Kubernetes <kuberay-index>` infrastructure.
Your cluster can have a fixed size
or :ref:`automatically scale up and down<cluster-autoscaler>` depending on the
demands of your application.

Where to go from here?
----------------------

.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Quick Start** 
    ^^^

    In this quick start tutorial you will take a sample application designed to
    run on a laptop and scale it up in the cloud.

    +++
    .. link-button:: ref-cluster-quick-start-vms-under-construction
        :type: ref
        :text: Ray Clusters Quick Start
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
    ---

    **Deployment Guide**
    ^^^

    Learn how to set up a distributed Ray cluster and run your workloads on it.

    +++
    .. link-button:: ref-deployment-guide
        :type: ref
        :text: Deploy on a Ray Cluster
        :classes: btn-outline-info btn-block
    ---

    **API**
    ^^^

    Get more in-depth information about the various APIs to interact with Ray
    Clusters, including the :ref:`Ray cluster config YAML and CLI<cluster-config>`,
    the :ref:`Ray Client API<ray-client>` and the
    :ref:`Ray job submission API<ray-job-submission-api-ref>`.

    +++
    .. link-button:: ref-cluster-api
        :type: ref
        :text: Read the API Reference
        :classes: btn-outline-info btn-block

.. include:: /_includes/clusters/announcement_bottom.rst
