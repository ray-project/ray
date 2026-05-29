# Ray on Cloud VMs
(cloud-vm-index)=

```{toctree}
:hidden:

getting-started
User Guides <user-guides/index>
Examples <examples/index>
references/index
```

## Overview

In this section we cover how to launch Ray clusters on Cloud VMs. Ray ships with built-in support
for launching AWS, GCP, and Azure clusters, and also has community-maintained integrations for Aliyun and vSphere.
Each Ray cluster consists of a head node and a collection of worker nodes. Optional
[autoscaling](vms-autoscaling) support allows the Ray cluster to be sized according to the
requirements of your Ray workload, adding and removing worker nodes as needed. Ray supports
clusters composed of multiple heterogeneous compute nodes (including GPU nodes).

Concretely, you will learn how to:

- Set up and configure Ray in public clouds
- Deploy applications and monitor your cluster

## Learn More

The Ray docs present all the information you need to start running Ray workloads on VMs.

```{eval-rst}
.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::

        **Getting Started**
        ^^^

        Learn how to start a Ray cluster and deploy Ray applications in the cloud.

        +++
        .. button-ref:: vm-cluster-quick-start
            :color: primary
            :outline:
            :expand:

            Get Started with Ray on Cloud VMs

    .. grid-item-card::

        **Examples**
        ^^^

        Try example Ray workloads in the Cloud

        +++
        .. button-ref:: vm-cluster-examples
            :color: primary
            :outline:
            :expand:

            Try example workloads

    .. grid-item-card::

        **User Guides**
        ^^^

        Learn best practices for configuring cloud clusters

        +++
        .. button-ref:: vm-cluster-guides
            :color: primary
            :outline:
            :expand:

            Read the User Guides

    .. grid-item-card::

        **API Reference**
        ^^^

        Find API references for cloud clusters

        +++
        .. button-ref:: vm-cluster-api-references
            :color: primary
            :outline:
            :expand:

            Check API references
```
