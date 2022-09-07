# Ray on Cloud VMs
(cloud-vm-index)=

## Overview

In this section we cover how to launch Ray clusters on Cloud VMs. Ray ships with built-in support
for launching AWS and GCP clusters, and also has community-maintained integrations for Azure and Aliyun.
Each Ray cluster consists of a head node and a collection of worker nodes. Optional
[autoscaling](vms-autoscaling) support allows the Ray cluster to be sized according to the
requirements of your Ray workload, adding and removing worker nodes as needed. Ray supports
clusters composed of multiple heterogenous compute nodes (including GPU nodes).

Concretely, you will learn how to:

- Set up and configure Ray in public clouds
- Deploy applications and monitor your cluster

## Learn More

The Ray docs present all the information you need to start running Ray workloads on VMs.

```{eval-rst}
.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Getting Started**
    ^^^

    Learn how to start a Ray cluster and deploy Ray applications in the cloud.

    +++
    .. link-button:: vm-cluster-quick-start
        :type: ref
        :text: Get Started with Ray on Cloud VMs
        :classes: btn-outline-info btn-block
    ---
    **Examples**
    ^^^

    Try example Ray workloads in the Cloud

    +++
    .. link-button:: vm-cluster-examples
        :type: ref
        :text: Try example workloads
        :classes: btn-outline-info btn-block
    ---
    **User Guides**
    ^^^

    Learn best practices for configuring cloud clusters

    +++
    .. link-button:: vm-cluster-guides
        :type: ref
        :text: Read the User Guides
        :classes: btn-outline-info btn-block
    ---
    **API Reference**
    ^^^

    Find API references for cloud clusters

    +++
    .. link-button:: vm-cluster-api-references
        :type: ref
        :text: Check API references
        :classes: btn-outline-info btn-block
```
