.. _virtual-cluster-index:

Ray Virtual Clusters Overview
=============================

.. toctree::
    :hidden:

    Key Concepts <key-concepts>
    Design Overview <design-overview>
    Virtual Cluster Management API <management>
    Virtual Cluster CLI <cli>
    Examples <examples>

As early as 2021, Ant Group began promoting and implementing virtual clusters feature.
In addition to being able to submit and carry out different kinds of activities within the same Ray Cluster, with the condition that these jobs be segregated from one another, our business requirement was to use the long-running Ray Cluster mode to solve the delayed pod provisioning issue in Job mode.
Furthermore, several companies anticipated that some jobs might be co-located to maximise resource utilisation and others may be segregated to improve stability within the same Ray Cluster.

The concept of virtual clusters is also mentioned in the planning of the Ray 3.0 community. 
The following is a quotation from the Ray 3.0 community planning's pertinent FAQ section about `virtual clusters <https://docs.google.com/document/d/1TJ3jHWVGGviJOQYYlB9yNjUUG8Hn54hYsNqnUrY4L14/edit?tab=t.0>`_:

    **Q: Aren't Ray clusters already multi-tenant?**

    Ray clusters are technically multi-tenant today, but do not provide sufficient isolation between jobs. For example, you cannot run two Ray Tune jobs on the same cluster, since they will both try to use all the cluster resources and this leads to hangs. Similar issues apply when trying to use multiple instances of Ray Data within even one job.

    What's missing is the ability to reserve a resource "slice" or "pool" within the cluster for workloads, which is what virtual clusters provide.


    **Q: Isn't this just placement groups?**

    Yes, you can think of virtual clusters like placement groups v2. Placement groups v1 is not usable for multi-tenancy due to implementation / API limitations (i.e., no nesting / autoscaling).


    **Q: What new problems does virtual clusters solve?**

    First, virtual clusters solve the multi-tenancy problem in Ray. Instead of your ML infra team needing to create custom infrastructure to manage new Ray clusters for each workload, they can queue jobs to run efficiently and with isolation within an existing multi-tenant Ray cluster.

    Second, virtual clusters allow applications to easily compose multipart workloads. For example, you can create a Service that easily launches fine-tuning jobs using Ray Train and Ray Data. Each fine-tuning job can run in a sub-virtual cluster with well defined resource allocation and isolation semantics. Without virtual clusters, these fine-tuning jobs can compete for cluster resources in an ad-hoc mechanism, without notion of isolation, locality, or priority.

    Finally, virtual clusters fully encapsulate both the resource requirements and application logic needed to run a Ray application. This means that an application using virtual clusters can more easily be run "serverlessly" on any physical cluster without resource allocation issues.
