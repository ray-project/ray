.. _cluster-index:

Distributed Ray Overview
========================

What is Distributed Ray?
------------------------

One of Ray's strengths is the ability to leverage multiple machines in the same program. Ray can, of course, be run on a single machine (and is done so often) but the real power is using Ray on a cluster of machines.

A Ray cluster consists of a **head node** and a set of **worker nodes**. The head node needs to be started first, and the worker nodes are given the address of the head node to form the cluster.

Distributed Ray is powered by the Ray Cluster Launcher, which automatically provisions machines and launches a multi-node Ray cluster. You can use the cluster launcher on GCP, Amazon EC2, Azure, or even Kubernetes. Distributed Ray also makes use of the Ray Autoscaler, which gives Ray the ability "auto-scale," meaning that it can interact with a Cloud Provider to request or release instances according to application workload.

How does it work?
-----------------

The Ray Cluster Launcher will automatically enable a load-based autoscaler. The scheduler will look at the task, actor, and placement group resource demands from the cluster, and tries to add the minimum set of nodes that can fulfill these demands. When nodes are idle for more than a timeout, they will be removed. The head node is never removed.

**TODO: ADD HIGH-LEVEL DESCRIPTION OF HOW AUTOSCALING WORKS (ALGORITHM/SCHEDULER)**

Supported features
------------------

Distributed Ray can deliver support for a broad set of requirements. Among others it incorporates the following features:

* **Multiple node type autoscaling**: Ray supports the use of :ref:`multiple node types <cluster-configuration-available-node-types>` in a single cluster. In this mode of operation, the scheduler will choose the types of nodes to add based on the resource demands, instead of always adding the same kind of node type.
* **Placement groups**: :ref:`Placement groups <ray-placement-group-doc-ref>` allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling).
* **Programatically scaling a cluster**: The cluster can be directly scaled up to a desired size from within a Ray program via the ``request_resources()`` :ref:`API <ref-autoscaler-sdk-request-resources>`.
* **TODO: ADD ANY ADDITIONAL FEATURES WE WANT TO HIGHLIGHT**