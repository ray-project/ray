.. _cluster-index:

Ray Cluster Overview
====================

What is a Ray cluster?
------------------------

One of Ray's strengths is the ability to leverage multiple machines in the same program. Ray can, of course, be run on a single machine (and is done so often), but the real power is using Ray on a cluster of machines.

A Ray cluster consists of a **head node** and a set of **worker nodes**. The head node needs to be started first, and the worker nodes are given the address of the head node to form the cluster.

You can use the Ray Cluster Launcher to provision machines and launch a multi-node Ray cluster. You can use the cluster launcher on AWS, GCP, Azure, Kubernetes, on-premise, and Staroid or even on your custom node provider. Ray clusters can also make use of the Ray Autoscaler, which allows Ray to interact with a cloud provider to request or release instances according to application workload.

How does it work?
-----------------

The Ray Cluster Launcher will automatically enable a load-based autoscaler. The autoscaler resource demand scheduler will look at the pending tasks, actors, and placement groups resource demands from the cluster, and try to add the minimum list of nodes that can fulfill these demands. When worker nodes are idle for more than :ref:`idle_timeout_minutes <cluster-configuration-idle-timeout-minutes>`, they will be removed (the head node is never removed unless the cluster is teared down).

Autoscaler uses a simple binpacking algorithm to binpack the user demands into the available cluster resources. The remaining unfulfilled demands are placed on the smallest list of nodes that satisfies the demand while maximizing utilization (starting from the smallest node).

**Here is "A Glimpse into the Ray Autoscaler" and how to debug/monitor your cluster:**

2021-19-01 by Ameer Haj-Ali, Anyscale, Inc.

.. youtube:: BJ06eJasdu4
