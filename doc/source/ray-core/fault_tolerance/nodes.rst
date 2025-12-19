.. _fault-tolerance-nodes:

Node Fault Tolerance
====================

A Ray cluster consists of one or more worker nodes,
each of which consists of worker processes and system processes (for example, raylet).
One of the worker nodes is designated as the head node and has extra processes like the GCS.

This page describes node failures and their impact on tasks, actors, and objects.

Worker node failure
-------------------

When a worker node fails, all the running tasks and actors fail and all the objects owned by worker processes of this node are lost. In this case, the :ref:`tasks <fault-tolerance-tasks>`, :ref:`actors <fault-tolerance-actors>`, :ref:`objects <fault-tolerance-objects>` fault tolerance mechanisms kick in and try to recover the failures using other worker nodes.

Head node failure
-----------------

When a head node fails, the entire Ray cluster fails.
To tolerate head node failures, :ref:`GCS <fault-tolerance-gcs>` needs to be fault tolerant
so that when a new head node starts, all the cluster-level data is still available.

Raylet failure
--------------

When a raylet process fails, the corresponding node is marked as dead and is treated the same as a node failure.
Each raylet is associated with a unique id, so even if the raylet restarts on the same physical machine,
it's treated as a new raylet/node to the Ray cluster.
