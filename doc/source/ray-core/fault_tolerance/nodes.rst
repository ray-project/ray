.. _fault-tolerance-nodes:

Node Fault Tolerance
====================

A Ray cluster consists of one or more worker nodes,
each of which consists of worker processes and system processes (e.g. raylet).
One of the worker nodes is designated as the head node and has extra processes like the GCS.

Here, we describe node failures and their impact on tasks, actors, and objects.

Worker node failure
-------------------

When a worker node fails, all the running tasks and actors will fail and all the objects owned by worker processes of this node will be lost. In this case, the :ref:`tasks <fault-tolerance-tasks>`, :ref:`actors <fault-tolerance-actors>`, :ref:`objects <fault-tolerance-objects>` fault tolerance mechanisms will kick in and try to recover the failures using other worker nodes.

Head node failure
-----------------

When a head node fails, the entire Ray cluster fails.
To tolerate head node failures, we need to make :ref:`GCS fault tolerant <fault-tolerance-gcs>`
so that when we start a new head node we still have all the cluster-level data.

Raylet failure
--------------

When a raylet process fails, the corresponding node will be marked as dead and is treated the same as node failure.
Each raylet is associated with a unique id, so even if the raylet restarts on the same physical machine,
it'll be treated as a new raylet/node to the Ray cluster.
