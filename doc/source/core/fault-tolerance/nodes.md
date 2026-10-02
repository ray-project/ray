---
myst:
  html_meta:
    description: "Node fault tolerance in Ray: what happens when a worker node, the head node, or an individual raylet fails."
---

(fault-tolerance-nodes)=

# Node fault tolerance

A Ray cluster consists of one or more worker nodes. Each worker node consists of worker processes and system processes, such as the raylet. One of the worker nodes is designated as the head node and has extra processes, such as the GCS.

This page describes node failures and their impact on tasks, actors, and objects.

## Worker node failure

When a worker node fails, all the tasks and actors running on it fail, and all the objects owned by its worker processes are lost. The {ref}`tasks <fault-tolerance-tasks>`, {ref}`actors <fault-tolerance-actors>`, and {ref}`objects <fault-tolerance-objects>` fault-tolerance mechanisms then try to recover from these failures using other worker nodes.

## Head node failure

When a head node fails, the entire Ray cluster fails. To tolerate head node failures, make the {ref}`GCS fault tolerant <fault-tolerance-gcs>` so that a new head node still has all the cluster-level data when you start it.

## Raylet failure

When a raylet process fails, the corresponding node is marked as dead and treated the same as a node failure. Each raylet has a unique ID, so even if the raylet restarts on the same physical machine, the Ray cluster treats it as a new raylet and a new node.
