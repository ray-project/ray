---
myst:
  html_meta:
    description: "Inspect the nodes and the total and available resources of a Ray cluster with ray.nodes, ray.cluster_resources, and ray.available_resources."
---

(core-cluster-state)=

# Inspect cluster state

Applications built on Ray often need information or diagnostics about the cluster. Common questions include the following:

1. How many nodes are in your autoscaling cluster?
1. What resources are available in your cluster, both used and total?
1. What objects are in your cluster?

To answer these questions, use the global state API.

## Get node information

To get information about the current nodes in your cluster, use {py:func}`ray.nodes`:

```{testcode}
import ray

ray.init()
print(ray.nodes())
```

```{testoutput}
:options: +MOCK

  [{'NodeID': '2691a0c1aed6f45e262b2372baf58871734332d7',
    'Alive': True,
    'NodeManagerAddress': '192.168.1.82',
    'NodeManagerHostname': 'host-MBP.attlocal.net',
    'NodeManagerPort': 58472,
    'ObjectManagerPort': 52383,
    'ObjectStoreSocketName': '/tmp/ray/session_2020-08-04_11-00-17_114725_17883/sockets/plasma_store',
    'RayletSocketName': '/tmp/ray/session_2020-08-04_11-00-17_114725_17883/sockets/raylet',
    'MetricsExportPort': 64860,
    'alive': True,
    'Resources': {'CPU': 16.0, 'memory': 100.0, 'object_store_memory': 34.0, 'node:192.168.1.82': 1.0}}]
```

The preceding output includes the following fields:

- `NodeID`: A unique identifier for the raylet.
- `alive`: Whether the node is alive.
- `NodeManagerAddress`: The private IP address of the node that the raylet runs on.
- `Resources`: The total resource capacity on the node.
- `MetricsExportPort`: The port number that serves metrics through a {ref}`Prometheus endpoint <collect-metrics>`.

## Get resource information

To get the current total resource capacity of your cluster, use {py:func}`ray.cluster_resources`.

To get the current available resource capacity of your cluster, use {py:func}`ray.available_resources`.
