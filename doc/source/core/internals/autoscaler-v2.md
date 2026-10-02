---
myst:
  html_meta:
    description: "Internals of Ray's autoscaler v2: worker group configuration, periodic reconciliation, bin packing, and the instance manager."
---

(autoscaler-v2)=

# Autoscaler v2

This page explains how the open-source autoscaler v2 works in Ray 2.48, including its high-level responsibilities and implementation details.


## Overview

The autoscaler resizes the cluster based on resource demand from tasks, actors, and placement groups. It evaluates worker group configurations, periodically reconciles cluster state with user constraints, applies bin-packing strategies to pending workload demands, and interacts with cloud instance providers through the Instance Manager. The following sections describe each of these components.

## Worker group configurations

Worker groups, also called node types, define the sets of nodes that the Ray autoscaler scales. Each worker group represents a logical category of nodes with the same resource configurations, such as CPU, memory, GPU, or custom resources.

As workload demands change, the autoscaler adjusts the cluster size by adding or removing nodes in each worker group, according to the specified scaling rules and resource requirements.

You configure worker groups in one of two ways, depending on how you launch the cluster:

- The [available_node_types](https://docs.ray.io/en/releases-2.48.0/cluster/vms/references/ray-cluster-configuration.html#node-types) field in the cluster YAML file, if you're using the `ray up` cluster launcher.
- The [workerGroupSpecs](https://docs.ray.io/en/releases-2.48.0/cluster/kubernetes/user-guides/config.html#pod-configuration-headgroupspec-and-workergroupspecs) field in the RayCluster CRD, if you're using KubeRay.

The configuration specifies the logical resources of each node in a worker group, along with the minimum and maximum number of nodes for each group.

:::{note}
Although the autoscaler fulfills pending resource demands and releases idle nodes, it doesn't schedule Ray tasks, actors, or placement groups. Ray handles scheduling internally. The autoscaler periodically runs its own simulation of scheduling decisions on pending demands to determine which nodes to launch or stop. The following sections describe this process.
:::


## Periodic reconciliation

The entry point of the autoscaler is [monitor.py](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/monitor.py#L332), which starts a GCS client and runs the reconciliation loop.

When you use the `ray up` cluster launcher, the [start_head_processes](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/_private/node.py#L1439) function launches this process on the head node. Under KubeRay, the process runs instead as a [separate autoscaler container](https://github.com/ray-project/kuberay/blob/94fa7d3eb793aa1278142f8e585cbe568fec3ae3/ray-operator/controllers/ray/common/pod.go#L191-L194) in the head Pod.

:::{warning}
With the cluster launcher, if the autoscaler process crashes, autoscaling stops. With KubeRay, if the autoscaler container crashes, Kubernetes restarts it according to the default container restart policy.
:::


The process uses the Reconciler to periodically [reconcile](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/autoscaler.py#L200-L213) against a snapshot of the following information:

- **The latest pending demands**: Pending Ray tasks, actors, and placement groups. The autoscaler queries them from the [get_cluster_resource_state](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/autoscaler.proto#L392-L394) GCS RPC.
- **The latest user cluster constraints**: The minimum cluster size, if you set one by calling `ray.autoscaler.sdk.request_resources`. The autoscaler queries it from the [get_cluster_resource_state](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/autoscaler.proto#L392-L394) GCS RPC.
- **The latest Ray node information**: The total and available resources of each Ray node in the cluster, along with each node's status, `ALIVE` or `DEAD`, and other information such as idle duration. The autoscaler queries it from the [get_cluster_resource_state](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/autoscaler.proto#L392-L394) GCS RPC. The appendix at the end of this page describes how GCS assembles this information.
- **The latest cloud instances**: The list of instances that the cloud instance provider implementation manages. The autoscaler [queries this list from the cloud instance provider's implementation](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/autoscaler.py#L205-L207).
- **The latest worker group configurations**: The autoscaler queries these from the cluster YAML file or the RayCluster CRD.

The autoscaler retrieves this information at the beginning of each reconciliation loop. The Reconciler uses it to construct its internal state and to perform "[passive](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L159)" instance lifecycle transitions based on observations. This stage is the [sync phase](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L112-L120).

After the sync phase, the Reconciler uses the `ResourceDemandScheduler` to perform the [following steps](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L840) in order:

1. Enforce configuration constraints, including the minimum and maximum number of nodes for each worker group.
1. Enforce user cluster constraints, if you specified any by calling [ray.autoscaler.sdk.request_resources](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/_private/commands.py#L186).
1. Fit pending demands into available resources on the cluster snapshot. This step is the simulation that the earlier note describes.
1. Fit any demands left over from the previous step against worker group configurations to determine which nodes to launch.
1. Terminate idle instances according to each node's `idle_duration_ms`, which the autoscaler queries from GCS, and the configured idle timeout for each group. The autoscaler doesn't consider a node idle if steps 1 through 4 need it.
1. Send the scaling decisions accumulated in steps 1 through 5 to the Instance Manager with [Reconciler._update_instance_manager](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L1157-L1193).
1. [Sleep for a short interval](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/monitor.py#L178), which is 5 seconds by default, then return to the sync phase.

:::{warning}
If any error occurs, such as an error from the cloud instance provider or a timeout in the sync phase, the autoscaler aborts the current reconciliation and jumps to step 7 to wait for the next reconciliation.
:::


:::{note}
The autoscaler accumulates all scaling decisions from steps 1 through 5 only in memory. No interaction with the cloud instance provider occurs until step 6.
:::


## Bin packing and worker group selection

The autoscaler scores each existing node with the following logic, selects the node with the highest score, and assigns it a subset of feasible demands. To launch new instances, it applies the same scoring logic to each worker group and selects the group with the highest score.

[Scoring](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L430) uses a tuple of four values:

1. Whether the node is a GPU node and whether feasible requests require GPUs:

   - `0` if the node is a GPU node and requests don't require GPUs.
   - `1` if the node isn't a GPU node or requests do require GPUs.
1. The number of resource types on the node that feasible requests use.
1. The minimum [utilization rate](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L481-L489) across all resource types that feasible requests use.
1. The average [utilization rate](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L481-L489) across all resource types that feasible requests use.

:::{note}
The utilization rate of a resource that feasible requests use is the difference between the total and available resources, divided by the total resources.
:::


As a result, the autoscaler avoids launching GPU nodes unless necessary, and it prefers nodes that maximize utilization and minimize unused resources.

For example, suppose a task requires 2 GPUs and two node types are available:

- A: [GPU: 6]
- B: [GPU: 2, TPU: 1]

The autoscaler should select node type A. Node type B would leave an unused TPU with a utilization rate of 0%, which makes B less favorable under the third scoring criterion.

This process repeats until the autoscaler packs all feasible pending demands or the cluster reaches its maximum size.


## Instance Manager and cloud instance provider

The [cloud instance provider](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/node_provider.py#L149) is an abstract interface that defines the operations for managing instances in the cloud.

The [Instance Manager](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/instance_manager.py#L29) tracks the instance lifecycle and drives event subscribers that call the cloud instance provider.

As the previous section describes, the autoscaler accumulates the scaling decisions from steps 1 through 5 in memory and reconciles them with the cloud instance provider through the Instance Manager.

Scaling decisions take the form of a list of [InstanceUpdateEvent](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/instance_manager.proto#L135) records. The following examples show update events for launching and terminating instances:

- To launch new instances:
  - `instance_id`: A randomly generated ID for Instance Manager tracking.
  - `instance_type`: The type of instance to launch.
  - `new_instance_status`: `QUEUED`.

- To terminate instances:
  - `instance_id`: The ID of the instance to stop.
  - `new_instance_status`: `TERMINATING` or `RAY_STOP_REQUESTED`.

The Instance Manager receives these update events and transitions instance statuses.

An instance normally moves through the following transitions:

- `(non-existent) -> QUEUED`: The Reconciler creates an instance with the `QUEUED` `InstanceUpdateEvent` when it decides to launch a new instance.
- `QUEUED -> REQUESTED`: During each reconciliation iteration, the Reconciler considers `max_concurrent_launches` and `upscaling_speed` when it selects an instance from the queue to transition to `REQUESTED`.
- `REQUESTED -> ALLOCATED`: When the Reconciler detects that the cloud instance provider has allocated the instance, it transitions the instance to `ALLOCATED`.
- `ALLOCATED -> RAY_INSTALLING`: If the cloud instance provider isn't `KubeRayProvider`, the Reconciler transitions the instance to `RAY_INSTALLING` when the instance is allocated.
- `RAY_INSTALLING -> RAY_RUNNING`: When the Reconciler detects from GCS that Ray has started on the instance, it transitions the instance to `RAY_RUNNING`.
- `RAY_RUNNING -> RAY_STOP_REQUESTED`: If the instance is idle for longer than the configured timeout, the Reconciler transitions the instance to `RAY_STOP_REQUESTED` to start draining the Ray process.
- `RAY_STOP_REQUESTED -> RAY_STOPPING`: When the Reconciler detects from GCS that the Ray process is draining, it transitions the instance to `RAY_STOPPING`.
- `RAY_STOPPING -> RAY_STOPPED`: When the Reconciler detects from GCS that the Ray process has stopped, it transitions the instance to `RAY_STOPPED`.
- `RAY_STOPPED -> TERMINATING`: The Reconciler transitions the instance from `RAY_STOPPED` to `TERMINATING`.
- `TERMINATING -> TERMINATED`: When the Reconciler detects that the cloud instance provider has terminated the instance, it transitions the instance to `TERMINATED`.

:::{note}
The drain request that `RAY_STOP_REQUESTED` sends can be rejected if the node is no longer idle when the request arrives at the node. In that case, the instance transitions back to `RAY_RUNNING` instead.
:::


You can find all valid transitions in the [get_valid_transitions](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/common.py#L193) method.

When the Reconciler triggers a transition, subscribers perform side effects. Examples include the following:

- `QUEUED -> REQUESTED`: CloudInstanceUpdater launches the instance through the cloud instance provider.
- `ALLOCATED -> RAY_INSTALLING`: ThreadedRayInstaller installs the Ray process.
- `RAY_RUNNING -> RAY_STOP_REQUESTED`: RayStopper stops the Ray process on the instance.
- `RAY_STOPPED -> TERMINATING`: CloudInstanceUpdater terminates the instance through the cloud instance provider.


:::{note}
These transitions trigger side effects, but side effects don't trigger new transitions directly. Instead, the Reconciler observes their results from external state during the sync phase and triggers subsequent transitions based on those observations.
:::


:::{note}
Cloud instance provider implementations in autoscaler v2 must implement the following operations:

- **Listing instances**: Return the set of instances that the provider is managing.
- **Launching instances**: Create new instances given the requested instance type and tags.
- **Terminating instances**: Safely remove instances identified by their IDs.

`KubeRayProvider` is one such cloud instance provider implementation.

`NodeProviderAdapter` can wrap a v1 node provider, such as `AWSNodeProvider`, to act as a cloud instance provider.
:::


## Appendix

This section covers implementation details that the earlier sections refer to.

### How `get_cluster_resource_state` aggregates cluster state

The autoscaler retrieves a cluster snapshot through the `get_cluster_resource_state` RPC, which GCS serves with [HandleGetClusterResourceState](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L48). That handler builds the reply in [MakeClusterResourceStateInternal](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L179). GCS combines per-node resource reports, pending workload demand, and any user-requested cluster constraints into a single `ClusterResourceState` message.

GCS draws on the following data sources:

- [GcsAutoscalerStateManager](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc) maintains a per-node cache of `ResourcesData` that includes total resources, available resources, and load-by-shape. GCS periodically polls each alive raylet with `GetResourceLoad` and updates this cache in [GcsServer::InitGcsResourceManager](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_server.cc#L375-L418) and [UpdateResourceLoadAndUsage](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L267-L281). It then uses the cache to construct snapshots.
- [GcsNodeInfo](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/gcs.proto#L307) provides each node's alive or dead status and its static and slowly changing metadata, covering the node ID, instance ID, node type name, IP address, labels, and instance type.
- Placement group demand comes from the [placement group manager](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_placement_group_mgr.cc#L934).
- User cluster constraints come from autoscaler SDK requests that GCS records.

GCS assembles the following fields in the reply:

- `node_states`: For each node, GCS sets identity and metadata from [GcsNodeInfo](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/gcs.proto#L307) and pulls resources and status from the cached `ResourcesData`. The [GetNodeStates](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L319) function builds this field. GCS marks dead nodes as `DEAD` and omits their resource details. For alive nodes, GCS also includes `idle_duration_ms` and any node activity strings.
- `pending_resource_requests`: GCS computes this field by aggregating per-node load-by-shape across the cluster in [GetPendingResourceRequests](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L303-L317). For each resource shape, the count is the sum of infeasible, backlog, and ready requests that haven't been scheduled yet.
- `pending_gang_resource_requests`: Pending or rescheduling placement groups, represented as gang requests. The [GetPendingGangResourceRequests](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L193) function builds this field.
- `cluster_resource_constraints`: The set of minimal cluster resource constraints that you previously requested through `ray.autoscaler.sdk.request_resources`. The [GetClusterResourceConstraints](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L245) function builds this field.
