.. _autoscaler-v2:

Autoscaler v2
=============

This document explains how the open-source autoscaler v2 works in Ray 2.48 and outlines its high-level responsibilities and implementation details.


Overview
--------

The autoscaler is responsible for resizing the cluster based on resource demand from tasks, actors, and placement groups.
To achieve this, it follows a structured process: evaluating worker group configurations, periodically reconciling cluster state with user constraints, applying bin-packing strategies to pending workload demands, and interacting with cloud instance providers through the Instance Manager.
The following sections describe these components in detail.

Worker Group Configurations
---------------------------

Worker groups (also referred to as node types) define the sets of nodes that the Ray autoscaler scales.
Each worker group represents a logical category of nodes with the same resource configurations, such as CPU, memory, GPU, or custom resources.

The autoscaler dynamically adjusts the cluster size by adding or removing nodes within each group as workload demands change. In other words, it scales the cluster by modifying the number of nodes per worker group according to the specified scaling rules and resource requirements.

Worker groups can be configured in these ways:

- The `available_node_types <https://docs.ray.io/en/releases-2.48.0/cluster/vms/references/ray-cluster-configuration.html#node-types>`__ field in the Cluster YAML file, if you are using the ``ray up`` cluster launcher.
- The `workerGroupSpecs <https://docs.ray.io/en/releases-2.48.0/cluster/kubernetes/user-guides/config.html#pod-configuration-headgroupspec-and-workergroupspecs>`__ field in the RayCluster CRD, if you are using KubeRay.

The configuration specifies the logical resources each node has in a worker group, along with the minimum and maximum number of nodes that should exist in each group.

.. note::

   Although the autoscaler fulfills pending resource demands and releases idle nodes, it doesn't perform the actual scheduling of Ray tasks, actors, or placement groups. Scheduling is handled internally by Ray.
   The autoscaler does its own simulation of scheduling decisions on pending demands periodically to determine which nodes to launch or to stop. See the next sections for details.


Periodic Reconciliation
-----------------------

The entry point of the autoscaler is `monitor.py <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/monitor.py#L332>`__, which starts a GCS client and runs the reconciliation loop.

This process is launched on the head node by the `start_head_processes <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/_private/node.py#L1439>`__ function when using the ``ray up`` cluster launcher.
When running under KubeRay, it instead runs as a `separate autoscaler container <https://github.com/ray-project/kuberay/blob/94fa7d3eb793aa1278142f8e585cbe568fec3ae3/ray-operator/controllers/ray/common/pod.go#L191-L194>`__ in the Head Pod.

.. warning::

   In the case of the cluster launcher, if the autoscaler process crashes, then there is no autoscaling.
   While in the case of KubeRay, Kubernetes restarts the autoscaler container if it crashes by the default container restart policy.


The process periodically `reconciles <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/autoscaler.py#L200-L213>`__ against a snapshot of the following information using the Reconciler:

1. **The latest pending demands** (queried from the `get_cluster_resource_state <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/autoscaler.proto#L392-L394>`__ GCS RPC): Pending Ray tasks, actors, and placement groups.
2. **The latest user cluster constraints** (queried from the `get_cluster_resource_state <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/autoscaler.proto#L392-L394>`__ GCS RPC): The minimum cluster size, if specified via the ``ray.autoscaler.sdk.request_resources`` invocation.
3. **The latest Ray nodes information** (queried from the `get_cluster_resource_state <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/autoscaler.proto#L392-L394>`__ GCS RPC): The total and currently available resources of each Ray node in the cluster. Also includes each Ray node's status (ALIVE or DEAD) and other information such as idle duration. See Appendix for more details.
4. **The latest cloud instances** (`queried from the cloud instance provider's implementation <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/autoscaler.py#L205-L207>`__): The list of instances managed by the cloud instance provider implementation.
5. **The latest worker group configurations** (queried from the cluster YAML file or the RayCluster CRD).

The preceding information is retrieved at the beginning of each reconciliation loop.
The Reconciler uses this information to construct its internal state and perform "`passive <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L159>`__" instance lifecycle transitions by observations. This is the `sync phase <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L112-L120>`__.

After the sync phase, the Reconciler performs the `following steps <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L840>`__ in order with the ``ResourceDemandScheduler``:

1. Enforce configuration constraints, including min/max nodes for each worker group.
2. Enforce user cluster constraints (if specified by `ray.autoscaler.sdk.request_resources <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/_private/commands.py#L186>`__ invocation).
3. Fit pending demands into available resources on the cluster snapshot. This is the simulation mentioned earlier.
4. Fit any remaining demands (left over from the previous step) against worker group configurations to determine which nodes to launch.
5. Terminate idle instances (nodes that are needed by the previous 1-4 steps aren't considered idle) according to each node's ``idle_duration_ms`` (queried from GCS) and the configured idle timeout for each group.
6. Send accumulated scaling decisions (steps 1–5) to the Instance Manager with `Reconciler._update_instance_manager <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L1157-L1193>`__.
7. `Sleep briefly (5s by default) <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/monitor.py#L178>`__, then return to the sync phase.

.. warning::

   If any error occurs, such as an error from the cloud instance provider or a timeout in the sync phase, the current reconciliation is aborted and the loop jumps to step 7 to wait for the next reconciliation.


.. note::

   All scaling decisions from steps 1–5 are accumulated purely in memory.
   No interaction with the cloud instance provider occurs until step 6.


Bin Packing and Worker Group Selection
--------------------------------------

The autoscaler applies the following scoring logic to evaluate each existing node. It selects the node with the highest score and assigns it a subset of feasible demands.
It also applies the same scoring logic to each worker group and selects the one with the highest score to launch new instances.

`Scoring <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L430>`__ is based on a tuple of four values:

1. Whether the node is a GPU node and whether feasible requests require GPUs:

   - ``0`` if the node is a GPU node and requests do **not** require GPUs.
   - ``1`` if the node isn't a GPU node or requests do require GPUs.
2. The number of resource types on the node used by feasible requests.
3. The minimum `utilization rate <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L481-L489>`__ across all resource types used by feasible requests.
4. The average `utilization rate <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L481-L489>`__ across all resource types used by feasible requests.

.. note::

   Utilization rate used by feasible requests is calculated as the difference between the total and available resources divided by the total resources.


In other words:

- The autoscaler avoids launching GPU nodes unless necessary.
- It prefers nodes that maximize utilization and minimize unused resources.

Example:

- Task requires **2 GPUs**.
- Two node types are available:

  - A: [GPU: 6]
  - B: [GPU: 2, TPU: 1]

Node type **A** should be selected, since node B would leave an unused TPU (with a utilization rate of 0% on TPU), making it less favorable with respect to the third scoring criterion.

This process repeats until all feasible pending demands are packed or the maximum cluster size is reached.


Instance Manager and Cloud Instance Provider
--------------------------------------------

`Cloud Instance Provider <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/node_provider.py#L149>`__ is an abstract interface that defines the operations for managing instances in the cloud.

`Instance Manager <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/instance_manager.py#L29>`__ is the component that tracks instance lifecycle and drives event subscribers that call the cloud instance provider.

As described in the previous section, the autoscaler accumulates scaling decisions (steps 1–5) in memory and reconciles them with the cloud instance provider through the Instance Manager.

Scaling decisions are represented as a list of `InstanceUpdateEvent <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/instance_manager.proto#L135>`__ records. For example:

- **For launching new instances**:
  - ``instance_id``: A randomly generated ID for Instance Manager tracking.
  - ``instance_type``: The type of instance to launch.
  - ``new_instance_status``: ``QUEUED``.

- **For terminating instances**:
  - ``instance_id``: The ID of the instance to stop.
  - ``new_instance_status``: ``TERMINATING`` or ``RAY_STOP_REQUESTED``.

These update events are passed to the Instance Manager, which transitions instance statuses.

A normal transition flow for an instance is:

- ``(non-existent) -> QUEUED``: The Reconciler creates an instance with the ``QUEUED`` ``InstanceUpdateEvent`` when it decides to launch a new instance.
- ``QUEUED -> REQUESTED``: The Reconciler considers ``max_concurrent_launches`` and ``upscaling_speed`` when selecting an instance from the queue to transition to ``REQUESTED`` during each reconciliation iteration.
- ``REQUESTED -> ALLOCATED``: Once the Reconciler detects the instance is allocated from the cloud instance provider, it will transition the instance to ``ALLOCATED``.
- ``ALLOCATED -> RAY_INSTALLING``: If the cloud instance provider is not ``KubeRayProvider``, the Reconciler will transition the instance to ``RAY_INSTALLING`` when the instance is allocated.
- ``RAY_INSTALLING -> RAY_RUNNING``: Once the Reconciler detects from GCS that Ray has started on the instance, it will transition the instance to ``RAY_RUNNING``.
- ``RAY_RUNNING -> RAY_STOP_REQUESTED``: If the instance is idle for longer than the configured timeout, the Reconciler will transition the instance to ``RAY_STOP_REQUESTED`` to start draining the Ray process.
- ``RAY_STOP_REQUESTED -> RAY_STOPPING``: Once the Reconciler detects from GCS that the Ray process is draining, it will transition the instance to ``RAY_STOPPING``.
- ``RAY_STOPPING -> RAY_STOPPED``: Once the Reconciler detects from GCS that the Ray process has stopped, it will transition the instance to ``RAY_STOPPED``.
- ``RAY_STOPPED -> TERMINATING``: The Reconciler will transition the instance from ``RAY_STOPPED`` to ``TERMINATING``.
- ``TERMINATING -> TERMINATED``: Once the Reconciler detects that the instance has been terminated by the cloud instance provider, it will transition the instance to ``TERMINATED``.

.. note::

   The drain request sent by ``RAY_STOP_REQUESTED`` can be rejected if the node is no longer idle when the drain request arrives the node. Then the instance will be transitioned back to ``RAY_RUNNING`` instead.


You can find all valid transitions in the `get_valid_transitions <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/common.py#L193>`__ method.

Once transitions are triggered by the Reconciler, subscribers perform side effects, such as:

- ``QUEUED -> REQUESTED``: CloudInstanceUpdater launches the instance through the Cloud Instance Provider.
- ``ALLOCATED -> RAY_INSTALLING``: ThreadedRayInstaller installs the Ray process.
- ``RAY_RUNNING -> RAY_STOP_REQUESTED``: RayStopper stops the Ray process on the instance.
- ``RAY_STOPPED -> TERMINATING``: CloudInstanceUpdater terminates the instance through the Cloud Instance Provider.


.. note::

   These transitions trigger side effects, but side effects don't trigger new transitions directly.
   Instead, their results are observed from external state during the sync phase; subsequent transitions are triggered based on those observations.


.. note::

   Cloud instance provider implementations in autoscaler v2 must implement:

   - **Listing instances**: Return the set of instances currently managed by the provider.
   - **Launching instances**: Create new instances given the requested instance type and tags.
   - **Terminating instances**: Safely remove instances identified by their IDs.

   ``KubeRayProvider`` is one such cloud instance provider implementation.

   ``NodeProviderAdapter`` is an adapter that can wrap a v1 node provider (such as ``AWSNodeProvider``) to act as a cloud instance provider.


Appendix
--------

How ``get_cluster_resource_state`` Aggregates Cluster State
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The autoscaler retrieves a cluster snapshot through the ``get_cluster_resource_state`` RPC served by GCS (`HandleGetClusterResourceState <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L48>`__) which builds the reply in `MakeClusterResourceStateInternal <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L179>`__. Internally, GCS assembles the reply by combining per-node resource reports, pending workload demand, and any user-requested cluster constraints into a single ``ClusterResourceState`` message.

- Data sources and ownership:

  - `GcsAutoscalerStateManager <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc>`__ maintains a per-node cache of ``ResourcesData`` that includes totals, availables, and load-by-shape. GCS periodically polls each alive raylet (``GetResourceLoad``) and updates this cache (`GcsServer::InitGcsResourceManager <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_server.cc#L375-L418>`__, `UpdateResourceLoadAndUsage <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L267-L281>`__), then uses it to construct snapshots.
  - `GcsNodeInfo <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/gcs.proto#L307>`__ provides static and slowly changing node metadata (node ID, instance ID, node type name, IP, labels, instance type) and dead/alive status.
  - Placement group demand comes from the `placement group manager <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_placement_group_mgr.cc#L934>`__.
  - User cluster constraints come from autoscaler SDK requests that GCS records.

- Fields assembled in the reply:

  - ``node_states``: For each node, GCS sets identity and metadata from `GcsNodeInfo <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/protobuf/gcs.proto#L307>`__ and pulls resources and status from the cached ``ResourcesData`` (`GetNodeStates <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L319>`__). Dead nodes are marked ``DEAD`` and omit resource details. For alive nodes, GCS also includes ``idle_duration_ms`` and any node activity strings.
  - ``pending_resource_requests``: Computed by aggregating per-node load-by-shape across the cluster (`GetPendingResourceRequests <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L303-L317>`__). For each resource shape, the count is the sum of infeasible, backlog, and ready requests that haven't been scheduled yet.
  - ``pending_gang_resource_requests``: Pending or rescheduling placement groups represented as gang requests (`GetPendingGangResourceRequests <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L193>`__).
  - ``cluster_resource_constraints``: The set of minimal cluster resource constraints previously requested via ``ray.autoscaler.sdk.request_resources`` (`GetClusterResourceConstraints <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.cc#L245>`__).
