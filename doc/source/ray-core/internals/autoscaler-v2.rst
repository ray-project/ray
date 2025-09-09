.. _autoscaler-v2:

Autoscaler V2
=============

This document explains how the OSS autoscaler V2 works in Ray 2.48, outlining its high-level responsibilities and implementation details.


Responsibilities
----------------

The autoscaler is responsible for resizing the cluster based on resource demand including tasks, actors, and placement groups.
To achieve this, it follows a structured process: evaluating worker group configurations, periodically reconciling cluster state with user constraints, applying bin-packing strategies to pending workloads, and interacting with cloud providers through the Instance Manager.
The following sections describe these components in detail.

Work Group Configurations
-------------------------

Work groups (node types) can be configured in these ways:

- A `available_nodes_types in the Cluster YAML file <https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html#node-types>`__, if you are using the `ray up` cluster launcher.
- The `workerGroupSpecs <https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html#pod-configuration-headgroupspec-and-workergroupspecs>`__ field in the RayCluster CRD, if you are using KubeRay.

This configuration specifies the logical resources each node has in a worker group, along with the minimum and maximum number of nodes that should exist in each group.
The autoscaler then adds or removes nodes from worker groups based on current resource demand and cluster capacity.

.. note::
   Although the autoscaler fulfills resource demands and releases idle resources, it doesn't perform the actual scheduling of Ray tasks, actors, or placement groups. Scheduling is handled internally by Ray.
   The autoscaler does its own simulation of scheduling decisions on pending demands periodically to determine which nodes to launch or to stop. See the next sections for details.


Periodic Reconciliation
-----------------------

The autoscaler periodically `reconciles <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/autoscaler.py#L200-L213>`__ on a snapshot of the following information with the Reconciler:

1. **The Latest Pending Demands** (queried from the `get_cluster_resource_state` GCS RPC): Pending Ray tasks, actors, and placement groups.
2. **The Latest User Cluster Constraints** (queried from the `get_cluster_resource_state` GCS RPC): The minimum cluster size, if specified by the user `ray.autoscaler.sdk.request_resources` invocation.
3. **The Latest Total and Available Node Resources** (queried from the `get_cluster_resource_state` GCS RPC): The total and currently available resources of each active Ray node.
4. **The Latest Cloud Instances** (queried from the cloud provider's implementation): The list of instances managed by the Cloud Provider implementation.
5. **The Latest Work Group Configurations.** (queried from the cluster YAML file or the RayCluster CRD).

The above information is retrieved at the beginning of each reconciliation loop.
The Reconciler uses this information to construct its internal state. This is the `sync phase <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L112-L120>`__.

After the sync phase, the Reconciler performs the `following steps <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L840>`__ with the ``ResourceDemandScheduler``:

1. Enforce configuration constraints, including min/max nodes for each worker group.
2. Enforce user cluster constraints (if specified by `ray.autoscaler.sdk.request_resources` invocation).
3. Bin-pack pending demands into available resources on the cluster snapshot. This is the simulation mentioned earlier.
4. Bin-pack the remaining demands from the previous step against worker groups to know what nodes to launch.
5. Terminate idle instances according to the configured idle timeout for each group.
6. Send accumulated scaling decisions (steps 1–5) to the Instance Manager with `Reconciler._update_instance_manager <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/reconciler.py#L1157-L1193>`__.
7. `Sleep briefly (5s by default) <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/monitor.py#L178>`__, then go back to the sync phase.

.. note::

   All scaling decisions from steps 1–5 are accumulated purely in memory.
   No interaction with the cloud provider occurs until step 6.


Bin Packing and Work Group Selection
------------------------------------

For each `batch` of pending demands, the autoscaler applies the following scoring logic to evaluate each node and selects the one with the highest score for feasible requests.
It also applies the same scoring logic to each worker group and selects the one with the highest score to launch new instances.

`Scoring <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/scheduler.py#L430>`__ is based on a tuple of four values:

1. Whether the node is a GPU node and whether feasible requests require GPUs:

   - ``0`` if the node is a GPU node and requests do **not** require GPUs.
   - ``1`` if the node isn't a GPU node, or if requests do require GPUs.
2. The number of resource types on the node used by feasible requests.
3. The minimum utilization rate across all resource types used by feasible requests.
4. The average utilization rate across all resource types used by feasible requests.

In other words:

- The autoscaler avoids launching GPU nodes unless necessary.
- It prefers nodes that maximize utilization and minimize unused resources.

Example:

- Task requires **2 GPUs**.
- Two node types are available:

  - A: [GPU: 6]
  - B: [GPU: 2, TPU: 1]

Node type **A** should be selected, since node B would leave an unused TPU (it has the utilization rate of 0% on TPU and makes it less favorable in terms of the third scoring criterion).

This process repeats until all feasible pending demands can be scheduled or the maximum cluster size is reached.


Instance Manager and Cloud Provider
-----------------------------------

As described earlier, the autoscaler accumulates scaling decisions (steps 1–5) in memory and reconciles them with the cloud provider through the Instance Manager.

Scaling decisions are represented as a list of ``InstanceUpdateEvent`` records. For example:

- **For launching new instances**:
  - ``instance_id``: A randomly generated ID for Instance Manager tracking.
  - ``instance_type``: The type of instance to launch.
  - ``new_instance_status``: ``QUEUED``.

- **For terminating instances**:
  - ``instance_id``: The ID of the instance to stop.
  - ``new_instance_status``: ``TERMINATING`` or ``RAY_STOP_REQUESTED``.

These update events are passed to the Instance Manager, which transitions instance statuses.

A normal status transition flow is:

- ``QUEUED -> REQUESTED``: The Reconciler considers max_concurrent_launches and upscaling_speed when selecting an instance from the queue to transition ``REQUESTED`` during each reconciliation iteration.
- ``REQUESTED -> ALLOCATED``: Once the Reconciler detects the instance is allocated, it will transition the instance to ``ALLOCATED``.
- ``ALLOCATED -> RAY_INSTALLING`` If the cloud provider is not KubeRayProvider, the Reconciler will transition the instance to ``RAY_INSTALLING`` when the instance is allocated.
- ``RAY_INSTALLING -> RAY_RUNNING`` Once the Reconciler detects Ray is started on the instance, it will transition the instance to ``RAY_RUNNING``.
- ``RAY_RUNNING -> RAY_STOP_REQUESTED`` If the instance is idle, the Reconciler will transition the instance to ``RAY_STOP_REQUESTED`` to start draining the Ray process.
- ``RAY_STOP_REQUESTED -> RAY_STOPPING`` Once the Reconciler detects the Ray process is draining, it will transition the instance to ``RAY_STOPPING``.
- ``RAY_STOPPING -> RAY_STOPPED`` Once the Reconciler detects the Ray process is stopped, it will transition the instance to ``RAY_STOPPED``.
- ``RAY_STOPPED -> TERMINATING`` Once the Reconciler detects the Ray process is stopped, it will transition the instance to ``TERMINATING``.
- ``TERMINATING -> TERMINATED`` Once the Reconciler detects the instance is stopped, it will transition the instance to ``TERMINATED``.

You can find all valid instance status transitions in the `get_valid_transitions <https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/python/ray/autoscaler/v2/instance_manager/common.py#L193>`__ method.

Once transitions are triggered by the Reconciler, subscribers perform side effects, such as:

- ``QUEUED -> REQUESTED``: CloudInstanceUpdater launches the instance through the Cloud Provider.
- ``ALLOCATED -> RAY_INSTALLING``: ThreadedRayInstaller installs the Ray process.
- ``RAY_RUNNING -> RAY_STOP_REQUESTED``: RayStopper stops the Ray process on the instance.
- ``RAY_STOPPED -> TERMINATING``: CloudInstanceUpdater terminates the instance through the Cloud Provider.


.. note::

   Status transitions trigger side effects, but side effects don't trigger new status transitions directly.
   Instead, their results are observed from the external states at the beginning, the sync phase, and their new status transitions are triggered from the observations.


.. note::

   An implementation of the cloud provider interface in autoscaler v2 should provide methods for:

   - **Listing instances**: Return the set of instances currently managed by the provider.
   - **Launching instances**: Create new instances given the requested instance type and tags.
   - **Terminating instances**: Safely remove instances identified by their IDs.

   KubeRayProvider is one of the cloud provider implementations.

   NodeProviderAdapter is an adapter that can wrap a v1 node provider, such as AWSNodeProvider, to be a cloud provider.