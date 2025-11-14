.. _system-metrics:

System Metrics
--------------
Ray exports a number of system metrics, which provide introspection into the state of Ray workloads, as well as hardware utilization statistics. The following table describes the officially supported metrics:

.. note::

   Certain labels are common across all metrics, such as `SessionName` (uniquely identifies a Ray cluster instance), `instance` (per-node label applied by Prometheus), and `JobId` (Ray job ID, as applicable).

.. list-table:: Ray System Metrics
   :header-rows: 1

   * - Prometheus Metric
     - Labels
     - Description
   * - `ray_tasks`
     - `Name`, `State`, `IsRetry`
     - Current number of tasks (both remote functions and actor calls) by state. The State label (e.g., RUNNING, FINISHED, FAILED) describes the state of the task. See `rpc::TaskState <https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/common.proto#L583>`_ for more information. The function/method name is available as the Name label. If the task was retried due to failure or reconstruction, the IsRetry label will be set to "1", otherwise "0".
   * - `ray_actors`
     - `Name`, `State`
     - Current number of actors in each state described in `rpc::ActorTableData::ActorState <https://github.com/ray-project/ray/blob/b3799a53dcabd8d1a4d20f22faa98e781b0059c7/src/ray/protobuf/gcs.proto#L79>`. ALIVE has two sub-states: ALIVE_IDLE, and ALIVE_RUNNING_TASKS. An actor is considered ALIVE_IDLE if it is not running any tasks.
   * - `ray_resources`
     - `Name`, `State`, `instance`
     - Logical resource usage for each node of the cluster. Each resource has some quantity that is in either `USED or AVAILABLE state <https://github.com/ray-project/ray/blob/9eab65ed77bdd9907989ecc3e241045954a09cb4/src/ray/stats/metric_defs.cc#L188>`_. The Name label defines the resource name (e.g., CPU, GPU).
   * - `ray_object_store_memory`
     - `Location`, `ObjectState`, `instance`
     - Object store memory usage in bytes, `broken down <https://github.com/ray-project/ray/blob/9eab65ed77bdd9907989ecc3e241045954a09cb4/src/ray/stats/metric_defs.cc#L231>`_ by logical Location (SPILLED, MMAP_DISK, MMAP_SHM, and WORKER_HEAP). Definitions are as follows. SPILLED--Objects that have spilled to disk or a remote Storage solution (for example, AWS S3). The default is the disk. MMAP_DISK--Objects stored on a memory-mapped page on disk. This mode very slow and only happens under severe memory pressure. MMAP_SHM--Objects store on a memory-mapped page in Shared Memory. This mode is the default, in the absence of memory pressure. WORKER_HEAP--Objects, usually smaller, stored in the memory of the Ray Worker process itself. Small objects are stored in the worker heap.
   * - `ray_placement_groups`
     - `State`
     - Current number of placement groups by state. The State label (e.g., PENDING, CREATED, REMOVED) describes the state of the placement group. See `rpc::PlacementGroupTable <https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/gcs.proto#L517>`_ for more information.
   * - `ray_memory_manager_worker_eviction_total`
     - `Type`, `Name`
     - The number of tasks and actors killed by the Ray Out of Memory killer (https://docs.ray.io/en/master/ray-core/scheduling/ray-oom-prevention.html) broken down by types (whether it is tasks or actors) and names (name of tasks and actors).
   * - `ray_node_cpu_utilization`
     - `instance`
     - The CPU utilization per node as a percentage quantity (0..100). This should be scaled by the number of cores per node to convert the units into cores.
   * - `ray_node_cpu_count`
     - `instance`
     - The number of CPU cores per node.
   * - `ray_node_gpus_utilization`
     - `instance`, `GpuDeviceName`, `GpuIndex`
     - The GPU utilization per GPU as a percentage quantity (0..NGPU*100). `GpuDeviceName` is a name of a GPU device (e.g., NVIDIA A10G) and `GpuIndex` is the index of the GPU.
   * - `ray_node_disk_usage`
     - `instance`
     - The amount of disk space used per node, in bytes.
   * - `ray_node_disk_free`
     - `instance`
     - The amount of disk space available per node, in bytes.
   * - `ray_node_disk_write_iops`
     - `instance`, `node_type`
     - The disk write operations per second per node.
   * - `ray_node_disk_io_write_speed`
     - `instance`
     - The disk write throughput per node, in bytes per second.
   * - `ray_node_disk_read_iops`
     - `instance`, `node_type`
     - The disk read operations per second per node.
   * - `ray_node_disk_io_read_speed`
     - `instance`
     - The disk read throughput per node, in bytes per second.
   * - `ray_node_mem_available`
     - `instance`, `node_type`
     - The amount of physical memory available per node, in bytes.
   * - `ray_node_mem_shared_bytes`
     - `instance`, `node_type`
     - The amount of shared memory per node, in bytes.
   * - `ray_node_mem_used`
     - `instance`
     - The amount of physical memory used per node, in bytes.
   * - `ray_node_mem_total`
     - `instance`
     - The amount of physical memory available per node, in bytes.
   * - `ray_component_uss_mb`
     - `Component`, `instance`
     - The measured unique set size in megabytes, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
   * - `ray_component_cpu_percentage`
     - `Component`, `instance`
     - The measured CPU percentage, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
   * - `ray_node_gram_available`
     - `instance`, `node_type`, `GpuIndex`, `GpuDeviceName`
     - The amount of GPU memory available per GPU, in megabytes.
   * - `ray_node_gram_used`
     - `instance`, `GpuDeviceName`, `GpuIndex`
     - The amount of GPU memory used per GPU, in bytes.
   * - `ray_node_network_received`
     - `instance`, `node_type`
     - The total network traffic received per node, in bytes.
   * - `ray_node_network_sent`
     - `instance`, `node_type`
     - The total network traffic sent per node, in bytes.
   * - `ray_node_network_receive_speed`
     - `instance`
     - The network receive throughput per node, in bytes per second.
   * - `ray_node_network_send_speed`
     - `instance`
     - The network send throughput per node, in bytes per second.
   * - `ray_cluster_active_nodes`
     - `node_type`
     - The number of healthy nodes in the cluster, broken down by autoscaler node type.
   * - `ray_cluster_failed_nodes`
     - `node_type`
     - The number of failed nodes reported by the autoscaler, broken down by node type.
   * - `ray_cluster_pending_nodes`
     - `node_type`
     - The number of pending nodes reported by the autoscaler, broken down by node type.

Metrics Semantics and Consistency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray guarantees all its internal state metrics are *eventually* consistent even in the presence of failures--- should any worker fail, eventually the right state will be reflected in the Prometheus time-series output. However, any particular metrics query is not guaranteed to reflect an exact snapshot of the cluster state.

For the `ray_tasks` and `ray_actors` metrics, you should use sum queries to plot their outputs (e.g., ``sum(ray_tasks) by (Name, State)``). The reason for this is that Ray's task metrics are emitted from multiple distributed components. Hence, there are multiple metric points, including negative metric points, emitted from different processes that must be summed to produce the correct logical view of the distributed system. For example, for a single task submitted and executed, Ray may emit  ``(submitter) SUBMITTED_TO_WORKER: 1, (executor) SUBMITTED_TO_WORKER: -1, (executor) RUNNING: 1``, which reduces to ``SUBMITTED_TO_WORKER: 0, RUNNING: 1`` after summation.
