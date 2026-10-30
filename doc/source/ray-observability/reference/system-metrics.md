---
myst:
  html_meta:
    description: "Reference table of the system metrics Ray exports for tasks, actors, resources, and hardware utilization, with semantics and consistency guarantees."
---

(system-metrics)=

# System Metrics
Ray exports a number of system metrics, which provide introspection into the state of Ray workloads, as well as hardware utilization statistics. The following table describes the officially supported metrics:

:::{note}
Certain labels are common across all metrics, such as `SessionName` (uniquely identifies a Ray cluster instance), `instance` (per-node label applied by Prometheus), and `JobId` (Ray job ID, as applicable).

Starting with Ray 2.53+, the `WorkerId` label is no longer exported by default due to its high cardinality. The Ray team doesn't expect this to be a breaking change, as none of Ray’s built-in components rely on this label. However, if you have custom tooling that depends on `WorkerId` label, take note of this change.

You can restore or adjust label behavior using the environment variable `RAY_metric_cardinality_level`:

- `legacy`: Preserve all labels. (This was the default behavior before Ray 2.53.)
- `recommended`: Drop high-cardinality labels. Ray internally determines specific labels; currently this includes only `WorkerId`. (This is the default behavior since Ray 2.53.)
- `low`: Same as `recommended`, but also drops the Name label for tasks and actors.
:::

:::{note}
Ray uses an optional NVML API to collect per-process GPU SM utilization. Ray automatically skips this API for devices whose name matches a known PPU device-name pattern (for example, `PPU-ZW810`). For other NVML-compatible libraries that don't safely support this API, set `RAY_SKIP_PROCESS_UTIL_API=true` on each affected Ray node. To skip specific device names, use the comma-separated `RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES` environment variable (names are matched case-insensitively and exactly).

These variables must be set in the Ray node and Dashboard Reporter process environment before `ray start` (for example, in the Pod environment when using KubeRay). They are not applied through `runtime_env`. Ray continues to report GPU process IDs, allocated GPU memory, and device-level utilization, memory, power, and temperature metrics. Only per-process GPU utilization is unavailable.
:::

(ascend-npu-monitoring)=
## Monitor Huawei Ascend NPUs

The Dashboard Reporter on each Ray node can collect Huawei Ascend NPU metrics directly from the Device Configuration Management Interface (DCMI). Ray loads `libdcmi.so` with `ctypes`; it doesn't invoke `npu-smi`, and the collector only calls read-only monitoring APIs.

### Ascend and DCMI prerequisites

Install a compatible Ascend driver on every Ray node that reports NPU metrics. The driver installation must provide `libdcmi.so` and the user that runs Ray must have permission to access the Ascend management and device interfaces. In a container, mount the DCMI library, its transitive shared-library dependencies, and the required Ascend device files into the Ray container.

The CANN toolkit and its `set_env.sh` script configure the Ascend runtime environment, but the toolkit alone doesn't replace the driver-provided DCMI library. Source the script before starting each Ray node so that the Dashboard Reporter inherits the environment. Adjust the path for your CANN installation:

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
export RAY_NPU_DCMI_LIBRARY_PATH=/usr/local/Ascend/driver/lib64/driver/libdcmi.so

# Start the head node or a worker only after setting the environment.
ray start --head
# ray start --address=<head-node-address>
```

Set these variables in the Ray node process environment, such as the container environment or KubeRay Pod template. Setting them through a Job or Task `runtime_env` is too late because the Dashboard Reporter starts with the Ray node. The reporter attempts DCMI initialization once per process, so restart the Ray node after correcting the environment or library path.

The following environment variables have different roles:

```{list-table}
:header-rows: 1

* - Environment variable
  - Meaning for NPU monitoring
* - `RAY_NPU_DCMI_LIBRARY_PATH`
  - Overrides the DCMI shared-library path used by the Dashboard Reporter. Set it to the library file, not its containing directory. An absolute path is recommended.
* - `LD_LIBRARY_PATH`
  - Helps the dynamic loader resolve `libdcmi.so` and its transitive dependencies. The CANN `set_env.sh` script normally adds the relevant Ascend library directories.
* - `ASCEND_RT_VISIBLE_DEVICES`
  - Controls the NPU devices assigned or exposed to Ray Tasks and Actors. The node-level monitoring provider enumerates devices through DCMI and doesn't filter its results using this variable. See {ref}`Accelerator support <accelerator-support>` for scheduling behavior.
* - `ASCEND_HOME_PATH`, `ASCEND_OPP_PATH`, `PATH`, and `PYTHONPATH`
  - Support CANN applications and command-line tools. The exact variables set by `set_env.sh` depend on the CANN version. The DCMI collector itself primarily requires the shared library and its loader dependencies.
```

When `RAY_NPU_DCMI_LIBRARY_PATH` is nonempty, Ray tries only that value. If the variable is unset or empty, Ray tries these candidates in order:

1. `/usr/local/Ascend/driver/lib64/driver/libdcmi.so`
2. `/usr/local/Ascend/driver/lib64/common/libdcmi.so`
3. `libdcmi.so`, resolved by the system dynamic loader

An invalid explicit override doesn't fall back to the default candidates. If Ray can't load or initialize DCMI, the Dashboard Reporter continues running but emits no Ascend NPU metrics.

Ray requires `dcmi_init` to initialize DCMI, `dcmi_get_card_list` to enumerate cards, and `dcmi_get_device_num_in_card` to enumerate devices. The remaining queries are optional per device: failures omit only the affected field or metric.

```{list-table}
:header-rows: 1

* - Data
  - DCMI API
  - Fallback behavior
* - Logical and physical IDs
  - `dcmi_get_device_logic_id`, `dcmi_get_device_phyid_from_logicid`
  - Ray assigns a unique negative synthetic index if the logical ID is unavailable and omits the physical ID if it can't be queried.
* - Device name
  - `dcmi_get_device_chip_info_v2`, then `dcmi_get_device_chip_info`
  - Ray reports `Ascend NPU` if neither chip-info query succeeds.
* - AI Core utilization
  - `dcmi_get_device_utilization_rate`
  - Ray omits the utilization value.
* - HBM capacity and usage
  - `dcmi_get_device_hbm_info`
  - Ray omits the HBM metrics.
* - Power, temperature, and health
  - `dcmi_get_device_power_info`, `dcmi_get_device_temperature`, `dcmi_get_device_health`
  - Ray independently omits each unavailable metric.
* - Per-process NPU memory
  - `dcmi_get_device_resource_info`
  - Ray omits per-process and per-component NPU memory information.
```

### Units and availability

DCMI reports HBM capacity and usage in MiB, which Ray converts to bytes for node-level metrics. Ray converts DCMI power values from tenths of a watt to milliwatts. The `ray_component_npu_memory_mb` metric uses MiB (`1 MiB = 1,048,576 bytes`) despite the `_mb` suffix. Device power is emitted only for device `0` on each card because DCMI reports board-level power and otherwise the value could be counted more than once.

The Dashboard API uses the same base units: `utilizationAicore` is a percentage, `memoryUsed` and `memoryTotal` are bytes, `powerMw` is milliwatts, and `temperatureC` is degrees Celsius. `healthStatus` and the Prometheus health metric use normalized health levels: `0` is OK, `1` is minor, `2` is major, `3` is critical, and `4` means that health information is missing. Ray maps the DCMI missing value `0xFFFFFFFF` to `4`.

### NPU metric labels

All node-level NPU metrics have the following Ray labels. Prometheus normally adds an `instance` label for the scrape target; its exact value depends on the Prometheus scrape and relabel configuration.

```{list-table}
:header-rows: 1

* - Label
  - Meaning
* - `ip`
  - IP address reported by the Ray node.
* - `Version`
  - Ray version used by the reporting process.
* - `SessionName`
  - Ray session identifier created when the head starts a cluster session and shared by the cluster's reporters. A new cluster session has a new value.
* - `IsHeadNode`
  - Legacy string label, `true` for the head node and `false` for a worker node.
* - `RayNodeType`
  - Ray node role, either `head` or `worker`.
* - `NpuDeviceName`
  - Chip name returned by DCMI, or `Ascend NPU` when unavailable.
* - `NpuIndex`
  - DCMI logical device ID used by `ASCEND_RT_VISIBLE_DEVICES`. Ray assigns a unique negative synthetic index when the logical ID query fails.
* - `NpuPhysicalId`
  - Optional DCMI physical device ID corresponding to `NpuIndex`.
* - `NpuCardId`
  - Card ID returned by DCMI card enumeration.
* - `NpuDeviceId`
  - Device ID within the DCMI card.
```

`ray_component_npu_memory_mb` aggregates NPU memory by Ray component and uses `ip`, `pid`, `Version`, `Component`, and `SessionName`, in addition to the Prometheus `instance` label. For workers, `Component` is the worker process name, such as `ray::<task-name>` or `ray::<actor-class>`, so multiple processes with the same name are grouped. The `pid` label can be empty for such grouped worker metrics. This metric doesn't contain device labels because a process can allocate memory on multiple NPUs and Ray sums those allocations before reporting the component value.

```{list-table} Ray System Metrics
:header-rows: 1

* - Prometheus Metric
  - Labels
  - Description
* - `ray_tasks`
  - `Name`, `State`, `IsRetry`
  - Current number of tasks (both remote functions and actor calls) by state. The State label (e.g., RUNNING, FINISHED, FAILED) describes the state of the task. See [rpc::TaskState](https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/common.proto#L583) for more information. The function/method name is available as the Name label. If the task was retried due to failure or reconstruction, the IsRetry label will be set to "1", otherwise "0".
* - `ray_actors`
  - `Name`, `State`
  - Current number of actors in each state described in [rpc::ActorTableData::ActorState](https://github.com/ray-project/ray/blob/b3799a53dcabd8d1a4d20f22faa98e781b0059c7/src/ray/protobuf/gcs.proto#L79). ALIVE has two sub-states: ALIVE_IDLE, and ALIVE_RUNNING_TASKS. An actor is considered ALIVE_IDLE if it is not running any tasks.
* - `ray_resources`
  - `Name`, `State`, `instance`
  - Logical resource usage for each node of the cluster. Each resource has some quantity that's either in the USED or AVAILABLE state. The Name label defines the resource name (e.g., CPU, GPU).
* - `ray_object_store_memory`
  - `Location`, `ObjectState`, `instance`
  - Object store memory usage in bytes, broken down by logical Location (SPILLED, MMAP_DISK, MMAP_SHM, and WORKER_HEAP). Definitions are as follows. SPILLED--Objects that have spilled to disk or a remote Storage solution (for example, AWS S3). The default is the disk. MMAP_DISK--Objects stored on a memory-mapped page on disk. This mode very slow and only happens under severe memory pressure. MMAP_SHM--Objects store on a memory-mapped page in Shared Memory. This mode is the default, in the absence of memory pressure. WORKER_HEAP--Objects, usually smaller, stored in the memory of the Ray Worker process itself. Small objects are stored in the worker heap.
* - `ray_placement_groups`
  - `State`
  - Current number of placement groups by state. The State label (e.g., PENDING, CREATED, REMOVED) describes the state of the placement group. See [rpc::PlacementGroupTable](https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/gcs.proto#L517) for more information.
* - `ray_gcs_storage_operation_count_total`
  - `Operation`, `TableName`
  - Number of operations the GCS invoked on its storage backend, broken down by operation and by the GCS table it addressed. `Operation` is one of `Put`, `Get`, `GetAll`, `MultiGet`, `Delete`, `BatchDelete`, `GetKeys`, `Exists`, or `GetNextJobID`. `TableName` is bounded to the names of Ray's internal `TablePrefix` enum, plus `JobCounter` for `GetNextJobID` and `OTHER` for a table name outside that enum. The values the GCS produces in practice are `JOB`, `ACTOR`, `ACTOR_TASK_SPEC`, `PLACEMENT_GROUP`, `NODE`, `WORKERS`, and `KV`. The remaining enum names are legacy, and no code path emits them. Keys, internal-KV namespaces, and any other user-supplied string never appear in labels. Recorded when the operation is issued, so this metric leads `ray_gcs_storage_operation_latency_ms` by the number of operations in flight. Exported on every GCS storage backend. On Redis, one operation can issue several Redis commands (see `ray_gcs_redis_command_count_total`), and `RAY_gcs_redis_storage_metrics_enabled=false` stops recording this metric and `ray_gcs_storage_operation_latency_ms`.
* - `ray_gcs_storage_operation_latency_ms`
  - `Operation`, `TableName`
  - Time to invoke an operation on the GCS storage backend, with the same label domain as `ray_gcs_storage_operation_count_total`. Recorded when the operation completes.
* - OpenTelemetry: `ray_gcs_redis_request_payload_bytes`; OpenCensus: `ray_gcs_redis_request_payload_bytes_total` (plus the deprecated unsuffixed gauge when `RAY_EXPORT_COUNTER_AS_GAUGE=true`)
  - `Command`, `TableName`
  - Application bytes in Redis command arguments accepted for sending by the GCS Redis client, by command and GCS table. Exported only when the GCS storage backend is Redis. Includes the verb, Redis key, field names, and values; excludes RESP framing, TLS, and TCP/IP overhead. Recorded on the first successful submission of each logical command; retries are not counted again. `Command` is the actual verb normalized to uppercase ASCII and truncated to 16 bytes; this limits label length, not the number of distinct labels, so production verbs must remain code-controlled. `TableName` is a GCS table, `NONE`, or `ALL`.
* - OpenTelemetry: `ray_gcs_redis_response_payload_bytes`; OpenCensus: `ray_gcs_redis_response_payload_bytes_total` (plus the deprecated unsuffixed gauge when `RAY_EXPORT_COUNTER_AS_GAUGE=true`)
  - `Command`, `TableName`
  - Application bytes in Redis replies received by the GCS, by command and GCS table. Exported only when the GCS storage backend is Redis. Includes bulk/status strings, HSCAN field names, and integer decimal text; excludes nil replies, error replies that are retried, RESP framing, TLS, and TCP/IP overhead.
* - OpenTelemetry and OpenCensus: `ray_gcs_redis_command_count_total`
  - `Command`, `TableName`
  - Number of logical Redis commands accepted for sending by the GCS Redis client, by command and GCS table. Exported only when the GCS storage backend is Redis. Batches count per chunk and table scans per HSCAN command; retries are not counted again, so this is not a count of network round trips. All three metrics exclude connection-establishment commands sent through synchronous or raw connection paths, such as `AUTH`, `SENTINEL MASTERS`, and `INFO CLUSTER`; they do not measure total Redis network traffic. Divide the byte metrics by this for mean bytes per logical command. Set `RAY_gcs_redis_payload_metrics_enabled=false` to stop recording all three metrics. Ray's `Sum` instrument is exposed as an OpenTelemetry up-down counter even though these byte metrics only record positive deltas, which is why its Prometheus name has no `_total` suffix on that backend.
* - `ray_memory_manager_worker_eviction_total`
  - `Type`, `Name`
  - The number of tasks and actors killed by the Ray Out of Memory killer (<https://docs.ray.io/en/master/ray-core/scheduling/ray-oom-prevention.html>) broken down by types (whether it is tasks or actors) and names (name of tasks and actors).
* - `ray_node_cpu_utilization`
  - `instance`
  - The CPU utilization per node as a percentage quantity (0.\.100). This should be scaled by the number of cores per node to convert the units into cores.
* - `ray_node_cpu_count`
  - `instance`
  - The number of CPU cores per node.
* - `ray_node_gpus_utilization`
  - `instance`, `GpuDeviceName`, `GpuIndex`, `GpuUuid`
  - The GPU utilization per GPU as a percentage quantity (0.\.100). `GpuDeviceName` is a name of a GPU device (e.g., NVIDIA A10G), `GpuIndex` is the index of the GPU, and `GpuUuid` is the unique device identifier. The series is absent for a device that does not report utilization, such as a MIG slice or a device whose NVML query fails; it is never -1.
* - `ray_node_gpus_available`
  - `instance`, `GpuDeviceName`, `GpuIndex`, `GpuUuid`
  - The number of GPUs available.
* - `ray_node_gpu_power_milliwatts`
  - `instance`, `GpuDeviceName`, `GpuIndex`, `GpuUuid`
  - The current GPU power per GPU, in milliwatts.
* - `ray_node_gpu_temperature_celsius`
  - `instance`, `GpuDeviceName`, `GpuIndex`, `GpuUuid`
  - The current GPU temperature per GPU, in Celsius.
* - `ray_tpu_tensorcore_utilization`
  - `instance`, `TpuIndex`, `TpuDeviceName`, `TpuType`, `TpuTopology`
  - The TensorCore utilization percentage per TPU device (0.\.100). Exported on Google Cloud TPU nodes when the GKE TPU device plugin metric server is active. See {ref}`What the KubeRay TPU webhook configures <kuberay-tpu-webhook-config>`.
* - `ray_tpu_memory_bandwidth_utilization`
  - `instance`, `TpuIndex`, `TpuDeviceName`, `TpuType`, `TpuTopology`
  - The high-bandwidth memory (HBM) bandwidth utilization percentage per TPU device (0.\.100).
* - `ray_tpu_duty_cycle`
  - `instance`, `TpuIndex`, `TpuDeviceName`, `TpuType`, `TpuTopology`
  - The percentage of the sample period that the TPU spends actively processing (0.\.100).
* - `ray_tpu_memory_used`
  - `instance`, `TpuIndex`, `TpuDeviceName`, `TpuType`, `TpuTopology`
  - The amount of HBM allocated per TPU device, in bytes.
* - `ray_tpu_memory_total`
  - `instance`, `TpuIndex`, `TpuDeviceName`, `TpuType`, `TpuTopology`
  - The total amount of HBM available per TPU device, in bytes.
* - `ray_node_npus_available`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The number of Ascend NPUs available. Ray emits a value of `1` for each device that DCMI enumerates.
* - `ray_node_npus_utilization`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The Ascend NPU AI Core utilization per device as a percentage from 0 to 100.
* - `ray_node_npu_hbm_used`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The amount of Ascend NPU HBM used per device, in bytes.
* - `ray_node_npu_hbm_available`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The amount of Ascend NPU HBM available per device, in bytes. Ray calculates this as total HBM minus used HBM and clamps the value to zero.
* - `ray_node_npu_power_milliwatts`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The current Ascend NPU board power, in milliwatts. Ray emits this metric only for device `0` on each card to avoid double counting board-level power.
* - `ray_node_npu_temperature_celsius`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The current Ascend NPU temperature per device, in degrees Celsius.
* - `ray_node_npu_health`
  - `instance`, `NpuDeviceName`, `NpuIndex`, `NpuPhysicalId`, `NpuCardId`, `NpuDeviceId`
  - The normalized Ascend NPU health level per device: `0` is OK, `1` is minor, `2` is major, `3` is critical, and `4` means that health information is missing.
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
* - `ray_node_mem_used_host`
  - `instance`
  - The host (OS-level) physical memory used per node, in bytes.
* - `ray_node_mem_total_host`
  - `instance`
  - The host (OS-level) total physical memory per node, in bytes.
* - `ray_node_cgroup_mem_used`
  - `instance`
  - The container memory usage per node (from cgroup), in bytes. Only emitted when cgroup memory limits are present.
* - `ray_node_cgroup_mem_total`
  - `instance`
  - The container memory limit per node (from cgroup), in bytes. Only emitted when cgroup memory limits are present.

* - `ray_component_rss_mb`
  - `Component`, `instance`
  - Note: This metric will be deprecated in the future, please use `ray_component_rss_bytes` instead. The measured resident set size in megabytes, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
* - `ray_component_rss_bytes`
  - `Component`, `instance`
  - The measured resident set size in bytes, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
* - `ray_component_shared_bytes`
  - `Component`, `instance`
  - The measured shared memory in bytes, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
* - `ray_component_uss_mb`
  - `Component`, `instance`
  - Note: This metric will be deprecated in the future, please use `ray_component_uss_bytes` instead. The measured unique set size in megabytes, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
* - `ray_component_uss_bytes`
  - `Component`, `instance`
  - The measured unique set size in bytes, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
* - `ray_component_cpu_percentage`
  - `Component`, `instance`
  - The measured CPU percentage, broken down by logical Ray component. Ray components consist of system components (e.g., raylet, gcs, dashboard, or agent) and the method names of running tasks/actors.
* - `ray_component_npu_memory_mb`
  - `Component`, `instance`
  - The Ascend NPU memory allocated by worker and driver processes, grouped by Ray component, in MiB (`1 MiB = 1,048,576 bytes`).
* - `ray_node_gram_available`
  - `instance`, `node_type`, `GpuIndex`, `GpuDeviceName`, `GpuUuid`
  - The amount of GPU memory available per GPU, in megabytes.
* - `ray_node_gram_used`
  - `instance`, `GpuDeviceName`, `GpuIndex`, `GpuUuid`
  - The amount of GPU memory used per GPU, in megabytes.
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
```

## Metrics Semantics and Consistency

Ray guarantees all its internal state metrics are *eventually* consistent even in the presence of failures--- should any worker fail, eventually the right state will be reflected in the Prometheus time-series output. However, any particular metrics query is not guaranteed to reflect an exact snapshot of the cluster state.

For the `ray_tasks` and `ray_actors` metrics, you should use sum queries to plot their outputs (e.g., `sum(ray_tasks) by (Name, State)`). The reason for this is that Ray's task metrics are emitted from multiple distributed components. Hence, there are multiple metric points, including negative metric points, emitted from different processes that must be summed to produce the correct logical view of the distributed system. For example, for a single task submitted and executed, Ray may emit  `(submitter) SUBMITTED_TO_WORKER: 1, (executor) SUBMITTED_TO_WORKER: -1, (executor) RUNNING: 1`, which reduces to `SUBMITTED_TO_WORKER: 0, RUNNING: 1` after summation.
