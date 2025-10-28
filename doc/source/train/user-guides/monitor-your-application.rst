.. _train-metrics:

Ray Train Metrics
-----------------
Ray Train exports Prometheus metrics for CPU, memory, GPU, disk, and network. You can use these metrics to monitor Ray Train runs.
The Ray dashboard displays these metrics in Grafana panels. See :ref:`Ray Dashboard docs<observability-getting-started>` for more information.

The following table lists the Prometheus metrics emitted by Ray Train:

.. list-table:: Train Metrics
    :header-rows: 1

    * - Prometheus Metric
      - Labels
      - Description
    * - `ray_train_controller_state`
      - `ray_train_run_name`, `ray_train_run_id`, `ray_train_controller_state`
      - Current state of the Ray Train controller.
    * - `ray_train_worker_group_start_total_time_s`
      - `ray_train_run_name`, `ray_train_run_id`
      - Total time taken to start the worker group.
    * - `ray_train_worker_group_shutdown_total_time_s`
      - `ray_train_run_name`, `ray_train_run_id`
      - Total time taken to shut down the worker group.
    * - `ray_train_report_total_blocked_time_s`
      - `ray_train_run_name`, `ray_train_run_id`, `ray_train_worker_world_rank`, `ray_train_worker_actor_id`
      - Cumulative time in seconds to report a checkpoint to storage.
    * - `ray_node_cpu_utilization`
      - `instance`, `RayNodeType`
      - The CPU utilization per node as a percentage quantity (0..100). This should be scaled by the number of cores per node to convert the units into cores.
    * - `ray_node_cpu_count`
      - `instance`, `RayNodeType`
      - The number of CPU cores per node.
    * - `ray_node_mem_used`
      - `instance`, `RayNodeType`
      - The amount of physical memory used per node, in bytes.
    * - `ray_node_mem_total`
      - `instance`, `RayNodeType`
      - The amount of physical memory available per node, in bytes.
    * - `ray_node_mem_available`
      - `instance`, `RayNodeType`
      - The amount of physical memory available per node, in bytes.
    * - `ray_node_mem_shared_bytes`
      - `instance`, `RayNodeType`
      - The amount of shared memory per node, in bytes.
    * - `ray_node_gpus_utilization`
      - `instance`, `RayNodeType`, `GpuIndex`, `GpuDeviceName`
      - The GPU utilization per GPU as a percentage quantity (0..NGPU*100).
    * - `ray_node_gpus_available`
      - `instance`, `RayNodeType`, `GpuIndex`, `GpuDeviceName`
      - The number of GPUs available per node.
    * - `ray_node_gram_used`
      - `instance`, `RayNodeType`, `GpuIndex`, `GpuDeviceName`
      - The amount of GPU memory used per GPU, in megabytes.
    * - `ray_node_gram_available`
      - `instance`, `RayNodeType`, `GpuIndex`, `GpuDeviceName`
      - The amount of GPU memory available per GPU, in megabytes.
    * - `ray_node_disk_usage`
      - `instance`, `RayNodeType`
      - The amount of disk space used per node, in bytes.
    * - `ray_node_disk_free`
      - `instance`, `RayNodeType`
      - The amount of disk space available per node, in bytes.
    * - `ray_node_disk_io_read_speed`
      - `instance`, `RayNodeType`
      - The disk read throughput per node, in bytes per second.
    * - `ray_node_disk_io_write_speed`
      - `instance`, `RayNodeType`
      - The disk write throughput per node, in bytes per second.
    * - `ray_node_disk_read_iops`
      - `instance`, `RayNodeType`
      - The disk read operations per second per node.
    * - `ray_node_disk_write_iops`
      - `instance`, `RayNodeType`
      - The disk write operations per second per node.
    * - `ray_node_network_receive_speed`
      - `instance`, `RayNodeType`
      - The network receive throughput per node, in bytes per second.
    * - `ray_node_network_send_speed`
      - `instance`, `RayNodeType`
      - The network send throughput per node, in bytes per second.
    * - `ray_node_network_sent`
      - `instance`, `RayNodeType`
      - The total network traffic sent per node, in bytes.
    * - `ray_node_network_received`
      - `instance`, `RayNodeType`
      - The total network traffic received per node, in bytes.