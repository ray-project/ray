.. _head-node-memory-management:  
  
Head Node Memory Management  
============================  
  
When running Ray clusters for extended periods, the head node's memory usage can steadily increase over time, potentially leading to out-of-memory (OOM) errors that can make the entire cluster unusable. This guide explains the causes of head node memory growth and provides mitigation strategies.  
  
.. contents::  
    :local:  
  
Why Head Node Memory Grows  
---------------------------  
  
The head node runs critical ray system processes such as the autoscaler, GCS (Global Control Service), and Ray driver processes. These processes can consume significant memory over time. 
  
Dashboard Event Caching  
~~~~~~~~~~~~~~~~~~~~~~~  
  
The Ray Dashboard caches cluster events in memory for display and debugging purposes. The cache size is controlled by the ``RAY_DASHBOARD_MAX_EVENTS_TO_CACHE`` environment variable (default: 10,000 events).  
  
Metrics and Reporting Overhead  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  
The system periodically collects and reports resource usage and metrics. The reporting interval is controlled by ``raylet_report_resources_period_milliseconds`` (default: 100ms).  
  
Memory Monitor Overhead  
~~~~~~~~~~~~~~~~~~~~~~~  
  
The memory monitor periodically checks memory usage, controlled by ``memory_monitor_refresh_ms`` (default: 250ms). While necessary for OOM prevention, this monitoring adds overhead.  
  
Job and Worker Metadata  
~~~~~~~~~~~~~~~~~~~~~~~  
  
The dashboard processes and stores logs and metadata from jobs and workers, which accumulates over time in long-running clusters.  
  
Mitigation Strategies  
---------------------  
  
Disable the Dashboard  
~~~~~~~~~~~~~~~~~~~~~  
  
If you don't need the dashboard, disabling it removes event caching and related memory overhead.  
  
**Python API:**  
  
.. code-block:: python  
  
    import ray  
    ray.init(include_dashboard=False)  
  
**CLI:**  
  
.. code-block:: bash  
  
    ray start --head --include-dashboard=False  
  
**Kubernetes:**  
  
Set ``spec.headGroupSpec.rayStartParams.include-dashboard`` to ``"false"`` in your RayCluster configuration.  
  
.. warning::  
    Disabling the dashboard will prevent KubeRay's ``RayJob`` and ``RayService`` features from working properly.  
  
Adjust Event Cache Size  
~~~~~~~~~~~~~~~~~~~~~~~~  
  
Reduce the dashboard event cache size by setting the environment variable before starting Ray:  
  
.. code-block:: bash  
  
    export RAY_DASHBOARD_MAX_EVENTS_TO_CACHE=5000  
    ray start --head  
  
Enable Resource Isolation  
~~~~~~~~~~~~~~~~~~~~~~~~~~  
  
Resource isolation reserves memory and CPU for Ray system processes through cgroupv2, preventing user workloads from consuming all available resources.  
  
**CLI Options:**  
  
.. code-block:: bash  
  
    ray start --head \  
        --enable-resource-isolation \  
        --system-reserved-memory=1073741824 \  
        --system-reserved-cpu=1.0  
  
- ``--enable-resource-isolation``: Enables cgroupv2-based resource isolation  
- ``--system-reserved-memory``: Memory in bytes to reserve for Ray system processes (default: min 500MB, max 10GB)  
- ``--system-reserved-cpu``: CPU cores to reserve for Ray system processes (default: min 1 core, max 3 cores)  
  
**Python API:**  
  
.. code-block:: python  
  
    import ray  
    ray.init(  
        enable_resource_isolation=True,  
        system_reserved_memory=1024**3,  # 1GB  
        system_reserved_cpu=1.0  
    )  
  
.. note::  
    Resource isolation requires cgroupv2 with read/write permissions for the raylet. Cgroup memory and CPU controllers must be enabled.  
  
Configure Memory Monitor  
~~~~~~~~~~~~~~~~~~~~~~~~~  
  
Adjust memory monitoring thresholds and intervals:  
  
- ``RAY_memory_usage_threshold``: Threshold for memory usage (default: 0.95, range: 0-1)  
- ``RAY_memory_monitor_refresh_ms``: Memory monitor refresh interval (default: 250ms, set to 0 to disable)  
  
.. code-block:: bash  
  
    RAY_memory_usage_threshold=0.90 ray start --head  
  
Kubernetes Configuration  
------------------------  
  
Head Pod Memory Settings  
~~~~~~~~~~~~~~~~~~~~~~~~  
  
When deploying on Kubernetes, configure appropriate memory requests and limits for the head pod.  
  
**Important:** Set memory and GPU resource requests equal to their limits. KubeRay uses the container's resource **limits** to configure Ray's logical resource capacities and ignores memory and GPU **requests**.  
  
Example configuration:  
  
.. code-block:: yaml  
  
    headGroupSpec:  
      template:  
        spec:  
          containers:  
          - name: ray-head  
            resources:  
              requests:  
                memory: "8Gi"  
                cpu: "4"  
              limits:  
                memory: "8Gi"  
                cpu: "4"  
  
Recommended Head Node Specifications  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  
For large clusters (1000+ nodes), a good starting specification for the head node is:  
  
- **CPU:** 8 cores  
- **Memory:** 32 GB  
  
The actual requirements depend on your workload and cluster size.  
  
Additionally, consider preventing Ray from scheduling tasks on the head node by setting ``num-cpus: "0"`` in ``rayStartParams``.  
  
Best Practices  
--------------  
  
1. **Start with conservative settings** and adjust based on observed behavior  
2. **Enable resource isolation** in production environments to prevent system process starvation  
3. **Disable the dashboard** if not actively used for debugging  
4. **Set appropriate Kubernetes resource limits** matching requests for memory and GPU  
5. **Reserve sufficient memory** for system processes (at least 1GB for large clusters)  
  
Troubleshooting  
---------------  
  
If your head node experiences OOM issues:  
  
1. Check current memory usage: ``ray memory``  
2. Verify dashboard event cache size is appropriate for your workload  
3. Ensure resource isolation is properly configured if enabled  
4. Consider increasing head node memory allocation  

For more information on OOM prevention, see :ref:`ray-oom-prevention`.