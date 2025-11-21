.. _head-node-memory-management:  
  
Head Node Memory Management  
============================  
  
When running Ray clusters for extended periods, the head node's memory usage can steadily increase over time, potentially leading to out-of-memory (OOM) errors that can make the entire cluster unusable. This guide explains the causes of head node memory growth and provides mitigation strategies.  
  
.. contents::  
    :local:  
  
Why Head Node Memory Grows  
---------------------------  
  
- The Ray Dashboard provides a web interface for cluster monitoring and debugging. For more details, see :ref:`observability-getting-started`.
- The Ray Dashboard caches cluster events in memory for display and debugging purposes. The cache size is controlled by the ``RAY_DASHBOARD_MAX_EVENTS_TO_CACHE`` environment variable. For implementation details, see the `event caching code <https://github.com/ray-project/ray/blob/814768317813afca2f0af740f58d024b059ae7d7/python/ray/dashboard/modules/event/event_head.py#L35>`_.  
- The memory monitor periodically checks memory usage and adds overhead. This monitoring applies only if scheduling is enabled on the head node.  
- The dashboard processes and stores logs and metadata from jobs and workers, which accumulate over time in long-running clusters.  
  
Mitigation Strategies  
---------------------  

Avoid Scheduling on the Head Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Running tasks or actors on the head node is not recommended because it hosts critical system components. Preventing scheduling on the head node helps reduce contention and memory pressure.

See :ref:`vms-large-cluster-configure-head-node` for head-node best practices.

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
2. **Disable the dashboard** if not actively used for debugging  
3. **Set appropriate Kubernetes resource limits** matching requests for memory and GPU  
4. **Reserve sufficient memory** for system processes (at least 1GB for large clusters)  
  
Troubleshooting  
---------------  
  
If your head node experiences OOM issues:  
  
1. Check current memory usage: ``ray memory``. See :ref:`debug-with-ray-memory`
2. Verify dashboard event cache size is appropriate for your workload  
3. Consider increasing head node memory allocation  

For more information on OOM prevention, see :ref:`ray-oom-prevention`.