.. _head-node-memory-management:  
  
Head Node Memory Management  
============================  
  
When running Ray clusters for extended periods, the head node's memory usage can steadily increase over time, potentially leading to out-of-memory (OOM) errors that can make the entire cluster unusable. This guide explains the causes of head node memory growth and provides mitigation strategies.  
  
.. contents::  
    :local:  
  
Why Head Node Memory Grows  
---------------------------  
  
- The Ray Dashboard provides a web interface for cluster monitoring and debugging. For more details, see :ref:`observability-getting-started`.
- The Ray Dashboard caches cluster events in memory for display and debugging purposes. The ``RAY_DASHBOARD_MAX_EVENTS_TO_CACHE`` environment variable controls the cache size. For implementation details, see the `event caching code <https://github.com/ray-project/ray/blob/814768317813afca2f0af740f58d024b059ae7d7/python/ray/dashboard/modules/event/event_head.py#L35>`_.  
- The dashboard processes and stores logs and metadata from jobs and workers, which accumulate over time in long-running clusters.  
  
Mitigation Strategies  
---------------------  

Avoid Scheduling on the Head Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Running tasks or actors on the head node isn't recommended because it hosts critical system components. Preventing scheduling on the head node helps reduce contention and memory pressure.

See :ref:`vms-large-cluster-configure-head-node` for head-node best practices.

Disable the Dashboard  
~~~~~~~~~~~~~~~~~~~~~  
  
If you don't need the dashboard, disabling it removes event caching and related memory overhead. This reduces observability into the system so it's not recommended for production clusters.
  
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
    Disabling the dashboard prevents KubeRay's ``RayJob`` and ``RayService`` features from working properly.


Kubernetes Configuration
------------------------

Head Pod Memory Settings  
~~~~~~~~~~~~~~~~~~~~~~~~  
  
When deploying on Kubernetes, configure appropriate memory requests and limits for the head pod.  
  
**Important:** Set memory and CPU resource requests equal to their limits. KubeRay uses the container's resource **limits** to configure Ray's logical resource capacities and ignores memory and CPU **requests**.  
  
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
  
For large clusters, a good starting specification for the head node is:
  
- **CPU:** 16 cores  
- **Memory:** 64 GB  
  
The actual requirements depend on your workload and cluster size.  
  
Additionally, consider preventing Ray from scheduling tasks on the head node by setting ``num-cpus: "0"`` in ``rayStartParams``.  
  
Best Practices  
--------------  

1. **Avoid scheduling on the head node** to reduce contention and memory pressure.
2. **Scale vertically and use a larger head node** before adjusting internal settings.
3. **Set appropriate Kubernetes resource limits** (match requests for memory and GPU).

.. note::
    You *can* disable the dashboard, but doing so severely limits observability and isn't **recommended for production**. If you choose to disable it, see the `Disable the Dashboard` section in the preceding text.

Troubleshooting
---------------  
  
If your head node experiences OOM issues:  
  
1. Check current memory usage: ``ray memory``. See :ref:`debug-with-ray-memory`
2. Consider increasing head node memory allocation

For more information on OOM prevention, see :ref:`ray-oom-prevention`.