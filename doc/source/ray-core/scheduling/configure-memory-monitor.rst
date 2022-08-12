Configure memory monitor
========================

The memory monitor periodically checks the memory usage of the node and reports whether 
the usage is above the threshold. The raylet uses this signal to evict worker processes to reduce the memory usage. This allows Ray to pre-emptively shed
the load and avoid node failure. If the head node fails due to running out of memory it may take down the cluster.

.. note::

  The memory monitor is only available on Linux. It looks at the node-wide memory usage and limit and does not work with container memory limit. It still works when Ray runs in a container that does not have a memory limit. In this case its limit is equal to the node and the usage may include other containers.

Eviction policy
---------------

When the memory monitor detects the usage is above the threshold, the raylet will look for the latest submitted task and kill its process. This applies to both actors and tasks. It selects and kills one process at a time regardless of how frequent the monitor runs. It will consume a retry for the task to be re-scheduled.

Configuration parameters
------------------------

The memory monitor is disabled by default and is enabled via environment variables.

`RAY_memory_usage_threshold_fraction [default_value=0, min=0.0, max=1.0, recommended=0.9]`: The usage fraction is calculated as (node memory used / node memory available). The memory here includes the shared memory allocated for the object store.

`RAY_memory_monitor_interval_ms [default_value=0, recommended=500]`: The monitor is disabled when this value is 0. This is the frequency in which the monitor computes the memory usage and notifies the raylet if the usage exceeds the threshold. Running the monitor more frequently allows it to respond faster to memory growth.

Detect and reduce worker evictions
-----------------------------------

The eviction events are exported as :ref:`application metrics <application-level-metrics>` under the name `memory_manager_worker_eviction_total`. The detailed messages of the memory monitor are written to the raylet log.

When the cluster is experiencing worker evictions the task or actor will retry and will fail when it runs out of retry. The options to reduce the number of evictions are

- Increase the memory-to-cpu ratio of the node
- Increase the cpu resource of the task and actor to reduce the parrallelism
- Estimate and set the memory resource of the task and actor




