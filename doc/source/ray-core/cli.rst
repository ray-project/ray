Ray Core CLI
============

.. _ray-cli:

Debugging applications
----------------------
This section contains commands for inspecting and debugging the current cluster.

.. _ray-stack-doc:

.. click:: ray.scripts.scripts:stack
   :prog: ray stack
   :show-nested:

.. _ray-memory-doc:

.. click:: ray.scripts.scripts:memory
   :prog: ray memory
   :show-nested:

.. _ray-timeline-doc:

.. click:: ray.scripts.scripts:timeline
   :prog: ray timeline
   :show-nested:

.. _ray-status-doc:

.. click:: ray.scripts.scripts:status
   :prog: ray status
   :show-nested:

.. click:: ray.scripts.scripts:debug
   :prog: ray debug
   :show-nested:


Usage Stats
-----------
This section contains commands to enable/disable :ref:`Ray usage stats <ref-usage-stats>`.

.. _ray-disable-usage-stats-doc:

.. click:: ray.scripts.scripts:disable_usage_stats
   :prog: ray disable-usage-stats
   :show-nested:

.. _ray-enable-usage-stats-doc:

.. click:: ray.scripts.scripts:enable_usage_stats
   :prog: ray enable-usage-stats
   :show-nested: