.. _ref-usage-stats:

Usage Stats Collection
======================

Starting in Ray 1.13, Ray collects usage stats data by default (guarded by an opt-out prompt).
This data will be used by the open-source Ray engineering team to better understand how to improve our libraries and core APIs, and how to prioritize bug fixes and enhancements.
For more context, please refer to this `RFC <https://github.com/ray-project/ray/issues/20857>`_.

What data is collected?
-----------------------
We collect non-sensitive data that helps us understand how Ray is used (e.g., which Ray libraries are used).
Personally identifiable data is never collected. Please check ``UsageStatsToReport`` in `usage_lib.py <https://github.com/ray-project/ray/blob/master/python/ray/_private/usage/usage_lib.py>`_ to see the data we collect.

How to disable it
-----------------
There are multiple ways to disable usage stats collection before starting a cluster:

#. Add ``--disable-usage-stats`` option to the command that starts the Ray cluster (e.g., ``ray start --head --disable-usage-stats``).

#. Run ``ray disable-usage-stats`` to disable collection for all future clusters. This won't affect currently running clusters. Under the hood, this command writes ``{"usage_stats": true}`` to the global config file ``~/.ray/config.json``.

#. Set the environment variable ``RAY_USAGE_STATS_ENABLED`` to 0 (e.g., ``RAY_USAGE_STATS_ENABLED=0 ray start --head``).

Currently there is no way to enable or disable collection for a running cluster; you have to stop and restart the cluster.

How does it work?
-----------------
When a Ray cluster is started via ``ray start --head``, ``ray up``, ``ray submit --start`` or ``ray exec --start``,
Ray will decide whether usage stats collection should be enabled or not by considering the following factors in order:

#. It checks whether the environment variable ``RAY_USAGE_STATS_ENABLED`` is set: 1 means enabled and 0 means disabled.

#. If the environment variable is not set, it reads the value of key ``usage_stats`` in the global config file ``~/.ray/config.json``: true means enabled and false means disabled.

#. If neither is set and the console is interactive, then the user will be prompted to enable or disable the collection. If the console is non-interactive, usage stats collection will be enabled by default. The decision will be saved to ``~/.ray/config.json``, so the prompt is only shown once.

Note: usage stats collection is not enabled when using local dev clusters started via ``ray.init()``. This means that Ray will never collect data from third-party library users not using Ray directly.

If usage stats collection is enabled, a background process on the head node will collect the usage stats
and report to ``https://usage-stats.ray.io/`` every hour. The reported usage stats will also be saved to
``/tmp/ray/session_xxx/usage_stats.json`` on the head node for inspection. You can check the existence of this file to see if collection is enabled.

Usage stats collection is very lightweight and should have no impact on your workload in any way.

How to request removal of collected data
----------------------------------------
To request removal of collected data, please email us at ``usage_stats@ray.io`` with the ``session_id`` that you can find in ``/tmp/ray/session_xxx/usage_stats.json``.

Contact us
----------
If you have any feedback regarding usage stats collection, please email us at ``usage_stats@ray.io``.
