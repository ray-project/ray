.. _ref-usage-stats:

Usage Stats Collection
======================

Starting in Ray 1.13, Ray collects usage stats data by default (guarded by an opt-out prompt).
The open-source Ray engineering team uses this data to better understand how to improve the libraries and core APIs, and how to prioritize bug fixes and enhancements.

Here are the guiding principles of the collection policy:

- **No surprises** — you are notified before data collection begins. You are notified of any changes to the data being collected or how it's used.
- **Easy opt-out:** You can easily opt-out of data collection
- **Transparency** — you can review all data that's sent
- **Control** — you have control over your data, and Ray honors requests to delete your data.
- Ray doesn't collect any personally identifiable data or proprietary code/data
- Ray doesn't sell data or buy data about you.

You can always :ref:`disable the usage stats collection <usage-disable>`.

For more context, refer to this `RFC <https://github.com/ray-project/ray/issues/20857>`_.

What data is collected?
-----------------------

Ray collects non-sensitive data that helps the team understand how Ray is used (for example, which Ray libraries are used).
**Personally identifiable data is never collected.** Check the UsageStatsToReport class to see the data Ray collects.

.. _usage-disable:

How to disable it
-----------------
There are multiple ways to disable usage stats collection before starting a cluster:

#. Add ``--disable-usage-stats`` option to the command that starts the Ray cluster (e.g., ``ray start --head --disable-usage-stats`` :ref:`command <ray-start-doc>`).

#. Run :ref:`ray disable-usage-stats <ray-disable-usage-stats-doc>` to disable collection for all future clusters. This won't affect currently running clusters. Under the hood, this command writes ``{"usage_stats": true}`` to the global config file ``~/.ray/config.json``.

#. Set the environment variable ``RAY_USAGE_STATS_ENABLED`` to 0 (for example, ``RAY_USAGE_STATS_ENABLED=0 ray start --head`` :ref:`command <ray-start-doc>`).

#. If you're using `KubeRay <https://github.com/ray-project/kuberay/>`_, you can add ``disable-usage-stats: 'true'`` to ``.spec.[headGroupSpec|workerGroupSpecs].rayStartParams.``.

Currently there is no way to enable or disable collection for a running cluster; you have to stop and restart the cluster.

How does it work?
-----------------

When a Ray cluster is started through :ref:`ray start --head <ray-start-doc>`, :ref:`ray up <ray-up-doc>`, :ref:`ray submit --start <ray-submit-doc>` or :ref:`ray exec --start <ray-exec-doc>`,
Ray decides whether usage stats collection should be enabled or not by considering the following factors in order:

#. It checks whether the environment variable ``RAY_USAGE_STATS_ENABLED`` is set: 1 means enabled and 0 means disabled.

#. If the environment variable isn't set, it reads the value of key ``usage_stats`` in the global config file ``~/.ray/config.json``: true means enabled and false means disabled.

#. If neither is set and the console is interactive, then the user is prompted to enable or disable the collection. If the console is non-interactive, usage stats collection is enabled by default. The decision is saved to ``~/.ray/config.json``, so the prompt is only shown once.

Note: usage stats collection isn't enabled when using local dev clusters started through ``ray.init()`` unless it's a nightly wheel. This means that Ray never collects data from third-party library users not using Ray directly.

If usage stats collection is enabled, a background process on the head node collects the usage stats
and reports to ``https://usage-stats.ray.io/`` every hour. The reported usage stats are also saved to
``/tmp/ray/session_xxx/usage_stats.json`` on the head node for inspection. Check the existence of this file to see if collection is enabled.

Usage stats collection is very lightweight and should have no impact on your workload in any way.

Requesting removal of collected data
------------------------------------

To request removal of collected data, email ``usage_stats@ray.io`` with the ``session_id`` that you can find in ``/tmp/ray/session_xxx/usage_stats.json``.

Frequently Asked Questions (FAQ)
--------------------------------

**Does the `session_id` map to personal data?**

No, the UUID is a Ray session/job-specific random ID that can't be used to identify a specific person nor machine. It doesn't live beyond the lifetime of your Ray session; and is primarily captured to enable the team to honor deletion requests.

The `session_id` is logged so that deletion requests can be honored.

**Could an enterprise easily configure an additional endpoint or substitute a different endpoint?**

Ray definitely sees this use case and would love to chat with you to make this work — email ``usage_stats@ray.io``.


Contact
-------
If you have any feedback regarding usage stats collection, email ``usage_stats@ray.io``.
