.. _execution_configurations:

=================================
Execution Configurations
=================================

Ray Data provides a number of configurations that can be used to control various aspects
of Ray Dataset execution. These configurations can be modified by the user using
:class:`~ray.data.ExecutionOptions` and :class:`~ray.data.DataContext`. 
This guide describes the most important of these configurations and when to use them.

Configuring :class:`~ray.data.ExecutionOptions`
===============================================

The :class:`~ray.data.ExecutionOptions` class is used to configure options during Ray Dataset execution.
To use it, you can modify the attributes in the current :class:`~ray.data.DataContext` object's `execution_options`. For example:

.. code-block::

   ctx = ray.data.DataContext.get_current()
   ctx.execution_options.verbose_progress = True

* `resource_limits`: Set a soft limit on the resource usage during execution. Auto-detected by default.
* `exclude_resources`: Amount of resources to exclude from Ray Data. Set this if you have other workloads running on the same cluster. Note: 

  * If using Ray Data with Ray Train, training resources are automatically excluded. Otherwise, off by default.
  * For each resource type, ``resource_limits`` and ``exclude_resources`` can not be both set.

* `locality_with_output`: Set this to prefer running tasks on the same node as the output node (node driving the execution). It can also be set to a list of node ids to spread the outputs across those nodes. Off by default.
* `preserve_order`: Set this to preserve the ordering between blocks processed by operators under the streaming executor. Off by default.
* `actor_locality_enabled`: Whether to enable locality-aware task dispatch to actors. This parameter applies to both stateful :meth:`~ray.data.Dataset.map` and :meth:`~ray.data.Dataset.streaming_split` operations. Off by default.
* `verbose_progress`: Whether to report progress individually per operator. By default, only AllToAll operators and global progress is reported. This option is useful for performance debugging. On by default.

For more details on each of the preceding options, see the API documentation for :class:`~ray.data.ExecutionOptions`.

Configuring :class:`~ray.data.DataContext`
==========================================
The :class:`~ray.data.DataContext` class is used to configure more general options for Ray Data usage, such as observability/logging options,
error handling/retry behavior, and internal data formats. To use it, you can modify the attributes in the current :class:`~ray.data.DataContext` object. For example:

.. code-block::

   ctx = ray.data.DataContext.get_current()
   ctx.verbose_stats_logs = True

Many of the options in :class:`~ray.data.DataContext` are intended for advanced use cases or for debugging purposes, 
and most users should not need to modify them. However, some of the most important options are:

* `max_errored_blocks`: Max number of blocks that are allowed to have errors, unlimited if negative. This option allows application-level exceptions in block processing tasks. These exceptions may be caused by UDFs (for example, due to corrupted data samples) or IO errors. Data in the failed blocks are dropped. This option can be useful to prevent a long-running job from failing due to a small number of bad blocks. By default, no retries are allowed.
* `write_file_retry_on_errors`: A list of sub-strings of error messages that should trigger a retry when writing files. This is useful for handling transient errors when writing to remote storage systems. By default, retries on common transient AWS S3 errors.
* `verbose_stats_logs`: Whether stats logs should be verbose. This includes fields such as ``extra_metrics`` in the stats output, which are excluded by default. Off by default.
* `log_internal_stack_trace_to_stdout`: Whether to include internal Ray Data/Ray Core code stack frames when logging to ``stdout``. The full stack trace is always written to the Ray Data log file. Off by default.

For more details on each of the preceding options, see the API documentation for :class:`~ray.data.DataContext`.