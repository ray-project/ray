.. _execution_configurations:

========================
Execution Configurations
========================

Ray Data provides a number of configuration options that control various aspects
of execution of Ray Data's :class:`~ray.data.Dataset` on top of configuration of the Ray Core cluster itself.

Ray Data's configuration is primarily controlled through either of :class:`~ray.data.ExecutionOptions`
or :class:`~ray.data.DataContext`.

This guide describes the most important of these configurations and when to use them.

Configuring :class:`~ray.data.ExecutionOptions`
===============================================

The :class:`~ray.data.ExecutionOptions` class is used to configure options during Ray Dataset execution.
To use it, modify the attributes in the current :class:`~ray.data.DataContext` object's `execution_options`. For example:

.. testcode::
   :hide:

   import ray

.. testcode::

   ctx = ray.data.DataContext.get_current()
   ctx.execution_options.verbose_progress = True

* `resource_limits`: Set a soft limit on the resource usage during execution. For example, if there are other parts of the code which require some minimum amount of resources, you may want to limit the amount of resources that Ray Data uses. Auto-detected by default.
* `exclude_resources`: Deprecated. Use ``label_selector`` to constrain Ray Data work to labeled nodes.

* `preserve_order`: Set this to preserve the ordering between blocks processed by operators under the streaming executor. Off by default.
* `actor_locality_enabled`: Deprecated. Ray Data manages actor locality internally.
* `verbose_progress`: Whether to report progress individually per operator. By default, only AllToAll operators and global progress is reported. This option is useful for performance debugging. On by default.

For more details on each of the preceding options, see :class:`~ray.data.ExecutionOptions`.

Configuring :class:`~ray.data.DataContext`
==========================================

The :class:`~ray.data.DataContext` class is used to configure more general options for Ray Data usage, such as observability/logging options,
error handling/retry behavior, and internal data formats. To use it, modify the attributes in the current :class:`~ray.data.DataContext` object. For example:

.. testcode::
	   :hide:

	   import ray

.. testcode::

   ctx = ray.data.DataContext.get_current()
   ctx.verbose_stats_logs = True

Many of the options in :class:`~ray.data.DataContext` are intended for advanced use cases or debugging,
and most users shouldn't need to modify them. However, some of the most important options are:

* `max_errored_blocks`: Max number of blocks that are allowed to have errors, unlimited if negative. This option allows application-level exceptions in block processing tasks. These exceptions may be caused by UDFs (for example, due to corrupted data samples) or IO errors. Data in the failed blocks are dropped. This option can be useful to prevent a long-running job from failing due to a small number of bad blocks. By default, no retries are allowed.
* `write_file_retry_on_errors`: A list of sub-strings of error messages that should trigger a retry when writing files. This is useful for handling transient errors when writing to remote storage systems. By default, retries on common transient AWS S3 errors.
* `verbose_stats_logs`: Whether stats logs should be verbose. This includes fields such as ``extra_metrics`` in the stats output, which are excluded by default. Off by default.
* `log_internal_stack_trace`: Whether to write the full Ray Data/Ray Core internal code stack frames to the Ray Data log file when logging a user-code error. These internal frames are always omitted from ``stdout``; by default they're also omitted from the log file. Set this to ``True`` to include them in the log file. Off by default.
* `raise_original_map_exception`: Whether to raise the original exception encountered in map UDF instead of wrapping it in a `UserCodeException`.

For more details on each of the preceding options, see :class:`~ray.data.DataContext`.

Job-level Checkpointing
-----------------------

Ray Data supports job-level checkpointing to improve fault tolerance for
long-running batch pipelines. When enabled, Ray Data can resume a failed job
by skipping rows that were successfully processed in a previous run, instead
of restarting from the beginning.

To configure job-level checkpointing, specify a
:class:`~ray.data.checkpoint.CheckpointConfig` on the current
:class:`~ray.data.DataContext`.

**Example configuration:**

.. code-block:: python

    import ray
    from ray.data.checkpoint import CheckpointConfig

    ctx = ray.data.DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path="s3://my-bucket/ray-data-checkpoints",  # Must be accessible by all nodes
        delete_checkpoint_on_success=False,  # Preserves checkpoints after successful runs
    ) 
