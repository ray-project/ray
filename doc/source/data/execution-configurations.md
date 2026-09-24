---
myst:
  html_meta:
    description: "Configuration options that control Ray Data execution on top of the Ray Core cluster, including job-level checkpointing."
---

(execution_configurations)=

# Execution configurations

Ray Data provides configuration options that control how a {class}`~ray.data.Dataset` executes, in addition to the configuration of the Ray Core cluster itself.

You configure Ray Data primarily through {class}`~ray.data.ExecutionOptions` or {class}`~ray.data.DataContext`.

This guide describes the most important of these options and when to use them.

(configuring-executionoptions)=

## Configure {class}`~ray.data.ExecutionOptions`

Use the {class}`~ray.data.ExecutionOptions` class to configure options for Dataset execution. To use it, modify the attributes in the current {class}`~ray.data.DataContext` object's `execution_options`, as the following example shows:

```{testcode}
:hide:

import ray
```

```{testcode}
ctx = ray.data.DataContext.get_current()
ctx.execution_options.verbose_progress = True
```

The following are some of the most important options:

* `resource_limits`: Set a soft limit on resource usage during execution. For example, if other parts of your code require a minimum amount of resources, you might want to limit the resources that Ray Data uses. Auto-detected by default.
* `exclude_resources`: Deprecated. Use `label_selector` to constrain Ray Data work to labeled nodes.
* `preserve_order`: Set this to preserve the ordering between blocks processed by operators under the streaming executor. Off by default.
* `actor_locality_enabled`: Deprecated. Ray Data manages actor locality internally.
* `verbose_progress`: Whether to report progress individually per operator. When off, Ray Data reports only global progress and progress for AllToAll operators. Use this option for performance debugging. On by default.

For more details on each of the preceding options, see {class}`~ray.data.ExecutionOptions`.

(configuring-datacontext)=

## Configure {class}`~ray.data.DataContext`

Use the {class}`~ray.data.DataContext` class to configure more general Ray Data options, such as observability and logging, error handling and retry behavior, and internal data formats. To use it, modify the attributes in the current {class}`~ray.data.DataContext` object, as the following example shows:

```{testcode}
:hide:

import ray
```

```{testcode}
ctx = ray.data.DataContext.get_current()
ctx.verbose_stats_logs = True
```

Many {class}`~ray.data.DataContext` options are for advanced use cases or debugging, and you usually don't need to modify them. The following are some of the most important options:

* `max_errored_blocks`: The maximum number of blocks that can have errors. A negative value means no limit. Set this option to tolerate application-level exceptions in block processing tasks. UDFs can raise these exceptions, for example on corrupted data samples, and IO errors can also cause them. Ray Data drops the data in the failed blocks. Use this option to keep a long-running job from failing because of a small number of bad blocks. By default, Ray Data tolerates no blocks with errors.
* `write_file_retry_on_errors`: A list of error message fragments that trigger a retry when writing files. Use it to handle transient errors when writing to remote storage systems. By default, Ray Data retries on common transient AWS S3 errors.
* `verbose_stats_logs`: Whether stats logs are verbose. Verbose logs include fields such as `extra_metrics` in the stats output, which are otherwise excluded. Off by default.
* `log_internal_stack_trace`: Whether to write the full internal stack frames from Ray Data and Ray Core to the Ray Data log file when logging a user-code error. Ray Data always omits these internal frames from `stdout`, and by default also omits them from the log file. Set this to `True` to include them in the log file. Off by default.
* `raise_original_map_exception`: Whether to raise the original exception from a map UDF instead of wrapping it in a `UserCodeException`.

For more details on each of the preceding options, see {class}`~ray.data.DataContext`.

(job-level-checkpointing)=

### Configure job-level checkpointing

Ray Data supports job-level checkpointing to improve fault tolerance for long-running batch pipelines. When you enable it, Ray Data can resume a failed job by skipping rows that a previous run processed successfully, instead of restarting from the beginning.

To configure job-level checkpointing, specify a {class}`~ray.data.checkpoint.CheckpointConfig` on the current {class}`~ray.data.DataContext`.

The following example configures job-level checkpointing:

```python
import ray
from ray.data.checkpoint import CheckpointConfig

ctx = ray.data.DataContext.get_current()
ctx.checkpoint_config = CheckpointConfig(
    id_column="id",
    checkpoint_path="s3://my-bucket/ray-data-checkpoints",  # Must be accessible by all nodes
    delete_checkpoint_on_success=False,  # Preserves checkpoints after successful runs
)
```
