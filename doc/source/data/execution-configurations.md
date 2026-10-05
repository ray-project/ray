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
ctx.execution_options.preserve_order = True
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
* `retried_io_errors`: A list of error message patterns that trigger a retry when reading or writing files. Ray Data matches each pattern first as literal text and then as a regular expression. Use it to handle transient errors from remote storage systems. By default, Ray Data retries on common transient AWS S3 errors. This option replaces the deprecated `write_file_retry_on_errors`.
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

#### Checkpoint without an ID column

Checkpointing needs a way to identify each input row. If your dataset has a unique ID column, pass it as `id_column`. If it doesn't, set `generated_id_column` instead, and Ray Data generates an ID for every row from where it lives in its Parquet file: the file, the row group, and the row's position in that row group.

```python
ctx.checkpoint_config = CheckpointConfig(
    generated_id_column="row_id",
    checkpoint_path="s3://my-bucket/ray-data-checkpoints",
)
```

When a job resumes, Ray Data skips committed work before reading it. It doesn't list files whose rows are all committed, doesn't read row groups whose rows are all committed, and processes only the uncommitted rows of a partly committed row group.

Keep the following in mind when you use generated row IDs:

- Ray Data generates row IDs only for Parquet inputs that `ray.data.read_parquet` reads on the V2 datasource path, which is the default. For other inputs, running the dataset raises an error. Use `id_column` for them instead.
- Checkpointing suits pipelines where each input row produces one output row, such as `map`-style batch inference that ends in a file write. Ray Data processes each input row at least once. It doesn't checkpoint join or aggregation state.
- Run the same pipeline when you resume, including any filters. Filters that Ray Data pushes into the read decide the position each row gets.
- Ray Data writes the generated ID column to the output. If you select columns, keep the generated column, or the write fails.
