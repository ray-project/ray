---
myst:
  html_meta:
    description: "Use Ray Data lineage reconstruction to recover lost blocks when a node fails, without relying on Ray Core lineage reconstruction."
---

(data_fault_tolerance)=

# Recover lost blocks due to transient failures

Node failure or preemption can be a common occurrence during long training/data processing runs. When a node fails, the blocks whose primary copies were stored in that node’s share of the object store are lost. Ray Data blocks are objects in Ray Core’s view. By default, Ray Core recovers lost objects by detecting a node loss, finding all objects that were resident on that node, and then re-running the tasks that created them. To learn how that works, see {ref}`Object fault tolerance <fault-tolerance-objects>`. The shortfalls with this approach are that block reconstruction occurs outside Ray Data’s resource management and scheduling, which can lead to excessive spilling or even OOMs due to oversubscription of the cluster with reconstruction tasks, without accounting for resources properly or proper backpressure mechanisms.

From 2.61 onwards, Ray Data can reconstruct lost blocks itself instead of relying on Ray Core. This means that reconstruction work is properly accounted for, and task resubmissions are limited only to the chain of producers, or the ‘lineage’ of a lost block. When a task’s output is lost, Ray Data traces the lost block back to the tasks that read the source data, such as a read task or a `from_items` task. Then, Data re-runs the chain of tasks that produce the lost block, subject to the existing backpressure and resource accounting of Ray Data.

Ray Data lineage reconstruction is experimental and off by default. Its behavior and configuration may change in future releases.

For the design, see the [Ray Data lineage reconstruction REP](https://github.com/ray-project/enhancements/pull/74).

(data_fault_tolerance_enable)=

## Turn on lineage reconstruction

Ray Data reconstructs lost blocks when you turn off Ray Core lineage reconstruction for the job. To do that, pass a {class}`~ray.job_config.JobConfig` with `_disable_job_level_lineage_reconstruction=True` to {func}`ray.init`. Ray Data reads this setting from the job and turns on its own reconstruction automatically.

```{testcode}
:hide:

import ray

ray.shutdown()

```

```{testcode}
import ray
from ray.job_config import JobConfig

ray.init(job_config=JobConfig(_disable_job_level_lineage_reconstruction=True))

ds = ray.data.range(1000).map_batches(lambda batch: {"id": batch["id"] * 2})
for batch in ds.iter_batches():
    pass
```

To check that lineage reconstruction is on, read `enable_ray_data_reconstruction` on the current {class}`~ray.data.DataContext`:

```{testcode}
print(ray.data.DataContext.get_current().enable_ray_data_reconstruction)
```

```{testoutput}
True
```

Keep the following in mind when you turn on lineage reconstruction:
- **Lineage reconstruction can only be enabled through setting the Ray Job Config.** `DataContext.enable_ray_data_reconstruction` reflects the job setting. Don’t set it on the `DataContext`, because that setting doesn’t propagate to the cluster. If a value on the `DataContext` conflicts with the job config, Ray Data raises a `ValueError`.
- **Enabling Ray Data lineage reconstruction disables Ray Core lineage reconstruction automatically.** This setting stops the job’s workers from keeping the lineage of their objects, so Ray Core can’t reconstruct any object in the job. Ray Data is the only reconstruction mechanism. If a dataset can’t use Ray Data lineage reconstruction, a lost object fails that dataset. To learn which datasets are supported, see {ref}`data_fault_tolerance_supported`.
- **Aggregations in your script disable lineage reconstruction for that dataset.** Methods like {meth}`Dataset.count() <ray.data.Dataset.count>` and {meth}`Dataset.sum() <ray.data.Dataset.sum>` add operators that lineage reconstruction doesn’t support. To keep reconstruction on, consume the dataset with {meth}`~ray.data.Dataset.iter_batches`, {meth}`~ray.data.Dataset.take_all`, or a write.

(data_fault_tolerance_supported)=

## Supported datasets

Ray Data lineage reconstruction supports the following:

- Datasets made only of reads and map transformations, such as {meth}`~ray.data.Dataset.map`, {meth}`~ray.data.Dataset.map_batches`, {meth}`~ray.data.Dataset.filter`, and {meth}`~ray.data.Dataset.flat_map`. This includes transformations that run on tasks and on actor pools.
- Multiple lost blocks in the same dataset.
- Consuming the dataset with {meth}`~ray.data.Dataset.iter_batches` or {meth}`~ray.data.Dataset.take_all`.

Ray Data turns off lineage reconstruction for the whole dataset if the dataset uses any of the following:

- Operations other than map transformations, such as {meth}`~ray.data.Dataset.limit`, {meth}`~ray.data.Dataset.union`, {meth}`~ray.data.Dataset.count` and other aggregations, {meth}`~ray.data.Dataset.sort`, and all-to-all shuffles.
- Strict streaming repartition, `repartition(target_num_rows_per_block=N, strict=True)`.
- `DataContext.execution_options.preserve_order = True`.

## Reconstruction failures

If Ray Data detects a lost block but can't start reconstructing it (for example, because the task that produced the block isn't tracked in the lineage graph, or Ray Data no longer holds the input of the read task that the block traces back to), Ray Data raises a `ray.data.exceptions.LineageReconstructionError`. The error message includes both why the block was lost and why reconstruction failed.

`LineageReconstructionError` is a subclass of {class}`~ray.exceptions.ObjectLostError`, so code that already catches `ObjectLostError` also catches it.

Ray Data counts a `LineageReconstructionError` as a failed block. By default, a failed block fails the dataset. To drop the lost block and continue instead, set `max_errored_blocks` on the {class}`~ray.data.DataContext`. To learn more, see {ref}`execution_configurations`.

## Troubleshooting

Ray Data writes lineage reconstruction logs to `ray-data.log`. To learn where to find this file, see {ref}`Ray Data logs <ray-data-logs>`.

### Follow a reconstruction

Ray Data logs each reconstruction step at the `INFO` level with the prefix `[lineage-reconstruction]`. Search for this prefix to see when Ray Data detects a lost block, which read tasks it re-runs, and on which operators.

### Inspect the task graph

Ray Data logs the task graph that it uses for reconstruction at the `DEBUG` level. For every task, the log records the task ID and the tasks whose output blocks it depends on. When a block is lost, the log also records the reconstruction plan, which is the chain of tasks from the lost block back to its read tasks. Use these lines to check which tasks Ray Data re-runs to rebuild a lost block.

## See also

- {ref}`Job-level checkpointing <job-level-checkpointing>`: Resume a failed job by skipping rows that a previous run processed.
- {ref}`Object fault tolerance <fault-tolerance-objects>`: How Ray Core reconstructs lost objects.
