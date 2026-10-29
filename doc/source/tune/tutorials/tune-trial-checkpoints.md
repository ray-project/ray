---
myst:
  html_meta:
    description: "Save and load Tune trial checkpoints with the function and class APIs, including periodic checkpointing and checkpointing at termination."
---

(tune-trial-checkpoint)=

# How to save and load trial checkpoints

Trial checkpoints are one of {ref}`the three types of data stored by Tune <tune-persisted-experiment-data>`. You define their contents, and they're meant to snapshot your training progress.

You save trial-level checkpoints through the {ref}`trainable <tune-60-seconds>` API. The trainable is where you define your custom training logic, and it's also where you define which trial state to checkpoint. This guide shows how to save and load checkpoints with the function API and the class API, and walks you through the configuration options.

(tune-function-trainable-checkpointing)=

## Save and load checkpoints with the function API

With the Ray Tune function API, save and load checkpoints as the following example shows. To create a checkpoint, use the {meth}`~ray.tune.Checkpoint.from_directory` API.

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __function_api_checkpointing_from_dir_start__
:end-before: __function_api_checkpointing_from_dir_end__
```

The preceding code does the following:

- It saves a checkpoint with {meth}`tune.report(..., checkpoint=checkpoint) <ray.tune.report>`. You must report every checkpoint alongside a set of metrics, so that Tune can order checkpoints by a specified metric.
- During training iteration `epoch`, it saves the checkpoint to the path `<storage_path>/<exp_name>/<trial_name>/checkpoint_<epoch>` on the node where training happens. Depending on the {ref}`storage configuration <tune-storage-options>`, Tune can then sync the checkpoint to a consolidated storage location.
- It loads a checkpoint with {meth}`tune.get_checkpoint() <ray.tune.get_checkpoint>`. Whenever Tune restores a trial, this call returns the trial's latest checkpoint. Tune restores a trial when the trial is configured to retry after a failure, when the experiment is restored, or when the trial resumes after a pause, such as with {doc}`PBT </tune/examples/pbt-guide>`.

  % TODO: for (1), link to tune fault tolerance guide. For (2), link to tune restore guide.

:::{note}
`checkpoint_frequency` and `checkpoint_at_end` don't work with function API checkpointing. With a function trainable, you control checkpointing manually instead. For example, to checkpoint every three epochs, use the following code:

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __function_api_checkpointing_periodic_start__
:end-before: __function_api_checkpointing_periodic_end__
```
:::

For more information on creating checkpoints, see {class}`Checkpoint <ray.tune.Checkpoint>`.

(tune-class-trainable-checkpointing)=

## Save and load checkpoints with the class API

You can also implement checkpointing and restoring with the class API:

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __class_api_checkpointing_start__
:end-before: __class_api_checkpointing_end__
```

You can checkpoint manually, periodically, or at termination.

(tune-class-trainable-checkpointing_manual-checkpointing)=

### Checkpoint manually from the trainable

To trigger checkpointing manually from a custom `Trainable`, return `should_checkpoint: True`, or `tune.result.SHOULD_CHECKPOINT: True`, in the result dictionary of `step`. This can be especially helpful on spot instances:

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __class_api_manual_checkpointing_start__
:end-before: __class_api_manual_checkpointing_end__
```

In the preceding example, when `detect_instance_preemption` returns `True`, the trainable sets `should_checkpoint=True` in its result to trigger a manual checkpoint.

(tune-callback-checkpointing)=

### Checkpoint manually from a Tuner callback

As with {ref}`manual checkpointing from the trainable <tune-class-trainable-checkpointing_manual-checkpointing>`, you can trigger checkpointing from {class}`Tuner <ray.tune.Tuner>` {class}`Callback <ray.tune.Callback>` methods. In the {meth}`on_trial_result() <ray.tune.Callback.on_trial_result>` method of your custom callback, set the `result["should_checkpoint"] = True` flag, or `result[tune.result.SHOULD_CHECKPOINT] = True`. Unlike checkpointing in the class API, this approach decouples checkpointing logic from training logic. It also gives you access to all {class}`Trial <ray.tune.experiment.trial.Trial>` instances, so you can build more complex checkpointing strategies.

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __callback_api_checkpointing_start__
:end-before: __callback_api_checkpointing_end__
```

### Checkpoint periodically

To checkpoint trials every *N* iterations, set `checkpoint_frequency=N`, as in the following example:

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __class_api_periodic_checkpointing_start__
:end-before: __class_api_periodic_checkpointing_end__
```

### Checkpoint at termination

The `checkpoint_frequency` might not coincide with the exact end of a trial. To also create a checkpoint at the end of a trial, set `checkpoint_at_end=True`:

```{literalinclude} /tune/doc_code/trial_checkpoint.py
:language: python
:start-after: __class_api_end_checkpointing_start__
:end-before: __class_api_end_checkpointing_end__
```

## Configure checkpointing

Configure checkpointing with {class}`CheckpointConfig <ray.tune.CheckpointConfig>`. Some of its options don't apply to the function API, because you determine checkpointing frequency manually in your own training loop. The following compatibility matrix shows which options each API supports.

```{list-table}
:header-rows: 1

* -
  - Class API
  - Function API
* - `num_to_keep`
  - ✅
  - ✅
* - `checkpoint_score_attribute`
  - ✅
  - ✅
* - `checkpoint_score_order`
  - ✅
  - ✅
* - `checkpoint_frequency`
  - ✅
  - ❌
* - `checkpoint_at_end`
  - ✅
  - ❌
```

## Summary

This guide covered how to save and load trial checkpoints in Tune. After you enable checkpointing, see one of the following guides:

- {ref}`Extract checkpoints from Tune experiment results <tune-analysis-guide>`
- {ref}`Configure persistent storage options <tune-storage-options>` for a {ref}`distributed Tune experiment <tune-distributed-ref>`

(tune-persisted-experiment-data)=

## Appendix: Types of data stored by Tune

Tune stores three types of data, which the following sections describe.

### Experiment checkpoints

Experiment-level checkpoints save the experiment state, which includes the following:

- The state of the search algorithm.
- The list of trials and their statuses, such as `PENDING`, `RUNNING`, `TERMINATED`, and `ERROR`.
- Metadata for each trial, such as its hyperparameter configuration and some derived trial results, including the minimum, maximum, and last values.

The driver on the head node saves the experiment-level checkpoint periodically. By default, Tune adjusts the save frequency automatically so that it spends at most 5% of the time saving experiment checkpoints and the remaining time handling training results and scheduling. You can also adjust this time with the {ref}`TUNE_GLOBAL_CHECKPOINT_S environment variable <tune-env-vars>`.

### Trial checkpoints

Trial-level checkpoints capture the per-trial state, which often includes the model and optimizer states. Trial checkpoints have several uses, including the following:

- If a trial is interrupted for some reason, such as on a spot instance, it can resume from its last state, so no training time is lost.
- Some search algorithms and schedulers pause trials to free up resources for other trials to train in the meantime. Pausing only makes sense if the trials can then continue training from their latest state.
- You can use the checkpoint later for downstream tasks such as batch inference.

To learn how to save and load trial checkpoints, see {ref}`tune-trial-checkpoint`.

### Trial results

Tune saves and logs the metrics that trials report to their respective trial directories. It stores this data in CSV, JSON, or TensorBoard format. TensorBoard files match `events.out.tfevents.*`. You can inspect this data with TensorBoard and use it for post-experiment analysis.
