---
myst:
  html_meta:
    description: "Inspect the Result object returned by trainer.fit: saved checkpoints, the metrics attached to them, the storage location, and training errors."
---

(train-inspect-results)=

# Inspect training results

`trainer.fit()` returns a {class}`~ray.train.Result` object.

Among other information, the {class}`~ray.train.Result` object contains the following:

- The checkpoints that you save with {func}`ray.train.report <ray.train.report>` {ref}`in your training function <train-monitoring-and-logging>`, and the metrics attached to each one.
- The storage location of the training run.
- Any data that worker 0's training function returns.

## Retrieve checkpoints

{ref}`Checkpoints <train-checkpointing>` contain all the information needed to restore the training state, which usually includes the trained model.

You can use checkpoints for common downstream tasks such as {doc}`offline batch inference with Ray Data </data/index>` or {doc}`online model serving with Ray Serve </serve/index>`.

### Last saved checkpoint

Use {attr}`Result.checkpoint <ray.train.Result>` to retrieve the last checkpoint.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_checkpoint_start__
:end-before: __result_checkpoint_end__
```

### Best checkpoint

Sometimes you want an earlier checkpoint. For example, if your loss increases with more training because of overfitting, you might want to retrieve the checkpoint with the lowest loss.

Use {meth}`Result.get_best_checkpoint <ray.train.Result.get_best_checkpoint>` to retrieve the checkpoint that ranks best by a metric that you attached to it. This method is an alpha API.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_get_best_checkpoint_start__
:end-before: __result_get_best_checkpoint_end__
```

### All saved checkpoints

Use {attr}`Result.best_checkpoints <ray.train.Result>` to retrieve a list of all saved checkpoints and their metrics. Ray Train keeps the checkpoints that the {ref}`checkpoint configuration <train-dl-configure-checkpoints>` selects. By default, it keeps all of them.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_best_checkpoint_start__
:end-before: __result_best_checkpoint_end__
```

## View the metrics attached to checkpoints

The {class}`~ray.train.Result` object contains the metrics that you attach to checkpoints when you call {func}`ray.train.report <ray.train.report>` {ref}`in your training function <train-monitoring-and-logging>`. They typically include the metric that you rank checkpoints by, such as the validation loss or accuracy. For a full history of metrics over the course of training, use an {ref}`experiment tracking tool <train-experiment-tracking-native>`.

:::{note}
Persisting free-floating metrics that you report through `ray.train.report(metrics, checkpoint=None)` is deprecated, and so is retrieving them from the {class}`~ray.train.Result` object. Ray Train persists only metrics attached to checkpoints. For details, see {ref}`train-metric-only-reporting-deprecation`.
:::

### Metrics of the last checkpoint

Use {attr}`Result.metrics <ray.train.Result>` to retrieve the metrics attached to the last saved checkpoint.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_metrics_start__
:end-before: __result_metrics_end__
```

### DataFrame of all checkpoint metrics

Use {attr}`Result.metrics_dataframe <ray.train.Result>` to retrieve a pandas DataFrame of the metrics attached to every saved checkpoint. Each row corresponds to a checkpoint.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_dataframe_start__
:end-before: __result_dataframe_end__
```

(returned-data-from-train-function)=

## Retrieve data returned from the training function

Use {attr}`Result.return_value <ray.train.Result>` to retrieve any data that worker 0's training function returns.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_return_value_start__
:end-before: __result_return_value_end__
```

(accessing-storage-location)=

## Access the storage location

Ray Train stores checkpoints and the metrics attached to them on the {ref}`persistent storage <train-log-dir>` that you configure with the `storage_path` argument of {class}`~ray.train.RunConfig`.

Use {attr}`Result.path <ray.train.Result>` to retrieve the storage location of the training run. It's a subdirectory of the `storage_path`, named after the `name` argument of the `RunConfig`. If you don't set `name`, Ray Train uses `ray_train_run-<date>`.

The result also contains a {class}`pyarrow.fs.FileSystem` that you can use to access the storage location. The file system is useful when the path is on cloud storage.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_path_start__
:end-before: __result_path_end__
```

### Restore a result from storage

To retrieve the checkpoints and metrics of a training run later, restore the result with {meth}`Result.from_path <ray.train.Result.from_path>`:

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_restore_start__
:end-before: __result_restore_end__
```

## Catch errors

If training fails, `trainer.fit()` raises a {class}`~ray.train.TrainingFailedError` instead of returning a {class}`~ray.train.Result` object. When a worker fails, the error is a {class}`~ray.train.WorkerGroupError`. Its `worker_failures` attribute maps each failed worker's rank to the exception that the worker raised.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_error_start__
:end-before: __result_error_end__
```

The checkpoints that you saved before the failure remain on persistent storage. Because `trainer.fit()` doesn't return a `Result`, build the storage location from the `storage_path` and `name` arguments of your `RunConfig`, `<storage_path>/<name>`, and pass it to {meth}`Result.from_path <ray.train.Result.from_path>`. To make the location predictable, set `name` explicitly.
