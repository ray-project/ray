---
myst:
  html_meta:
    description: "Inspect the Result object returned by trainer.fit: reported metrics, a dataframe of all metrics, saved checkpoints, and the storage location."
---

(train-inspect-results)=

# Inspect training results

`trainer.fit()` returns a {class}`~ray.train.Result` object.

Among other information, the {class}`~ray.train.Result` object contains the following:

- The last reported checkpoint, which you can use to load the model, and its attached metrics.
- Error messages, if any errors occurred.
- Any data that worker 0's training function returns.

## View metrics
You can retrieve reported metrics attached to a checkpoint from the {class}`~ray.train.Result` object.

Common metrics include the training or validation loss and prediction accuracy.

The metrics in the {class}`~ray.train.Result` object correspond to the metrics you pass as an argument to {func}`train.report <ray.train.report>` {ref}`in your training function <train-monitoring-and-logging>`.


:::{note}
Persisting free-floating metrics that you report through `ray.train.report(metrics, checkpoint=None)` is deprecated, and so is retrieving them from the {class}`~ray.train.Result` object. Ray Train persists only metrics attached to checkpoints. For details, see {ref}`train-metric-only-reporting-deprecation`.
:::


### Last reported metrics

Use {attr}`Result.metrics <ray.train.Result>` to retrieve the metrics attached to the last reported checkpoint.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_metrics_start__
:end-before: __result_metrics_end__
```

### Dataframe of all reported metrics

Use {attr}`Result.metrics_dataframe <ray.train.Result>` to retrieve a pandas DataFrame of all metrics reported alongside checkpoints.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_dataframe_start__
:end-before: __result_dataframe_end__
```

(returned-data-from-train-function)=

### Data returned from the training function

Use {attr}`Result.return_value <ray.train.Result>` to retrieve any data that worker 0's training function returns.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_return_value_start__
:end-before: __result_return_value_end__
```

## Retrieve checkpoints
You can retrieve checkpoints reported to Ray Train from the {class}`~ray.train.Result` object.

{ref}`Checkpoints <train-checkpointing>` contain all the information needed to restore the training state, which usually includes the trained model.

You can use checkpoints for common downstream tasks such as {doc}`offline batch inference with Ray Data </data/index>` or {doc}`online model serving with Ray Serve </serve/index>`.

The checkpoints in the {class}`~ray.train.Result` object correspond to the checkpoints you pass as an argument to {func}`train.report <ray.train.report>` {ref}`in your training function <train-monitoring-and-logging>`.

### Last saved checkpoint
Use {attr}`Result.checkpoint <ray.train.Result>` to retrieve the last checkpoint.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_checkpoint_start__
:end-before: __result_checkpoint_end__
```


### Other checkpoints
Sometimes you want an earlier checkpoint. For example, if your loss increases with more training because of overfitting, you might want to retrieve the checkpoint with the lowest loss.

Retrieve a list of all available checkpoints and their metrics with {attr}`Result.best_checkpoints <ray.train.Result>`.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_best_checkpoint_start__
:end-before: __result_best_checkpoint_end__
```

:::{seealso}
For more information on checkpointing, see {ref}`train-checkpointing`.
:::

(accessing-storage-location)=

## Access the storage location
To retrieve the results later, get the storage location of the training run with {attr}`Result.path <ray.train.Result>`.

This path corresponds to the {ref}`storage_path <train-log-dir>` you configured in the {class}`~ray.train.RunConfig`. It's a nested subdirectory of that path, usually of the form `TrainerName_date-string/TrainerName_id_00000_0_...`.

The result also contains a {class}`pyarrow.fs.FileSystem` that you can use to access the storage location. The file system is useful when the path is on cloud storage.


```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_path_start__
:end-before: __result_path_end__
```


Restore a result with {meth}`Result.from_path <ray.train.Result.from_path>`:

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_restore_start__
:end-before: __result_restore_end__
```


## Catch errors
If an error occurs during training, {attr}`Result.error <ray.train.Result>` contains the raised exception.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __result_error_start__
:end-before: __result_error_end__
```


(finding-results-on-persistent-storage)=

## Find results on persistent storage
Ray Train stores all training results, including reported metrics and checkpoints, on the configured {ref}`persistent storage <train-log-dir>`.

To configure this location for your training run, see {ref}`the persistent storage guide <train-log-dir>`.
