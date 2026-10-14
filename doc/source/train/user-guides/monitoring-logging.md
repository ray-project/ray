---
myst:
  html_meta:
    description: "Obtain and aggregate training metrics reported from multiple Ray Train workers."
---

(train-monitoring-and-logging)=

# Monitor and log metrics

To attach metrics to {ref}`checkpoints <train-checkpointing>` from the training function, call {func}`ray.train.report(metrics, checkpoint) <ray.train.report>`. Ray Train collects the results from the distributed workers and passes them to the Ray Train driver process for bookkeeping.

Reporting has two primary use cases:

* Metrics, such as accuracy and loss, at the end of each training epoch. See {ref}`train-dl-saving-checkpoints` for usage examples.
* Validating checkpoints on a validation set with a validation function that you define. See {ref}`train-validating-checkpoints` for usage examples.

Ray Train attaches only the result that the rank 0 worker reports to the checkpoint. However, `train.report()` acts as a barrier to ensure consistency, so you must call it on each worker. To aggregate results from multiple workers, see {ref}`train-aggregating-results`.


(train-aggregating-results)=

(how-to-obtain-and-aggregate-results-from-different-workers)=

## Obtain and aggregate results from different workers

In real applications, you might need optimization metrics beyond accuracy and loss, such as recall, precision, and F-beta score. You might also need metrics from multiple workers. Ray Train currently reports metrics only from the rank 0 worker. To report metrics from multiple workers, use third-party libraries or the distributed primitives of your machine learning framework.


::::{tab-set}
:::{tab-item} Native PyTorch
Ray Train natively supports [TorchMetrics](https://torchmetrics.readthedocs.io/en/latest/), which provides machine learning metrics for distributed, scalable PyTorch models.

The following example reports both the aggregated R2 score and the mean training and validation loss from all workers:

```{literalinclude} ../doc_code/metric_logging.py
:language: python
:start-after: __torchmetrics_start__
:end-before: __torchmetrics_end__
```
:::
::::


(train-metric-only-reporting-deprecation)=

## Deprecated: Reporting free-floating metrics

Reporting metrics with `ray.train.report(metrics, checkpoint=None)` from every worker writes the metrics to the Ray Tune log files `progress.csv` and `result.json`. Access them through `Result.metrics_dataframe` on the {class}`~ray.train.Result` that `trainer.fit()` returns.

As of Ray 2.43, this behavior is deprecated. Ray Train V2, an overhaul of Ray Train's implementation and select APIs, doesn't support it.

Ray Train V2 keeps only the slim set of experiment tracking features that fault tolerance needs, so it doesn't support reporting free-floating metrics that aren't attached to checkpoints. To track metrics, report them directly from the workers to experiment tracking tools such as MLflow and W&B. See {ref}`train-experiment-tracking-native` for examples.

In Ray Train V2, reporting only metrics from all workers is a no-op. However, you can still access the results that all workers report and use them to implement custom metric-handling logic, as the following example shows:

```{literalinclude} ../doc_code/metric_logging.py
:language: python
:start-after: __report_callback_start__
:end-before: __report_callback_end__
```


To use Ray Tune {class}`Callbacks <ray.tune.Callback>` that depend on free-floating metrics that workers report, {ref}`run Ray Train as a single Ray Tune trial <train-with-tune-callbacks>`.

For details, see the following resources:

* [Train V2 REP](https://github.com/ray-project/enhancements/blob/main/reps/2024-10-18-train-tune-api-revamp/2024-10-18-train-tune-api-revamp.md): The Ray Enhancement Proposal with technical details about the API changes in Train V2.
* [Train V2 migration guide](https://github.com/ray-project/ray/issues/49454): The full migration guide for Train V2.
