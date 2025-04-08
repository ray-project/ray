.. _train-monitoring-and-logging:

Monitoring and Logging Metrics
==============================

Ray Train provides an API for attaching metrics to :ref:`checkpoints <train-checkpointing>` from the training function by calling :func:`ray.train.report(metrics, checkpoint) <ray.train.report>`.
The results will be collected from the distributed workers and passed to the Ray Train driver process for book-keeping.

The primary use-case for reporting is for metrics (accuracy, loss, etc.) at the end of each training epoch. See :ref:`train-dl-saving-checkpoints` for usage examples.

Only the result reported by the rank 0 worker will be attached to the checkpoint.
However, in order to ensure consistency, ``train.report()`` acts as a barrier and must be called on each worker.
To aggregate results from multiple workers, see :ref:`train-aggregating-results`.


.. _train-aggregating-results:

How to obtain and aggregate results from different workers?
-----------------------------------------------------------

In real applications, you may want to calculate optimization metrics besides accuracy and loss: recall, precision, Fbeta, etc.
You may also want to collect metrics from multiple workers. While Ray Train currently only reports metrics from the rank 0
worker, you can use third-party libraries or distributed primitives of your machine learning framework to report
metrics from multiple workers.


.. tab-set::

    .. tab-item:: Native PyTorch

        Ray Train natively supports `TorchMetrics <https://torchmetrics.readthedocs.io/en/latest/>`_, which provides a collection of machine learning metrics for distributed, scalable PyTorch models.

        Here is an example of reporting both the aggregated R2 score and mean train and validation loss from all workers.

        .. literalinclude:: ../doc_code/metric_logging.py
            :language: python
            :start-after: __torchmetrics_start__
            :end-before: __torchmetrics_end__


.. _train-metric-only-reporting-deprecation:

(Deprecated) Reporting free-floating metrics
--------------------------------------------

Reporting metrics with ``ray.train.report(metrics, checkpoint=None)`` from every worker writes the metrics to a Ray Tune log file (``progress.csv``, ``result.json``)
and is accessible via the ``Result.metrics_dataframe`` on the :class:`~ray.train.Result` returned by ``trainer.fit()``.

As of Ray 2.43, this behavior is deprecated and will not be supported in Ray Train V2,
which is an overhaul of Ray Train's implementation and select APIs.

Ray Train V2 only keeps a slim set of experiment tracking features that are necessary for fault tolerance, so it does not support reporting free-floating metrics that are not attached to checkpoints.
The recommendation for metric tracking is to report metrics directly from the workers to experiment tracking tools such as MLFlow and WandB.
See :ref:`train-experiment-tracking-native` for examples.

In Ray Train V2, reporting only metrics from all workers is a no-op. However, it is still possible to access the results reported by all workers to implement custom metric-handling logic.

.. literalinclude:: ../doc_code/metric_logging.py
    :language: python
    :start-after: __report_callback_start__
    :end-before: __report_callback_end__


To use Ray Tune :class:`Callbacks <ray.tune.Callback>` that depend on free-floating metrics reported by workers, :ref:`run Ray Train as a single Ray Tune trial. <train-with-tune-callbacks>`

See the following resources for more information:

* `Train V2 REP <https://github.com/ray-project/enhancements/blob/main/reps/2024-10-18-train-tune-api-revamp/2024-10-18-train-tune-api-revamp.md>`_: Technical details about the API changes in Train V2
* `Train V2 Migration Guide <https://github.com/ray-project/ray/issues/49454>`_: Full migration guide for Train V2
