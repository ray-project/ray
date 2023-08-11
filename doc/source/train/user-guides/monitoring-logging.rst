.. _train-monitoring-and-logging:

Monitoring and Logging
======================

Ray Train provides an API for reporting intermediate
results and checkpoints from the training function (run on distributed workers) up to the
``Trainer`` (where your python script is executed) by calling ``train.report(metrics)``.
The results will be collected from the distributed workers and passed to the driver to
be logged and displayed.

.. warning::

    Only the results from rank 0 worker will be used. However, in order to ensure
    consistency, ``train.report()`` has to be called on each worker. If you
    want to aggregate results from multiple workers, see :ref:`train-aggregating-results`.

The primary use-case for reporting is for metrics (accuracy, loss, etc.) at
the end of each training epoch.

.. tab-set::

    .. tab-item:: PyTorch

        .. code-block:: python

            from ray import train

            def train_func():
                ...
                for i in range(num_epochs):
                    result = model.train(...)
                    train.report({"result": result})

    .. tab-item:: PyTorch Lightning

        In PyTorch Lightning, we use a callback to call ``train.report()``.

        .. code-block:: python

            from ray import train
            import pytorch_lightning as pl
            from pytorch_lightning.callbacks import Callback

            class MyRayTrainReportCallback(Callback):
                def on_train_epoch_end(self, trainer, pl_module):
                    metrics = trainer.callback_metrics
                    metrics = {k: v.item() for k, v in metrics.items()}

                    train.report(metrics=metrics)

            def train_func_per_worker():
                ...
                trainer = pl.Trainer(
                    # ...
                    callbacks=[MyRayTrainReportCallback()]
                )
                trainer.fit()

The session concept exists on several levels: The execution layer (called `Tune Session`) and the Data Parallel training layer
(called `Train Session`).
The following figure shows how these two sessions look like in a Data Parallel training scenario.

.. image:: ../../ray-air/images/session.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1g0pv8gqgG29aPEPTcd4BC0LaRNbW1sAkv3H6W1TCp0c/edit



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

        .. literalinclude:: ../doc_code/torchmetrics_example.py
            :language: python
            :start-after: __start__
