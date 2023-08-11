Inspecting Training Results
===========================

The return value of your :meth:`Trainer.fit() <ray.train.base_trainer.BaseTrainer.fit>`
call is a :class:`~ray.air.result.Result` object.

The :class:`~ray.air.result.Result` object contains, among others:

- The last reported metrics (e.g. the loss)
- The last reported checkpoint (to load the model)
- Error messages, if any errors occurred

Last reported metrics
---------------------

Use :attr:`Result.metrics <ray.air.result.Result.metrics>` to retrieve the
latest reported metrics.

This corresponds to the metrics you passed to :func:`train.report <ray.train.report>`
as an argument :ref:`in your training function <train-monitoring-and-logging>`.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_metrics_start__
    :end-before: __result_metrics_end__

Dataframe of all reported metrics
---------------------------------
Use :attr:`Result.metrics_dataframe <ray.air.result.Result.metrics_dataframe>` to retrieve
a pandas DataFrame of all reported metrics.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_dataframe_start__
    :end-before: __result_dataframe_end__


Last saved checkpoint
---------------------
Use :attr:`Result.checkpoint <ray.air.result.Result.checkpoint>` to retrieve the
last checkpoint.

This corresponds to the checkpoint you passed to :func:`train.report <ray.train.report>`
as an argument :ref:`in your training function <train-monitoring-and-logging>`.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_checkpoint_start__
    :end-before: __result_checkpoint_end__


Other checkpoints
-----------------
Sometimes you want to access an earlier checkpoint. For instance, if your loss increased
after more training due to overfitting, you may want to retrieve the checkpoint with
the lowest loss.

You can retrieve a list of all available checkpoints and their metrics with
:attr:`Result.best_checkpoints <ray.air.result.Result.best_checkpoints>`

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_best_checkpoint_start__
    :end-before: __result_best_checkpoint_end__

Storage location
----------------
If you need to retrieve the results later, you can inspect where they are stored
with :meth:`Result.path <ray.air.result.Result.path>`.

This path will correspond to the :ref:`storage_path <train-log-dir>` you configured
in the :class:`~ray.air.RunConfig`.


.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_path_start__
    :end-before: __result_path_end__


Errors
------
If an error occurred during training,
:meth:`Result.error <ray.air.result.Result.error>` will be set and contain the exception
that was raised.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_error_start__
    :end-before: __result_error_end__

