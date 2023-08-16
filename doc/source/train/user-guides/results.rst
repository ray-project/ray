Inspecting Training Results
===========================

The return value of your :meth:`Trainer.fit() <ray.train.trainer.BaseTrainer.fit>`
call is a :class:`~ray.air.result.Result` object.

The :class:`~ray.air.result.Result` object contains, among other information:

- The last reported metrics (e.g. the loss)
- The last reported checkpoint (to load the model)
- Error messages, if any errors occurred

Viewing metrics
---------------
You can retrieve metrics reported to Ray Train from the :class:`~ray.air.result.Result`
object.

Common metrics include the training or validation loss, or prediction accuracies.

The metrics retrieved from the :class:`~ray.air.result.Result` object
correspond to those you passed to :func:`train.report <ray.train.report>`
as an argument :ref:`in your training function <train-monitoring-and-logging>`.


Last reported metrics
~~~~~~~~~~~~~~~~~~~~~

Use :attr:`Result.metrics <ray.air.Result.metrics>` to retrieve the
latest reported metrics.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_metrics_start__
    :end-before: __result_metrics_end__

Dataframe of all reported metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :attr:`Result.metrics_dataframe <ray.air.Result.metrics_dataframe>` to retrieve
a pandas DataFrame of all reported metrics.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_dataframe_start__
    :end-before: __result_dataframe_end__


Retrieving checkpoints
----------------------
You can retrieve checkpoints reported to Ray Train from the :class:`~ray.air.result.Result`
object.

:ref:`Checkpoints <train-checkpointing>` contain all the information that is needed
to restore the training state. This usually includes the trained model.

You can use checkpoints for common downstream tasks such as
:ref:`offline batch inference with Ray Data <batch_inference_ray_train>`,
or :doc:`online model serving with Ray Serve </serve/index>`.

The checkpoints retrieved from the :class:`~ray.air.result.Result` object
correspond to those you passed to :func:`train.report <ray.train.report>`
as an argument :ref:`in your training function <train-monitoring-and-logging>`.

Last saved checkpoint
~~~~~~~~~~~~~~~~~~~~~
Use :attr:`Result.checkpoint <ray.air.Result.checkpoint>` to retrieve the
last checkpoint.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_checkpoint_start__
    :end-before: __result_checkpoint_end__


Other checkpoints
~~~~~~~~~~~~~~~~~
Sometimes you want to access an earlier checkpoint. For instance, if your loss increased
after more training due to overfitting, you may want to retrieve the checkpoint with
the lowest loss.

You can retrieve a list of all available checkpoints and their metrics with
:attr:`Result.best_checkpoints <ray.air.Result.best_checkpoints>`

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_best_checkpoint_start__
    :end-before: __result_best_checkpoint_end__

Accessing storage location
---------------------------
If you need to retrieve the results later, you can get the storage location
with :attr:`Result.path <ray.air.Result.path>`.

This path will correspond to the :ref:`storage_path <train-log-dir>` you configured
in the :class:`~ray.air.RunConfig`.


.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_path_start__
    :end-before: __result_path_end__


Viewing Errors
--------------
If an error occurred during training,
:attr:`Result.error <ray.air.Result.error>` will be set and contain the exception
that was raised.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_error_start__
    :end-before: __result_error_end__

