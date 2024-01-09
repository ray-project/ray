.. _train-inspect-results:

Inspecting Training Results
===========================

The return value of your :meth:`Trainer.fit() <ray.train.trainer.BaseTrainer.fit>`
call is a :class:`~ray.train.Result` object.

The :class:`~ray.train.Result` object contains, among other information:

- The last reported metrics (e.g. the loss)
- The last reported checkpoint (to load the model)
- Error messages, if any errors occurred

Viewing metrics
---------------
You can retrieve metrics reported to Ray Train from the :class:`~ray.train.Result`
object.

Common metrics include the training or validation loss, or prediction accuracies.

The metrics retrieved from the :class:`~ray.train.Result` object
correspond to those you passed to :func:`train.report <ray.train.report>`
as an argument :ref:`in your training function <train-monitoring-and-logging>`.


Last reported metrics
~~~~~~~~~~~~~~~~~~~~~

Use :attr:`Result.metrics <ray.train.Result.metrics>` to retrieve the
latest reported metrics.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_metrics_start__
    :end-before: __result_metrics_end__

Dataframe of all reported metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :attr:`Result.metrics_dataframe <ray.train.Result.metrics_dataframe>` to retrieve
a pandas DataFrame of all reported metrics.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_dataframe_start__
    :end-before: __result_dataframe_end__


Retrieving checkpoints
----------------------
You can retrieve checkpoints reported to Ray Train from the :class:`~ray.train.Result`
object.

:ref:`Checkpoints <train-checkpointing>` contain all the information that is needed
to restore the training state. This usually includes the trained model.

You can use checkpoints for common downstream tasks such as
:ref:`offline batch inference with Ray Data <batch_inference_ray_train>`,
or :doc:`online model serving with Ray Serve </serve/index>`.

The checkpoints retrieved from the :class:`~ray.train.Result` object
correspond to those you passed to :func:`train.report <ray.train.report>`
as an argument :ref:`in your training function <train-monitoring-and-logging>`.

Last saved checkpoint
~~~~~~~~~~~~~~~~~~~~~
Use :attr:`Result.checkpoint <ray.train.Result.checkpoint>` to retrieve the
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
:attr:`Result.best_checkpoints <ray.train.Result.best_checkpoints>`

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_best_checkpoint_start__
    :end-before: __result_best_checkpoint_end__

.. seealso::

    See :ref:`train-checkpointing` for more information on checkpointing.

Accessing storage location
---------------------------
If you need to retrieve the results later, you can get the storage location
of the training run with :attr:`Result.path <ray.train.Result.path>`.

This path will correspond to the :ref:`storage_path <train-log-dir>` you configured
in the :class:`~ray.train.RunConfig`. It will be a
(nested) subdirectory within that path, usually
of the form `TrainerName_date-string/TrainerName_id_00000_0_...`.

The result also contains a :class:`pyarrow.fs.FileSystem` that can be used to
access the storage location, which is useful if the path is on cloud storage.


.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_path_start__
    :end-before: __result_path_end__


You can restore a result with :meth:`Result.from_path <ray.train.Result.from_path>`:

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_restore_start__
    :end-before: __result_restore_end__



Viewing Errors
--------------
If an error occurred during training,
:attr:`Result.error <ray.train.Result.error>` will be set and contain the exception
that was raised.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __result_error_start__
    :end-before: __result_error_end__


Finding results on persistent storage
-------------------------------------
All training results, including reported metrics, checkpoints, and error files,
are stored on the configured :ref:`persistent storage <train-log-dir>`.

See :ref:`our persistent storage guide <train-log-dir>` to configure this location
for your training run.
