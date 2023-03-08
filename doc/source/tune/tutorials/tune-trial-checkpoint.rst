.. _tune-trial-checkpoint:

Trial Checkpoint Saving and loading
===================================

Tune supports both :ref:`class trainable and functional trainable APIs <tune_60_seconds_trainables>`.
We will go through how trial checkpoint works separately for each of the sections below.

.. _tune-function-trainable-checkpointing:

Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

For function API, one can save and load checkpoints in Ray Tune in the following manner:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_checkpointing_start__
    :end-before: __function_api_checkpointing_end__

.. note:: ``checkpoint_frequency`` and ``checkpoint_at_end`` will not work with Function API checkpointing.

In this example, checkpoints will be saved by training iteration to ``<local_dir>/<exp_name>/trial_name/checkpoint_<step>``.

Tune also may copy or move checkpoints during the course of tuning. For this purpose,
it is important not to depend on absolute paths in the implementation of ``save``.

See :ref:`here for more information on creating checkpoints <air-checkpoint-ref>`.
If using framework-specific trainers from Ray AIR, see :ref:`here <air-trainer-ref>` for
references to framework-specific checkpoints such as `TensorflowCheckpoint`.


.. _tune-class-trainable-checkpointing:

Class API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~

You can also implement checkpoint/restore using the Trainable Class API:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_checkpointing_start__
    :end-before: __class_api_checkpointing_end__

You can checkpoint with three different mechanisms: manually, periodically, and at termination.

**Manual Checkpointing**: A custom Trainable can manually trigger checkpointing by returning ``should_checkpoint: True``
(or ``tune.result.SHOULD_CHECKPOINT: True``) in the result dictionary of `step`.
This can be especially helpful in spot instances:

.. code-block:: python

    def step(self):
        # training code
        result = {"mean_accuracy": accuracy}
        if detect_instance_preemption():
            result.update(should_checkpoint=True)
        return result


**Periodic Checkpointing**: periodic checkpointing can be used to provide fault-tolerance for experiments.
This can be enabled by setting ``checkpoint_frequency=<int>`` and ``max_failures=<int>`` to checkpoint trials
every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(checkpoint_frequency=10),
            failure_config=air.FailureConfig(max_failures=5))
    )
    results = tuner.fit()

**Checkpointing at Termination**: The checkpoint_frequency may not coincide with the exact end of an experiment.
If you want a checkpoint to be created at the end of a trial, you can additionally set the ``checkpoint_at_end=True``:

.. code-block:: python
   :emphasize-lines: 5

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(checkpoint_frequency=10, checkpoint_at_end=True),
            failure_config=air.FailureConfig(max_failures=5))
    )
    results = tuner.fit()


Use ``validate_save_restore`` to catch ``save_checkpoint``/``load_checkpoint`` errors before execution.

.. code-block:: python

    from ray.tune.utils import validate_save_restore

    # both of these should return
    validate_save_restore(MyTrainableClass)
    validate_save_restore(MyTrainableClass, use_object_store=True)

Configurations
==============
Checkpointing can be configured through :class:`CheckpointConfig <ray.air.config.CheckpointConfig>`.
Some of the options may not apply to Function Trainable. See table below.

.. list-table::
   :header-rows: 1

   * -
     - Function API
     - Class API
   * - ``num_to_keep``
     - |:heavy_check_mark:|
     - |:heavy_check_mark:|
   * - ``checkpoint_score_attribute``
     - |:heavy_check_mark:|
     - |:heavy_check_mark:|
   * - ``checkpoint_score_order``
     - |:heavy_check_mark:|
     - |:heavy_check_mark:|
   * - ``checkpoint_frequency``
     - |:heavy_multiplication_x:|
     - |:heavy_check_mark:|
   * - ``checkpoint_at_end``
     - |:heavy_multiplication_x:|
     - |:heavy_check_mark:|



See also
========
:ref:`Storage configuration guide <tune-storage-options>`
