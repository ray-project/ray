.. _tune-trial-checkpoint:

Trial checkpoint is one of :ref:`the three types of data stored by Tune <tune-persisted-experiment-data>`.

When training models with Tune, you can snapshot training progress with **checkpoints**,
which often includes the model and optimizer states of each trial. At the end of the experiment,
you can then use the checkpoint that corresponds with the best hyperparameter configuration.

Here are a few other uses of trial checkpoints:
- If the trial is interrupted for some reason (e.g., on spot instances), it can be resumed from the latest state
  -- no training time is lost.
- Some searchers or schedulers pause trials to free up resources for other trials to train.
  This only makes sense if the trials checkpoint before pausing, and then continue training from their latest state.
- The checkpoint is an output of the Tune experiment that can be used for other downstream
  tasks like finetuning on a different task, batch inference, and serving.

Trial-level checkpoints are saved via the [Tune Trainable](tune-60-seconds-trainables) API.
In this guide, we will show how to save and load checkpoints for Tune's Function Trainable and Class Trainable APIs,
as well as walk you through configuration options.

How to Save and Load Trial Checkpoint
=====================================

.. _tune-function-trainable-checkpointing:

Function API Checkpointing
--------------------------

If using Ray Tune's Function API, one can save and load checkpoints in the following manner:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_checkpointing_start__
    :end-before: __function_api_checkpointing_end__

In this example, a checkpoint saved during training iteration `step` is saved to a path of
``<local_dir>/<exp_name>/<trial_name>/checkpoint_<step>`` on the node on which training happens
and will be synced to either head node or cloud storage depending on
:ref:`storage option configured <tune-storage-options>`.

In the above code snippet:
- We implement *checkpoint saving* with ``session.report(..., checkpoint=checkpoint)``.
  Note that every checkpoint must be reported alongside a set of metrics --
  this way, checkpoints can be ordered with respect to a specified metric.
- The saved checkpoint during training iteration `step` is saved to the path
  ``<local_dir>/<exp_name>/<trial_name>/checkpoint_<step>`` on the node on which training happens
  and will be synced to either head node or cloud storage depending on
  :ref:`storage option configured <tune-storage-options>`.
- We implement *checkpoint loading* with ``session.get_checkpoint()``.
  This will be populated with a trial's latest checkpoint whenever Tune restores a trial.
  This happens when (1) a trial is configured to retry after encountering a failure
  <link to failure config / fault tolerance guide>, (2) the experiment is being
  restored <link to tune stopping/restoring guide>, and (3) the trial is being
  resumed after a pause (ex: :doc:`PBT </tune/examples/pbt_guide>`).

  .. TODO: for (1), link to tune fault tolerance guide. For (2), link to tune restore guide.

.. note:: ``checkpoint_frequency`` and ``checkpoint_at_end`` will not work with Function API checkpointing.
These are configured manually with Function Trainable:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_checkpointing_periodic_start__
    :end-before: __function_api_checkpointing_periodic_end__


See :ref:`here for more information on creating checkpoints <air-checkpoint-ref>`.
If using framework-specific trainers from Ray AIR, see :ref:`here <air-trainer-ref>` for
references to framework-specific checkpoints such as `TensorflowCheckpoint`.

To create an AIR checkpoint, one can either use :meth:`~ray.air.checkpoint.Checkpoint.from_dict`
(above example) or :meth:`~ray.air.checkpoint.Checkpoint.from_directory` APIs (below example).

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_checkpointing_from_dir_start__
    :end-before: __function_api_checkpointing_from_dir_end__

.. warning:: When using ``from_directory``, the content of the checkpoint will be copied and moved
    to a tune managed folder (<trial_name>/checkpoint_<step>). This may cause some inefficiency when
    checkpoint is synced to driver node or the cloud. We are planning to work on it to address the
    issue.


.. _tune-class-trainable-checkpointing:

Class API Checkpointing
-----------------------

You can also implement checkpoint/restore using the Trainable Class API:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_checkpointing_start__
    :end-before: __class_api_checkpointing_end__

You can checkpoint with three different mechanisms: manually, periodically, and at termination.

Manual Checkpointing
~~~~~~~~~~~~~~~~~~~~

A custom Trainable can manually trigger checkpointing by returning ``should_checkpoint: True``
(or ``tune.result.SHOULD_CHECKPOINT: True``) in the result dictionary of `step`.
This can be especially helpful in spot instances:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_manual_checkpointing_start__
    :end-before: __class_api_manual_checkpointing_end__

In the above example, if ``detect_instance_preemption`` returns True, manual checkpointing can be triggered.


Periodic Checkpointing
~~~~~~~~~~~~~~~~~~~~~~

Periodic checkpointing can be used to provide fault-tolerance for experiments.
This can be enabled by setting ``checkpoint_frequency=<int>`` and ``max_failures=<int>`` to checkpoint trials
every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_periodic_checkpointing_start__
    :end-before: __class_api_periodic_checkpointing_end__


Checkpointing at Termination
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The checkpoint_frequency may not coincide with the exact end of an experiment.
If you want a checkpoint to be created at the end of a trial, you can additionally set the ``checkpoint_at_end=True``:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_end_checkpointing_start__
    :end-before: __class_api_end_checkpointing_end__


Configurations
==============
Checkpointing can be configured through :class:`CheckpointConfig <ray.air.config.CheckpointConfig>`.
Some of the configurations do not apply to Function Trainable API, since checkpointing frequency
is determined manually within the user-defined training loop. See the compatibility matrix below.

.. list-table::
   :header-rows: 1

   * -
     - Function API
     - Class API
   * - ``num_to_keep``
     - ✅
     - ✅
   * - ``checkpoint_score_attribute``
     - ✅
     - ✅
   * - ``checkpoint_score_order``
     - ✅
     - ✅
   * - ``checkpoint_frequency``
     - ✅
     - ❌
   * - ``checkpoint_at_end``
     - ✅
     - ❌



Summary
=======

In this user guide, we covered how to save and load trial checkpoints in Tune. Once checkpointing is enabled,
move onto one of the following guides to  find out how to:

- :doc:`Extract checkpoints from Tune experiment results </tune/examples/tune_analyze_results>`
- :ref:`Configure persistent storage options` for a :ref:`distributed Tune experiment <tune-distributed-ref>`

.. _tune-persisted-experiment-data:

Appendix - Types of data stored by Tune
=======================================

Tune stores mainly three types of data.

Experiment Checkpoints
~~~~~~~~~~~~~~~~~~~~~~

Experiment-level checkpoints save the experiment state. This includes the state of the searcher,
the list of trials and their statuses (e.g., PENDING, RUNNING, TERMINATED, ERROR), and
metadata pertaining to each trial (e.g., hyperparameter configuration, some derived trial results
(min, max, last), etc).

The experiment-level checkpoint is periodically saved by the driver on the head node.
By default, the frequency at which it is saved is automatically
adjusted so that at most 5% of the time is spent saving experiment checkpoints,
and the remaining time is used for handling training results and scheduling.
This time can also be adjusted with the
:ref:`TUNE_GLOBAL_CHECKPOINT_S environment variable <tune-env-vars>`.

Trial Checkpoints
~~~~~~~~~~~~~~~~~

Trial-level checkpoints capture the per-trial state. They are saved by the
:ref:`trainable <tune_60_seconds_trainables>` itself. This often includes the model and optimizer states.
Following are a few uses of trial checkpoints:

- If the trial is interrupted for some reason (e.g., on spot instances), it can be resumed from the
  last state. No training time is lost.
- Some searchers or schedulers pause trials to free up resources for other trials to train in
  the meantime. This only makes sense if the trials can then continue training from the latest state.
- The checkpoint can be later used for other downstream tasks like batch inference.

Learn saving and loading trial checkpoints here: :ref:`here <tune-trial-checkpoint>`.

Trial Results
~~~~~~~~~~~~~

Metrics reported by trials are saved and logged to their respective trial directories.
This is the data stored in CSV, JSON or Tensorboard (events.out.tfevents.*) formats.
that can be inspected by Tensorboard and used for post-experiment analysis.