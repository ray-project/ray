:orphan:

.. _train-fault-tolerance-deprecated-api:

Handling Failures and Node Preemption (Deprecated API)
======================================================

.. important::
    This user guide covers deprecated fault tolerance APIs. See :ref:`train-fault-tolerance` for the new API user guide.

    Please see :ref:`here <train-fault-tolerance-deprecation-info>` for information about the deprecation and migration.

Automatically Recover from Train Worker Failures
------------------------------------------------

Ray Train has built-in fault tolerance to recover from worker failures (i.e.
``RayActorError``\s). When a failure is detected, the workers will be shut
down and new workers will be added in.

The training function will be restarted, but progress from the previous execution can
be resumed through checkpointing.

.. tip::
    In order to retain progress when recovery, your training function
    **must** implement logic for both :ref:`saving <train-dl-saving-checkpoints>`
    *and* :ref:`loading checkpoints <train-dl-loading-checkpoints>`.

Each instance of recovery from a worker failure is considered a retry. The
number of retries is configurable through the ``max_failures`` attribute of the
:class:`~ray.train.FailureConfig` argument set in the :class:`~ray.train.RunConfig`
passed to the ``Trainer``:

.. literalinclude:: ../doc_code/fault_tolerance.py
    :language: python
    :start-after: __failure_config_start__
    :end-before: __failure_config_end__

Which checkpoint will be restored?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Train will automatically resume training from the latest available
:ref:`checkpoint reported to Ray Train <train-checkpointing>`.

This will be the last checkpoint passed to :func:`train.report() <ray.train.report>`.


Restore a Ray Train Experiment
------------------------------

At the experiment level, Trainer restoration
allows you to resume a previously interrupted experiment from where it left off.

A Train experiment may be interrupted due to one of the following reasons:

- The experiment was manually interrupted (e.g., Ctrl+C, or pre-empted head node instance).
- The head node crashed (e.g., OOM or some other runtime error).
- The entire cluster went down (e.g., network error affecting all nodes).

Trainer restoration is possible for all of Ray Train's built-in trainers,
but we use ``TorchTrainer`` in the examples for demonstration.
We also use ``<Framework>Trainer`` to refer to methods that are shared across all
built-in trainers.

Let's say your initial Train experiment is configured as follows.
The actual training loop is just for demonstration purposes: the important detail is that
:ref:`saving <train-dl-saving-checkpoints>` *and* :ref:`loading checkpoints <train-dl-loading-checkpoints>`
has been implemented.

.. literalinclude:: ../doc_code/dl_guide.py
    :language: python
    :start-after: __ft_initial_run_start__
    :end-before: __ft_initial_run_end__

The results and checkpoints of the experiment are saved to the path configured by :class:`~ray.train.RunConfig`.
If the experiment has been interrupted due to one of the reasons listed above, use this path to resume:

.. literalinclude:: ../doc_code/dl_guide.py
    :language: python
    :start-after: __ft_restored_run_start__
    :end-before: __ft_restored_run_end__

.. tip::

    You can also restore from a remote path (e.g., from an experiment directory stored in a s3 bucket).

    .. literalinclude:: ../doc_code/dl_guide.py
        :language: python
        :dedent:
        :start-after: __ft_restore_from_cloud_initial_start__
        :end-before: __ft_restore_from_cloud_initial_end__

    .. literalinclude:: ../doc_code/dl_guide.py
        :language: python
        :dedent:
        :start-after: __ft_restore_from_cloud_restored_start__
        :end-before: __ft_restore_from_cloud_restored_end__

.. note::

    Different trainers may allow more parameters to be optionally re-specified on restore.
    Only **datasets** are required to be re-specified on restore, if they were supplied originally.

    `TorchTrainer.restore`, `TensorflowTrainer.restore`, and `HorovodTrainer.restore`
    can take in the same parameters as their parent class's
    :meth:`DataParallelTrainer.restore <ray.train.data_parallel_trainer.DataParallelTrainer.restore>`.

    Unless otherwise specified, other trainers will accept the same parameters as
    :meth:`BaseTrainer.restore <ray.train.trainer.BaseTrainer.restore>`.


Auto-resume
~~~~~~~~~~~

Adding the branching logic below will allow you to run the same script after the interrupt,
picking up training from where you left on the previous run. Notice that we use the
:meth:`<Framework>Trainer.can_restore <ray.train.trainer.BaseTrainer.can_restore>` utility method
to determine the existence and validity of the given experiment directory.

.. literalinclude:: ../doc_code/dl_guide.py
    :language: python
    :start-after: __ft_autoresume_start__
    :end-before: __ft_autoresume_end__

.. seealso::

    See the :meth:`BaseTrainer.restore <ray.train.trainer.BaseTrainer.restore>` docstring
    for a full example.

.. note::

    `<Framework>Trainer.restore` is different from
    :class:`<Framework>Trainer(..., resume_from_checkpoint=...) <ray.train.trainer.BaseTrainer>`.
    `resume_from_checkpoint` is meant to be used to start a *new* Train experiment,
    which writes results to a new directory and starts over from iteration 0.

    `<Framework>Trainer.restore` is used to continue an existing experiment, where
    new results will continue to be appended to existing logs.
