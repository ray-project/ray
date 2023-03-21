.. _tune-fault-tolerance:

How to Enable Fault Tolerance in Ray Tune
=========================================

Fault tolerance is an important feature for distributed machine learning experiments
that can help mitigate the impact of node failures due to out of memory and out of disk issues.

With fault tolerance, users can:

- **Save time and resources by preserving training progress** even if a node fails.
- **Access the cost savings of preemptible spot instance nodes** in the distributed setting.

.. seealso::

    In a *distributed* Tune experiment, a prerequisite to enabling fault tolerance
    is configuring some form of persistent storage where all trial results and
    checkpoints can be consolidated. See :ref:`tune-storage-options`.

In this guide, we will cover how to enable different types of fault tolerance offered by Ray Tune.


.. _tune-experiment-level-fault-tolerance:

Experiment-level Fault Tolerance in Tune
----------------------------------------

At the experiment level, users can use the :meth:`Tuner.restore <ray.tune.Tuner.restore>`
method to resume a Tune experiment.

You should use :meth:`Tuner.restore <ray.tune.Tuner.restore>` in the following cases:

1. The driver script that calls :meth:`Tuner.fit() <ray.tune.Tuner.fit>` errors out (e.g., due to the head node running out of memory or out of disk).
2. The experiment is manually interrupted with ``Ctrl+C``.
3. The entire cluster, and the experiment along with it, crashes due to an ephemeral error such as the network going down or Ray object store memory filling up.

.. note::

    :meth:`Tuner.restore <ray.tune.Tuner.restore>` is *not* meant for resuming a terminated
    experiment and modifying hyperparameter search spaces or stopping criteria.
    Rather, experiment restoration is meant to resume and complete the *exact job*
    that was previously submitted via :meth:`Tuner.fit <ray.tune.Tuner.fit>`.

    For example, consider a Tune experiment configured to run for ``10`` training iterations,
    where all trials have already completed.
    :meth:`Tuner.restore <ray.tune.Tuner.restore>` cannot be used to restore the experiment,
    change the number of training iterations to ``20``, then continue training.
    Instead, this should be achieved by starting a *new* experiment and initializing
    your model weights with a checkpoint from the previous experiment.

    See :ref:`this FAQ post <TODO>` for an example of this.


.. note::

    Bugs in your user-defined training loop cannot be fixed with restoration. Instead, the issue
    that caused the experiment to crash in the first place should be *ephemeral*,
    meaning that the retry attempt after restoring can succeed the next time.


.. _tune-experiment-restore-example:

Example: Restore a Tune Experiment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's say your initial Tune experiment is configured as follows.
The actual training loop is just for demonstration purposes: the important detail is that
:ref:`saving and loading checkpoints has been implemented in the trainable <tune-trial-checkpoint>`.

.. literalinclude:: /tune/doc_code/fault_tolerance.py
    :language: python
    :start-after: __ft_initial_run_start__
    :end-before: __ft_initial_run_end__

The results and checkpoints of the experiment are saved to ``~/ray_results/tune_fault_tolerance_guide``,
as configured by :class:`~ray.air.config.RunConfig`.
If the experient has been interrupted due to one of the reasons listed above, use this path to restore the experiment:

.. literalinclude:: /tune/doc_code/fault_tolerance.py
    :language: python
    :start-after: __ft_restored_run_start__
    :end-before: __ft_restored_run_end__

.. tip::

    You can also restore the experiment from a cloud bucket path. See :ref:`tune-storage-options`.

    .. code-block:: python

        tuner = tune.Tuner.restore(
            path="s3://cloud-bucket/tune_fault_tolerance_guide", trainable=trainable
        )


.. _tune-experiment-autoresume-example:

Example: Auto-resume
~~~~~~~~~~~~~~~~~~~~

When running in a production setting, one may want a *single script* that (1) launches the
initial training run in the beginning and (2) restores the experiment (1) already happened previously.

Use the :meth:`Tuner.can_restore <ray.tune.Tuner.can_restore>` utility to accomplish this:

.. literalinclude:: /tune/doc_code/fault_tolerance.py
    :language: python
    :start-after: __ft_restore_multiplexing_start__
    :end-before: __ft_restore_multiplexing_start__

Running this script the first time will launch the initial training run.
Running this script the second time will attempt to resume from the outputs of the first run.


.. _tune-trial-level-fault-tolerance:

Trial-level Fault Tolerance in Tune
-----------------------------------

Trial-level fault tolerance deals with individual trial failures in the cluster, which can be caused by:

- Running with preemptible spot instances.
- Ephemeral network connection issues.
- Nodes running out of memory or out of disk space.

Ray Tune provides a way to configure failure handling of individual trials with the :class:`~ray.air.config.FailureConfig`.

Assuming that we're using the ``trainable`` from the previous example that implements
trial checkpoint saving and loading, here is how to configure :class:`~ray.air.config.FailureConfig`.

.. literalinclude:: /tune/doc_code/fault_tolerance.py
    :language: python
    :start-after: __ft_trial_failure_start__
    :end-before: __ft_trial_failure_end__

When a trial encounters a runtime error, the above configuration will re-schedule that trial
up to ``max_failures=3`` times.

Similarly, if a node failure occurs for node ``X`` (e.g., pre-empted or lost connection),
this configuration will reschedule all trials that lived on node ``X`` up to ``3`` times.


Summary
-------

In this user guide, we covered how to enable experiment-level and trial-level fault tolerance in Ray Tune.

See the following resources for more information:

- :ref:`tune-storage-options`
- :ref:`tune-trial-checkpoint`
- :ref:`tune-distributed-ref`