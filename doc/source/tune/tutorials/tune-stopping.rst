.. _tune-stopping-guide:
.. _tune-stopping-ref:

How to Define Stopping Criteria for a Ray Tune Experiment
=========================================================

When running a Tune experiment, it can be challenging to determine the ideal duration of training beforehand. Stopping criteria in Tune can be useful for terminating training based on specific conditions.

For instance, one may want to set up the experiment to stop under the following circumstances:

1. Set up an experiment to end after ``N`` epochs or when the reported evaluation score surpasses a particular threshold, whichever occurs first.
2. Stop the experiment after ``T``` seconds.
3. Terminate when trials encounter runtime errors.
4. Stop underperforming trials early by utilizing Tune's early-stopping schedulers.

This user guide will illustrate how to achieve these types of stopping criteria in a Tune experiment.

For all the code examples, we use the following training function for demonstration:

.. literalinclude:: /tune/doc_code/stopping.py
    :language: python
    :start-after: __stopping_example_trainable_start__
    :end-before: __stopping_example_trainable_end__

Stop a Tune experiment manually
-------------------------------

If you send a ``SIGINT`` signal to the process running :meth:`Tuner.fit() <ray.tune.Tuner.fit>`
(which is usually what happens when you press ``Ctrl+C`` in the terminal), Ray Tune shuts
down training gracefully and saves the final experiment state.

.. note::

    Forcefully terminating a Tune experiment, for example, through multiple ``Ctrl+C``
    commands, will not give Tune the opportunity to snapshot the experiment state
    one last time. If you resume the experiment in the future, this could result
    in resuming with stale state.

Ray Tune also accepts the ``SIGUSR1`` signal to interrupt training gracefully. This
should be used when running Ray Tune in a remote Ray task
as Ray will filter out ``SIGINT`` and ``SIGTERM`` signals per default.


Stop using metric-based criteria
--------------------------------

In addition to manual stopping, Tune provides several ways to stop experiments programmatically. The simplest way is to use metric-based criteria. These are a fixed set of thresholds that determine when the experiment should stop.

You can implement the stopping criteria using either a dictionary, a function, or a custom :class:`Stopper <ray.tune.stopper.Stopper>`.

.. tab-set::

    .. tab-item:: Dictionary

        If a dictionary is passed in, the keys may be any field in the return result of ``session.report`` in the
        Function API or ``step()`` in the Class API.

        .. note::

            This includes :ref:`auto-filled metrics <tune-autofilled-metrics>` such as ``training_iteration``.

        In the example below, each trial will be stopped either when it completes ``10`` iterations or when it
        reaches a mean accuracy of ``0.8`` or more.

        These metrics are assumed to be **increasing**, so the trial will stop once the reported metric has exceeded the threshold specified in the dictionary.

        .. literalinclude:: /tune/doc_code/stopping.py
            :language: python
            :start-after: __stopping_dict_start__
            :end-before: __stopping_dict_end__

    .. tab-item:: User-defined Function

        For more flexibility, you can pass in a function instead.
        If a function is passed in, it must take ``(trial_id: str, result: dict)`` as arguments and return a boolean
        (``True`` if trial should be stopped and ``False`` otherwise).

        In the example below, each trial will be stopped either when it completes ``10`` iterations or when it
        reaches a mean accuracy of ``0.8`` or more.

        .. literalinclude:: /tune/doc_code/stopping.py
            :language: python
            :start-after: __stopping_fn_start__
            :end-before: __stopping_fn_end__

    .. tab-item:: Custom Stopper Class

        Finally, you can implement the :class:`~ray.tune.stopper.Stopper` interface for
        stopping individual trials or even entire experiments based on custom stopping
        criteria. For example, the following example stops all trials after the criteria
        is achieved by any individual trial and prevents new ones from starting:

        .. literalinclude:: /tune/doc_code/stopping.py
            :language: python
            :start-after: __stopping_cls_start__
            :end-before: __stopping_cls_end__

        In the example, once any trial reaches a ``mean_accuracy`` of 0.8 or more, all trials will stop.

        .. note::

            When returning ``True`` from ``stop_all``, currently running trials will not stop immediately.
            They will stop after finishing their ongoing training iteration (after ``session.report`` or ``step``).

        Ray Tune comes with a set of out-of-the-box stopper classes. See the :ref:`Stopper <tune-stoppers>` documentation.


Stop trials after a certain amount of time
------------------------------------------

There are two choices to stop a Tune experiment based on time: stopping trials individually
after a specified timeout, or stopping the full experiment after a certain amount of time.

Stop trials individually with a timeout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use a dictionary stopping criteria as described above, using the ``time_total_s`` metric that is auto-filled by Tune.

.. literalinclude:: /tune/doc_code/stopping.py
    :language: python
    :start-after: __stopping_trials_by_time_start__
    :end-before: __stopping_trials_by_time_end__

.. note::

    You need to include some intermediate reporting via :meth:`session.report <ray.air.session.report>`
    if using the :ref:`Function Trainable API <tune-function-api>`.
    Each report will automatically record the trial's ``time_total_s``, which allows Tune to stop based on time as a metric.

    If the training loop hangs somewhere, Tune will not be able to intercept the training and stop the trial for you.
    In this case, you can explicitly implement timeout logic in the training loop.


Stop the experiment with a timeout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``TuneConfig(time_budget_s)`` configuration to tell Tune to stop the experiment after ``time_budget_s`` seconds.

.. literalinclude:: /tune/doc_code/stopping.py
    :language: python
    :start-after: __stopping_experiment_by_time_start__
    :end-before: __stopping_experiment_by_time_end__

.. note::

    You need to include some intermediate reporting via :meth:`session.report <ray.air.session.report>`
    if using the :ref:`Function Trainable API <tune-function-api>`, for the same reason as above.


Stop on trial failures
----------------------

In addition to stopping trials based on their performance, you can also stop the entire experiment if any trial encounters a runtime error. To do this, you can use the :class:`ray.air.config.FailureConfig` class.

With this configuration, if any trial encounters an error, the entire experiment will stop immediately.

.. literalinclude:: /tune/doc_code/stopping.py
    :language: python
    :start-after: __stopping_on_trial_error_start__
    :end-before: __stopping_on_trial_error_end__

This is useful when you are debugging a Tune experiment with many trials.


Early stopping with Tune schedulers
-----------------------------------

Another way to stop Tune experiments is to use early stopping schedulers.
These schedulers monitor the performance of trials and stop them early if they are not making sufficient progress.

:class:`~ray.tune.schedulers.AsyncHyperBandScheduler` and :class:`~ray.tune.schedulers.HyperBandForBOHB` are examples of early stopping schedulers built into Tune.
See :ref:`the Tune scheduler API reference <tune-schedulers>` for a full list, as well as more realistic examples.

In the following example, we use both a dictionary stopping criteria along with an early-stopping criteria:

.. literalinclude:: /tune/doc_code/stopping.py
    :language: python
    :start-after: __early_stopping_start__
    :end-before: __early_stopping_end__

Summary
-------

In this user guide, we learned how to stop Tune experiments using metrics, trial errors,
and early stopping schedulers.

See the following resources for more information:

- :ref:`Tune Stopper API reference <tune-stoppers>`
- For an experiment that was manually interrupted or the cluster dies unexpectedly while trials are still running, it's possible to resume the experiment. See :ref:`tune-fault-tolerance-ref`.