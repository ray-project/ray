.. _tune-stopping-guide:

Stopping and Resuming a Tune Run
================================

Ray Tune periodically checkpoints the run state so that it can be restarted when it fails or stops.

If you send a SIGINT signal to the process running ``Tuner.fit()`` (which is
usually what happens when you press Ctrl+C in the console), Ray Tune shuts
down training gracefully and saves a final experiment-level checkpoint.

Ray Tune also accepts the SIGUSR1 signal to interrupt training gracefully. This
should be used when running Ray Tune in a remote Ray task
as Ray will filter out SIGINT and SIGTERM signals per default.

How to resume a Tune run?
-------------------------

If you've stopped a run and and want to resume from where you left off,
you can then call ``Tuner.restore()`` like this:

.. code-block:: python

    tuner = Tuner.restore(
        path="~/ray_results/my_experiment"
    )
    tuner.fit()

There are a few options for restoring an experiment:
``resume_unfinished``, ``resume_errored`` and ``restart_errored``.
Please see the documentation of
:meth:`Tuner.restore() <ray.tune.tuner.Tuner.restore>` for more details.

``path`` here is determined by the ``air.RunConfig.name`` you supplied to your ``Tuner()``.
If you didn't supply name to ``Tuner``, it is likely that your ``path`` looks something like:
"~/ray_results/my_trainable_2021-01-29_10-16-44".

You can see which name you need to pass by taking a look at the results table
of your original tuning run:

.. code-block::
    :emphasize-lines: 5

    == Status ==
    Memory usage on this node: 11.0/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 1/16 CPUs, 0/0 GPUs, 0.0/4.69 GiB heap, 0.0/1.61 GiB objects
    Result logdir: /Users/ray/ray_results/my_trainable_2021-01-29_10-16-44
    Number of trials: 1/1 (1 RUNNING)

What's happening under the hood in a Tune run?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:ref:`Here <tune-persisted-experiment-data>`, we describe the two types of Tune checkpoints:
experiment-level and trial-level checkpoints.

Upon resuming an interrupted/errored Tune run:

#. Tune first looks at the experiment-level checkpoint to find the list of trials at the time of the interruption.

#. Tune then locates and restores from the trial-level checkpoint of each trial.

#. Depending on the specified resume option (``resume_unfinished``, ``resume_errored``, ``restart_errored``), Tune decides whether to restore a given unfinished trial from its latest available checkpoint or to start from scratch.

.. _tune-stopping-ref:

How to stop Tune runs programmatically?
---------------------------------------

We've just covered the case in which you manually interrupt a Tune run.
But you can also control when trials are stopped early by passing the ``stop`` argument to ``Tuner``.
This argument takes, a dictionary, a function, or a :class:`Stopper <ray.tune.stopper.Stopper>` class as an argument.

If a dictionary is passed in, the keys may be any field in the return result of ``session.report`` in the
Function API or ``step()`` (including the results from ``step`` and auto-filled metrics).

Stopping Tune runs with a dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the example below, each trial will be stopped either when it completes ``10`` iterations or when it
reaches a mean accuracy of ``0.98``.
These metrics are assumed to be **increasing**.

.. code-block:: python

    # training_iteration is an auto-filled metric by Tune.
    tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(stop={"training_iteration": 10, "mean_accuracy": 0.98})
    ).fit()

Stopping Tune runs with a function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more flexibility, you can pass in a function instead.
If a function is passed in, it must take ``(trial_id, result)`` as arguments and return a boolean
(``True`` if trial should be stopped and ``False`` otherwise).

.. code-block:: python

    def stopper(trial_id, result):
        return result["mean_accuracy"] / result["training_iteration"] > 5

    tune.Tuner(my_trainable, run_config=air.RunConfig(stop=stopper)).fit()

Stopping Tune runs with a class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finally, you can implement the :class:`Stopper <ray.tune.stopper.Stopper>` abstract class for stopping entire experiments. For example, the following example stops all trials after the criteria is fulfilled by any individual trial, and prevents new ones from starting:

.. code-block:: python

    from ray.tune import Stopper

    class CustomStopper(Stopper):
        def __init__(self):
            self.should_stop = False

        def __call__(self, trial_id, result):
            if not self.should_stop and result['foo'] > 10:
                self.should_stop = True
            return self.should_stop

        def stop_all(self):
            """Returns whether to stop trials and prevent new ones from starting."""
            return self.should_stop

    stopper = CustomStopper()
    tune.Tuner(my_trainable, run_config=air.RunConfig(stop=stopper)).fit()


Note that in the above example the currently running trials will not stop immediately but will do so
once their current iterations are complete.

Ray Tune comes with a set of out-of-the-box stopper classes. See the :ref:`Stopper <tune-stoppers>` documentation.


Stopping a ``Tuner`` after the first failure
--------------------------------------------

By default, ``Tuner.fit()`` will continue executing until all trials have terminated or errored.
To stop the entire Tune run as soon as **any** trial errors:

.. code-block:: python

    tune.Tuner(trainable, run_config=air.RunConfig(failure_config=air.FailureConfig(fail_fast=True))).fit()

This is useful when you are trying to setup a large hyperparameter experiment.
