.. _tune-stopping-guide:

How to Define Stopping Criteria for a Tune Experiment
=====================================================

When running a Tune experiment, it is important to define stopping criteria to ensure that the experiment ends in a timely and efficient manner. In this user guide, we will discuss how to define stopping criteria for a Tune experiment.

Stopping a Tune Experiment Manually
-----------------------------------

If you send a SIGINT signal to the process running ``Tuner.fit()`` (which is
usually what happens when you press Ctrl+C in the console), Ray Tune shuts
down training gracefully and saves the final experiment state.

Ray Tune also accepts the SIGUSR1 signal to interrupt training gracefully. This
should be used when running Ray Tune in a remote Ray task
as Ray will filter out SIGINT and SIGTERM signals per default.

.. _tune-stopping-ref:

Stop Programmatically with Metric-based Criteria
------------------------------------------------

In addition to manual stopping, Tune provides several ways to stop experiments programmatically. The simplest way is to use a static condition. A static condition is a fixed set of rules that determine when the experiment should stop. Here are a few ways to use static conditions:

You can implement the stopping criteria using either a dictionary, a function, or a custom :class:`Stopper <ray.tune.stopper.Stopper>`.

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

    def stopper(trial_id: str, result: dict):
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


Stop on Trial Failures
----------------------

In addition to stopping trials based on their performance, you can also stop the entire experiment if any trial encounters a runtime error. To do this, you can use the FailureConfig class. Here is an example:

With this configuration, if any trial encounters an error, the entire experiment will stop immediately.

.. code-block:: python

    tune.Tuner(trainable, run_config=air.RunConfig(failure_config=air.FailureConfig(fail_fast=True))).fit()

This is useful when you are debugging a Tune experiment with many trials.


Early stopping with Tune schedulers
-----------------------------------

Another way to stop Tune experiments is to use early stopping schedulers. These schedulers monitor the performance of trials and stop them early if they are not making sufficient progress. Tune currently supports two schedulers: ASHA and BOHB.
