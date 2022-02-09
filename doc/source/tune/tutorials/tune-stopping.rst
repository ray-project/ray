Stopping and Resuming Tune Trials
=================================

Ray Tune periodically checkpoints the experiment state so that it can be restarted when it fails or stops.
The checkpointing period is dynamically adjusted so that at least 95% of the time is used for handling
training results and scheduling.

If you send a SIGINT signal to the process running ``tune.run()`` (which is
usually what happens when you press Ctrl+C in the console), Ray Tune shuts
down training gracefully and saves a final experiment-level checkpoint.

How to resume a Tune run?
-------------------------

If you've stopped a run and and want to resume from where you left off,
you can then call ``tune.run()`` with ``resume=True`` like this:

.. code-block:: python
    :emphasize-lines: 5

    tune.run(
        train,
        # other configuration
        name="my_experiment",
        resume=True
    )

You will have to pass a ``name`` if you are using ``resume=True`` so that Ray Tune can detect the experiment
folder (which is usually stored at e.g. ``~/ray_results/my_experiment``).
If you forgot to pass a name in the first call, you can still pass the name when you resume the run.
Please note that in this case it is likely that your experiment name has a date suffix, so if you
ran ``tune.run(my_trainable)``, the ``name`` might look like something like this:
``my_trainable_2021-01-29_10-16-44``.

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

Another useful option to know about is ``resume="AUTO"``, which will attempt to resume the experiment if possible,
and otherwise will start a new experiment.
For more details and other options for ``resume``, see the :ref:`Tune run API documentation <tune-run-ref>`.

.. _tune-stopping-ref:

How to stop Tune runs programmatically?
---------------------------------------

We've just covered the case in which you manually interrupt a Tune run.
But you can also control when trials are stopped early by passing the ``stop`` argument to ``tune.run``.
This argument takes, a dictionary, a function, or a :class:`Stopper <ray.tune.stopper.Stopper>` class as an argument.

If a dictionary is passed in, the keys may be any field in the return result of ``tune.report`` in the
Function API or ``step()`` (including the results from ``step`` and auto-filled metrics).

Stopping with a dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~

In the example below, each trial will be stopped either when it completes ``10`` iterations or when it
reaches a mean accuracy of ``0.98``.
These metrics are assumed to be **increasing**.

.. code-block:: python

    # training_iteration is an auto-filled metric by Tune.
    tune.run(
        my_trainable,
        stop={"training_iteration": 10, "mean_accuracy": 0.98}
    )

Stopping with a function
~~~~~~~~~~~~~~~~~~~~~~~~

For more flexibility, you can pass in a function instead.
If a function is passed in, it must take ``(trial_id, result)`` as arguments and return a boolean
(``True`` if trial should be stopped and ``False`` otherwise).

.. code-block:: python

    def stopper(trial_id, result):
        return result["mean_accuracy"] / result["training_iteration"] > 5

    tune.run(my_trainable, stop=stopper)

Stopping with a class
~~~~~~~~~~~~~~~~~~~~~

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
    tune.run(my_trainable, stop=stopper)


Note that in the above example the currently running trials will not stop immediately but will do so
once their current iterations are complete.

Ray Tune comes with a set of out-of-the-box stopper classes. See the :ref:`Stopper <tune-stoppers>` documentation.


Stopping after the first failure
--------------------------------

By default, ``tune.run`` will continue executing until all trials have terminated or errored.
To stop the entire Tune run as soon as **any** trial errors:

.. code-block:: python

    tune.run(trainable, fail_fast=True)

This is useful when you are trying to setup a large hyperparameter experiment.
