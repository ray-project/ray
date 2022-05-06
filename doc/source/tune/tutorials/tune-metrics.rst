A Guide To Callbacks & Metrics in Tune
======================================

.. _tune-callbacks:

How to work with Callbacks?
---------------------------

Ray Tune supports callbacks that are called during various times of the training process.
Callbacks can be passed as a parameter to ``tune.run()``, and the sub-method you provide will be invoked automatically.

This simple callback just prints a metric each time a result is received:

.. code-block:: python

    from ray import tune
    from ray.tune import Callback


    class MyCallback(Callback):
        def on_trial_result(self, iteration, trials, trial, result, **info):
            print(f"Got result: {result['metric']}")


    def train(config):
        for i in range(10):
            tune.report(metric=i)


    tune.run(
        train,
        callbacks=[MyCallback()])

For more details and available hooks, please :ref:`see the API docs for Ray Tune callbacks <tune-callbacks-docs>`.


.. _tune-autofilled-metrics:

How to use log metrics in Tune?
-------------------------------

You can log arbitrary values and metrics in both Function and Class training APIs:

.. code-block:: python

    def trainable(config):
        for i in range(num_epochs):
            ...
            tune.report(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)

    class Trainable(tune.Trainable):
        def step(self):
            ...
            # don't call report here!
            return dict(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)


.. tip::
    Note that ``tune.report()`` is not meant to transfer large amounts of data, like models or datasets.
    Doing so can incur large overheads and slow down your Tune run significantly.

Which metrics get automatically filled in?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tune has the concept of auto-filled metrics.
During training, Tune will automatically log the below metrics in addition to any user-provided values.
All of these can be used as stopping conditions or passed as a parameter to Trial Schedulers/Search Algorithms.

* ``config``: The hyperparameter configuration
* ``date``: String-formatted date and time when the result was processed
* ``done``: True if the trial has been finished, False otherwise
* ``episodes_total``: Total number of episodes (for RLlib trainables)
* ``experiment_id``: Unique experiment ID
* ``experiment_tag``: Unique experiment tag (includes parameter values)
* ``hostname``: Hostname of the worker
* ``iterations_since_restore``: The number of times ``tune.report()/trainable.train()`` has been
  called after restoring the worker from a checkpoint
* ``node_ip``: Host IP of the worker
* ``pid``: Process ID (PID) of the worker process
* ``time_since_restore``: Time in seconds since restoring from a checkpoint.
* ``time_this_iter_s``: Runtime of the current training iteration in seconds (i.e.
  one call to the trainable function or to ``_train()`` in the class API.
* ``time_total_s``: Total runtime in seconds.
* ``timestamp``: Timestamp when the result was processed
* ``timesteps_since_restore``: Number of timesteps since restoring from a checkpoint
* ``timesteps_total``: Total number of timesteps
* ``training_iteration``: The number of times ``tune.report()`` has been
  called
* ``trial_id``: Unique trial ID

All of these metrics can be seen in the ``Trial.last_result`` dictionary.
