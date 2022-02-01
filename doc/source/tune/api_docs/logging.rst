.. _loggers-docstring:

Loggers (tune.logger)
=====================

Tune has default loggers for Tensorboard, CSV, and JSON formats. By default, Tune only logs the returned result dictionaries from the training function.

If you need to log something lower level like model weights or gradients, see :ref:`Trainable Logging <trainable-logging>`.

.. note::
    Tune's per-trial ``Logger`` classes have been deprecated. They can still be used, but we encourage you
    to use our new interface with the ``LoggerCallback`` class instead.

Custom Loggers
--------------

You can create a custom logger by inheriting the LoggerCallback interface (:ref:`logger-interface`):

.. code-block:: python

    from typing import Dict, List

    import json
    import os

    from ray.tune.logger import LoggerCallback


    class CustomLoggerCallback(LoggerCallback):
        """Custom logger interface"""

        def __init__(self, filename: str = "log.txt):
            self._trial_files = {}
            self._filename = filename

        def log_trial_start(self, trial: "Trial"):
            trial_logfile = os.path.join(trial.logdir, self._filename)
            self._trial_files[trial] = open(trial_logfile, "at")

        def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
            if trial in self._trial_files:
                self._trial_files[trial].write(json.dumps(result))

        def on_trial_complete(self, iteration: int, trials: List["Trial"],
                              trial: "Trial", **info):
            if trial in self._trial_files:
                self._trial_files[trial].close()
                del self._trial_files[trial]


You can then pass in your own logger as follows:

.. code-block:: python

    from ray import tune

    tune.run(
        MyTrainableClass,
        name="experiment_name",
        callbacks=[CustomLoggerCallback("log_test.txt")]
    )

Per default, Ray Tune creates JSON, CSV and TensorboardX logger callbacks if you don't pass them yourself.
You can disable this behavior by setting the ``TUNE_DISABLE_AUTO_CALLBACK_LOGGERS`` environment variable to ``"1"``.

An example of creating a custom logger can be found in :doc:`/tune/examples/logging_example`.

.. _trainable-logging:

Trainable Logging
-----------------

By default, Tune only logs the *training result dictionaries* from your Trainable. However, you may want to visualize the model weights, model graph, or use a custom logging library that requires multi-process logging. For example, you may want to do this if you're trying to log images to Tensorboard.

You can do this in the trainable, as shown below:

.. tip:: Make sure that any logging calls or objects stay within scope of the Trainable. You may see Pickling/serialization errors or inconsistent logs otherwise.

**Function API**:

``library`` refers to whatever 3rd party logging library you are using.

.. code-block:: python

    def trainable(config):
        library.init(
            name=trial_id,
            id=trial_id,
            resume=trial_id,
            reinit=True,
            allow_val_change=True)
        library.set_log_path(tune.get_trial_dir())

        for step in range(100):
            library.log_model(...)
            library.log(results, step=step)
            tune.report(results)


**Class API**:

.. code-block:: python

    class CustomLogging(tune.Trainable)
        def setup(self, config):
            trial_id = self.trial_id
            library.init(
                name=trial_id,
                id=trial_id,
                resume=trial_id,
                reinit=True,
                allow_val_change=True)
            library.set_log_path(self.logdir)

        def step(self):
            library.log_model(...)

        def log_result(self, result):
            res_dict = {
                str(k): v
                for k, v in result.items()
                if (v and "config" not in k and not isinstance(v, str))
            }
            step = result["training_iteration"]
            library.log(res_dict, step=step)

Use ``self.logdir`` (only for Class API) or ``tune.get_trial_dir()`` (only for Function API) for the trial log directory.

In the distributed case, these logs will be sync'ed back to the driver under your logger path. This will allow you to visualize and analyze logs of all distributed training workers on a single machine.


Viskit
------

Tune automatically integrates with `Viskit <https://github.com/vitchyr/viskit>`_ via the ``CSVLoggerCallback`` outputs. To use VisKit (you may have to install some dependencies), run:

.. code-block:: bash

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

The nonrelevant metrics (like timing stats) can be disabled on the left to show only the relevant ones (like accuracy, loss, etc.).

.. image:: ../images/ray-tune-viskit.png


TBXLogger
---------

.. autoclass:: ray.tune.logger.TBXLoggerCallback

JsonLogger
----------

.. autoclass:: ray.tune.logger.JsonLoggerCallback

CSVLogger
---------

.. autoclass:: ray.tune.logger.CSVLoggerCallback

MLFlowLogger
------------

Tune also provides a default logger for `MLflow <https://mlflow.org>`_. You can install MLflow via ``pip install mlflow``.
You can see the :doc:`tutorial here </tune/tutorials/tune-mlflow>`.

WandbLogger
-----------

Tune also provides a default logger for `Weights & Biases <https://www.wandb.ai/>`_. You can install Wandb via ``pip install wandb``.
You can see the :doc:`tutorial here </tune/tutorials/tune-wandb>`


.. _logger-interface:

LoggerCallback
--------------

.. autoclass:: ray.tune.logger.LoggerCallback
    :members: log_trial_start, log_trial_restore, log_trial_save, log_trial_result, log_trial_end
