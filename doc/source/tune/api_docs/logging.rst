.. _loggers-docstring:

Loggers (tune.logger)
=====================

Tune has default loggers for Tensorboard, CSV, and JSON formats. By default, Tune only logs the returned result dictionaries from the training function.

If you need to log something lower level like model weights or gradients, see :ref:`Trainable Logging <trainable-logging>`.

Custom Loggers
--------------

You can create a custom logger by inheriting the Logger interface (:ref:`logger-interface`):

.. code-block:: python

    from ray.tune.logger import Logger

    class MLFLowLogger(Logger):
        """MLFlow logger.

        Requires the experiment configuration to have a MLFlow Experiment ID
        or manually set the proper environment variables.
        """

        def _init(self):
            from mlflow.tracking import MlflowClient
            client = MlflowClient()

            # self.config is the same config that your Trainable will see.
            run = client.create_run(self.config.get("mlflow_experiment_id"))
            self._run_id = run.info.run_id
            for key, value in self.config.items():
                client.log_param(self._run_id, key, value)
            self.client = client

        def on_result(self, result):
            for key, value in result.items():
                if not isinstance(value, float):
                    continue
                self.client.log_metric(
                    self._run_id, key, value, step=result.get(TRAINING_ITERATION))

        def close(self):
            self.client.set_terminated(self._run_id)

You can then pass in your own logger as follows:

.. code-block:: python

    from ray.tune.logger import DEFAULT_LOGGERS

    tune.run(
        MyTrainableClass,
        name="experiment_name",
        loggers=DEFAULT_LOGGERS + (CustomLogger1, CustomLogger2)
    )

These loggers will be called along with the default Tune loggers. You can also check out `logger.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/logger.py>`__ for implementation details.

An example of creating a custom logger can be found in :doc:`/tune/examples/logging_example`.

.. _trainable-logging:

Trainable Logging
-----------------

By default, Tune only logs the *training result dictionaries* from your Trainable. However, you may want to visualize the model weights, model graph, or use a custom logging library that requires multi-process logging. For example, you may want to do this if:

 * you're using `Weights and Biases <https://www.wandb.com/>`_
 * you're using `MLFlow <https://github.com/mlflow/mlflow/>`__
 * you're trying to log images to Tensorboard.

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

Tune automatically integrates with `Viskit <https://github.com/vitchyr/viskit>`_ via the ``CSVLogger`` outputs. To use VisKit (you may have to install some dependencies), run:

.. code-block:: bash

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

The nonrelevant metrics (like timing stats) can be disabled on the left to show only the relevant ones (like accuracy, loss, etc.).

.. image:: /ray-tune-viskit.png


UnifiedLogger
-------------

.. autoclass:: ray.tune.logger.UnifiedLogger

TBXLogger
---------

.. autoclass:: ray.tune.logger.TBXLogger

JsonLogger
----------

.. autoclass:: ray.tune.logger.JsonLogger

CSVLogger
---------

.. autoclass:: ray.tune.logger.CSVLogger

MLFLowLogger
------------

Tune also provides a default logger for `MLFlow <https://mlflow.org>`_. You can install MLFlow via ``pip install mlflow``. An example can be found in :doc:`/tune/examples/mlflow_example`. Note that this currently does not include artifact logging support. For this, you can use the native MLFlow APIs inside your Trainable definition.

.. autoclass:: ray.tune.logger.MLFLowLogger


.. _logger-interface:

Logger
------

.. autoclass:: ray.tune.logger.Logger
