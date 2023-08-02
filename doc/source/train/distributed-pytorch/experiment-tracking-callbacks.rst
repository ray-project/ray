.. _train-monitoring:

Experiment Tracking and Callbacks
=================================

Ray Train has mechanisms to easily collect intermediate results from the training workers during the training run
and also has a :ref:`Callback interface <train-callbacks>` to perform actions on these intermediate results (such as logging, aggregations, etc.).
You can use either the :ref:`built-in callbacks <air-builtin-callbacks>` that Ray AIR provides,
or implement a :ref:`custom callback <train-custom-callbacks>` for your use case. The callback API
is shared with Ray Tune.

.. _train-callbacks:

Callbacks
---------

You may want to plug in your training code with your favorite experiment tracking framework.
Ray AIR provides an interface to fetch intermediate results and callbacks to process/log your intermediate results
(the values passed into :func:`ray.air.session.report`).

Ray AIR contains :ref:`built-in callbacks <air-builtin-callbacks>` for popular tracking frameworks, or you can implement your own callback via the :ref:`Callback <tune-callbacks-docs>` interface.


Example: Logging to MLflow and TensorBoard
++++++++++++++++++++++++++++++++++++++++++

**Step 1: Install the necessary packages**

.. code-block:: bash

    $ pip install mlflow
    $ pip install tensorboardX

**Step 2: Run the following training script**

.. literalinclude:: /../../python/ray/train/examples/mlflow_simple_example.py
   :language: python

.. _train-custom-callbacks:

Custom Callbacks
++++++++++++++++

If the provided callbacks do not cover your desired integrations or use-cases,
you may always implement a custom callback by subclassing :py:class:`~ray.tune.logger.LoggerCallback`. If
the callback is general enough, please feel welcome to :ref:`add it <getting-involved>`
to the ``ray`` `repository <https://github.com/ray-project/ray>`_.

A simple example for creating a callback that will print out results:

.. code-block:: python

    from typing import List, Dict

    from ray.air import session, RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.tune.logger import LoggerCallback

    # LoggerCallback is a higher level API of Callback.
    class LoggingCallback(LoggerCallback):
        def __init__(self) -> None:
            self.results = []

        def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
            self.results.append(trial.last_result)

    def train_func():
        for i in range(3):
            session.report({"epoch": i})

    callback = LoggingCallback()
    trainer = TorchTrainer(
        train_func,
        run_config=RunConfig(callbacks=[callback]),
        scaling_config=ScalingConfig(num_workers=2),
    )
    trainer.fit()

    print("\n".join([str(x) for x in callback.results]))
    # {'trial_id': '0f1d0_00000', 'experiment_id': '494a1d050b4a4d11aeabd87ba475fcd3', 'date': '2022-06-27_17-03-28', 'timestamp': 1656349408, 'pid': 23018, 'hostname': 'ip-172-31-43-110', 'node_ip': '172.31.43.110', 'config': {}}
    # {'epoch': 0, '_timestamp': 1656349412, '_time_this_iter_s': 0.0026497840881347656, '_training_iteration': 1, 'time_this_iter_s': 3.433483362197876, 'done': False, 'timesteps_total': None, 'episodes_total': None, 'training_iteration': 1, 'trial_id': '0f1d0_00000', 'experiment_id': '494a1d050b4a4d11aeabd87ba475fcd3', 'date': '2022-06-27_17-03-32', 'timestamp': 1656349412, 'time_total_s': 3.433483362197876, 'pid': 23018, 'hostname': 'ip-172-31-43-110', 'node_ip': '172.31.43.110', 'config': {}, 'time_since_restore': 3.433483362197876, 'timesteps_since_restore': 0, 'iterations_since_restore': 1, 'warmup_time': 0.003779172897338867, 'experiment_tag': '0'}
    # {'epoch': 1, '_timestamp': 1656349412, '_time_this_iter_s': 0.0013833045959472656, '_training_iteration': 2, 'time_this_iter_s': 0.016670703887939453, 'done': False, 'timesteps_total': None, 'episodes_total': None, 'training_iteration': 2, 'trial_id': '0f1d0_00000', 'experiment_id': '494a1d050b4a4d11aeabd87ba475fcd3', 'date': '2022-06-27_17-03-32', 'timestamp': 1656349412, 'time_total_s': 3.4501540660858154, 'pid': 23018, 'hostname': 'ip-172-31-43-110', 'node_ip': '172.31.43.110', 'config': {}, 'time_since_restore': 3.4501540660858154, 'timesteps_since_restore': 0, 'iterations_since_restore': 2, 'warmup_time': 0.003779172897338867, 'experiment_tag': '0'}




