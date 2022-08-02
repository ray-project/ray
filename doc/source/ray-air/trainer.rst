.. _air-trainers:

Ray AIR Trainers
================

.. https://docs.google.com/drawings/d/1anmT0JVFH9abR5wX5_WcxNHJh6jWeDL49zWxGpkfORA/edit

.. image:: images/train.svg


Ray AIR offers integration with popular machine learning training framework through a variety of Trainers,
including Pytorch, Tensorflow, Horovod, XGBoost, as well as scikit-learn and HuggingFace. It also offers a RLTrainer
with RLlib integration.

Within Ray AIR, a user can choose different models/frameworks in a composable manner.
Ray AIR trainer is also designed with interoperability in mind. It has built in support for Ray Dataset as
input to fully leverage Ray for data ingestion. It also has ``ray.air.Checkpoint`` for seamless transition
to batch prediction and inference. Moreover, Ray AIR trainer can be readily supplied to Tuner
for hyperparameter tuning with minimal boilerplate code.

Following we will give examples of how to use each type of trainers.

Distributed Deep Learning
-------------------------
Within this category we mainly have TorchTrainer, TensorflowTrainer and HorovodTrainer, they all implement
DataParallelTrainer API.

User needs to supply ``train_loop_per_worker``, which is the main training logic that runs on each training worker.
User also needs to specify ``ScalingConfig`` which determines the number of workers and the resources for each worker.

Under the hood, Ray AIR will start training workers per specification. The input training dataset is automatically
split across all the workers through ``session.get_dataset_shard()``.

.. tabbed:: Torch

    .. literalinclude:: examples/torch_trainer.py
        :language: python

.. tabbed:: Tensorflow

    .. literalinclude:: examples/tf_starter.py
        :language: python
        :start-after: __air_tf_train_start__
        :end-before: __air_tf_train_end__

.. tabbed:: Horovod

    .. literalinclude:: examples/hvd_trainer.py
        :language: python


Report metrics and checkpoint through ``Session``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One often needs to report metrics and save checkpoints for fault tolerance or future reference.
This can be challenging in a distributed environment, where the calculation of metrics and
the generation of checkpoints are spread out across multiple nodes in a cluster.

A user can simply call :ref:`Session <air-session-ref>` API and under the hood,
this API makes sure that metrics are presented in the final training or tuning result. And
checkpoints are synced to driver or the cloud storage based on user's configurations.

Take a look at the following code snippet.

.. literalinclude:: doc_code/report_metrics_and_save_checkpoints.py
    :language: python
    :start-after: __air_session_start__
    :end-before: __air_session_end__


HuggingFace Trainer
~~~~~~~~~~~~~~~~~~~
HuggingFaceTrainer further extends TorchTrainer. The main logic is inside ``trainer_init_per_worker``.
Take a look at the following example.

.. literalinclude:: doc_code/hf_trainer.py
    :language: python
    :start-after: __hf_trainer_start__
    :end-before: __hf_trainer_end__


XGBoost Trainer
---------------
Ray AIR also provides an easy to use XGBoost Trainer for classic machine learning,
which is a wrapper around "Distributed XGBoost on Ray".
See the example below.

.. literalinclude:: doc_code/xgboost_trainer.py
    :language: python


Scikit-learn Trainer
-------------------
scikit-learn Trainer is a thin wrapper for one to launch scikit-learn training within Ray AIR.
**Note:** this is done in a non-distributed fashion. See the example below.

.. literalinclude:: doc_code/sklearn_trainer.py
    :language: python


RLlib Trainer
-------------
RLTrainer provides an interface to RL Trainables.

.. literalinclude:: doc_code/rl_trainer.py
    :language: python


How to interprete training result
---------------------------------
After specifying Trainer, one can then kick off training by simply calling ``fit()``.
The following is how you can interact with training result:

.. code-block:: python

    result = trainer.fit()


- ``result.checkpoint`` gives last saved checkpoint
- ``result.best_checkpoints`` gives N best saved checkpoints, as configured in ``RunConfig.CheckpointConfig``.
- ``result.metrics`` gives the final metrics as reported.
