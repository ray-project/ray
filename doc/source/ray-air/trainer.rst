.. _air-trainers:

Ray AIR Trainers
================

.. https://docs.google.com/drawings/d/1anmT0JVFH9abR5wX5_WcxNHJh6jWeDL49zWxGpkfORA/edit

.. image:: images/train.svg


Ray AIR Trainers provide a way to scale out training with popular machine learning frameworks

As part of Ray Train, Trainers provide a seamless abstraction for running distributed multi-node training with fault tolerance.

Ray AIR Trainers also integrate with the rest of the Ray ecosystem. Trainers leverage Ray Data to enable scalable preprocessing
and performant distributed data ingestion. After executing training, Trainers output the trained model in the form of
a :class:`checkpoint <ray.air.Checkpoint>`, which can be used for batch or online prediction inference. Trainers
also can be composed with Tuners.

There are three broad categories of Trainers that AIR offers:

* Deep Learning Trainers (Pytorch, Tensorflow, Horovod)
* Tree-based Trainers (XGboost, LightGBM)
* Other ML frameworks (HuggingFace, Scikit-Learn, RLlib)

Trainer Basics
--------------

All trainers inherit from the :class:`BaseTrainer <ray.air.base_trainer.BaseTrainer>` interface. To
construct a Trainer, you can provide:

* A `scaling_config`, which specifies how many parallel training workers and what type of resources (cpus/gpus) to use per worker during training.
* A `run_config`, which configures a variety of runtime parameters such as fault tolerance, logging, and callbacks.
* A collection of `datasets` and a `preprocessor` for the provided dataset, which configures preprocessing and the datasets to ingest from.
* `resume_from_checkpoint`, which is a checkpoint path to resume from, should your training run be interrupted.

After construction, you can invoke a trainer by calling `Trainer.fit()`.

.. literalinclude:: doc_code/xgboost_trainer.py
    :language: python

Deep Learning Trainers
----------------------

Ray Train offers 3 main deep learning trainers:
:class:`TorchTrainer <ray.train.torch.TorchTrainer>`,
:class:`TensorflowTrainer <ray.train.tensorflow.TensorflowTrainer>`, and
:class:`HorovodTrainer <ray.train.horovod.HorovodTrainer>`.

These three trainers all take a ``train_loop_per_worker`` parameter, which is a function that defines
the main training logic that runs on each training worker.

Under the hood, Ray AIR will use the provided ``scaling_config`` to instantiate
the correct number of workers.

If provided a dataset, the dataset will be automatically
sharded across all the workers such that each worker gets a different
set of data. You can access the data shard within a worker via ``session.get_dataset_shard()``.
You can read more about :ref:`data ingest <air-ingest>` here.

Read more about :ref:`Ray Train's Deep Learning Trainers <train-user-guide>`.

.. tabbed:: Torch

    .. literalinclude:: doc_code/torch_trainer.py
        :language: python

.. tabbed:: Tensorflow

    .. literalinclude:: doc_code/tf_starter.py
        :language: python
        :start-after: __air_tf_train_start__
        :end-before: __air_tf_train_end__

.. tabbed:: Horovod

    .. literalinclude:: doc_code/hvd_trainer.py
        :language: python


How to report metrics and checkpoints?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One often needs to report metrics and save checkpoints for fault tolerance or future reference.
This can be challenging in a distributed environment, where the calculation of metrics and
the generation of checkpoints are spread out across multiple nodes in a cluster.

Use the :ref:`Session <air-session-ref>` API to gather metrics and register checkpoints.
Registered checkpoints are synced to driver or the cloud storage based on user's configurations,
as specified in `Trainer(run_config=...)`.

.. literalinclude:: doc_code/report_metrics_and_save_checkpoints.py
    :language: python
    :start-after: __air_session_start__
    :end-before: __air_session_end__

Tree-based Trainers
-------------------

Ray Train offers 2 main tree-based trainers:
:class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>` and
:class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`.


XGBoost Trainer
~~~~~~~~~~~~~~~

Ray AIR also provides an easy to use :class:`XGBoostTrainer  <ray.train.xgboost.XGBoostTrainer>`
for training xgboost models at scale.

To use this trainer, you will need to first run: ``pip install -U xgboost-ray``.

.. literalinclude:: doc_code/xgboost_trainer.py
    :language: python

LightGBMTrainer
~~~~~~~~~~~~~~~

TODO

Other Trainers
--------------

HuggingFace Trainer
~~~~~~~~~~~~~~~~~~~

HuggingFaceTrainer further extends TorchTrainer. The main logic is inside ``trainer_init_per_worker``.

.. literalinclude:: doc_code/hf_trainer.py
    :language: python
    :start-after: __hf_trainer_start__
    :end-before: __hf_trainer_end__


Scikit-learn Trainer
~~~~~~~~~~~~~~~~~~~~~

.. note:: This trainer is not distributed.

The Scikit-Learn Trainer is a thin wrapper for one to launch scikit-learn training within Ray AIR.
It is not distributed but can still benefit from integrating with Ray Tune and batch/online prediction.

.. literalinclude:: doc_code/sklearn_trainer.py
    :language: python


RLlib Trainer
~~~~~~~~~~~~~

RLTrainer provides an interface to RL Trainables.

TODO

.. literalinclude:: doc_code/rl_trainer.py
    :language: python


How to interprete training results?
-----------------------------------

After specifying Trainer, one can then kick off training by simply calling ``fit()``.
The following is how you can interact with training result:

.. code-block:: python

    result = trainer.fit()


- ``result.checkpoint`` gives last saved checkpoint
- ``result.best_checkpoints`` gives N best saved checkpoints, as configured in ``RunConfig.CheckpointConfig``.
- ``result.metrics`` gives the final metrics as reported.
