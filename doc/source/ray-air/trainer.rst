.. _air-trainers:

Ray AIR Trainers
================

.. https://docs.google.com/drawings/d/1anmT0JVFH9abR5wX5_WcxNHJh6jWeDL49zWxGpkfORA/edit

.. image:: images/train.svg


Ray AIR Trainers provide a way to scale out training with popular machine learning frameworks.

As part of Ray Train, Trainers enable users to run distributed multi-node training with fault tolerance.

Trainers also integrate with the rest of the Ray ecosystem. Trainers leverage :ref:`Ray Data <air-ingest>` to enable scalable preprocessing
and performant distributed data ingestion. After executing training, Trainers output the trained model in the form of
a :class:`Checkpoint <ray.air.checkpoint.Checkpoint>`, which can be used for batch or online prediction inference. Trainers
also can be composed with Tuners.

There are three broad categories of Trainers that AIR offers:

* :ref:`Deep Learning Trainers <air-trainers-dl>` (Pytorch, Tensorflow, Horovod)
* :ref:`Tree-based Trainers <air-trainers-tree>` (XGboost, LightGBM)
* :ref:`Other ML frameworks <air-trainers-other>` (HuggingFace, Scikit-Learn, RLlib)

Trainer Basics
--------------

All trainers inherit from the :class:`BaseTrainer <ray.air.base_trainer.BaseTrainer>` interface. To
construct a Trainer, you can provide:

* A :class:`scaling_config <ray.air.config.ScalingConfig>`, which specifies how many parallel training workers and what type of resources (CPUs/GPUs) to use per worker during training.
* A :class:`run_config <ray.air.config.RunConfig>`, which configures a variety of runtime parameters such as fault tolerance, logging, and callbacks.
* A collection of :ref:`datasets <air-ingest>` and a :ref:`preprocessor <air-preprocessors>` for the provided dataset, which configures preprocessing and the datasets to ingest from.
* `resume_from_checkpoint`, which is a checkpoint path to resume from, should your training run be interrupted.

**Note about datasets:** If the ``datasets`` dict contains a training dataset (denoted by
the "train" key), then it will be split into multiple dataset
shards, with each worker training on a single shard. All the other datasets will not be split.

After construction, you can invoke a trainer by calling :meth:`Trainer.fit() <ray.train.trainer.Trainer.fit>`.

.. literalinclude:: doc_code/xgboost_trainer.py
    :language: python

.. _air-trainers-dl:

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

.. dropdown:: Code examples

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

A common use case is to collect training metrics and save checkpoints for fault tolerance during
training or downstream processing (e.g. serving the model).
This can be challenging in a distributed environment, where the calculation of metrics and
the generation of checkpoints are spread out across multiple nodes in a cluster.

Use the :ref:`Session <air-session-ref>` API to gather metrics and register checkpoints.
Registered checkpoints are synced to driver or the cloud storage based on user's configurations,
as specified in ``Trainer(run_config=...)``.

.. dropdown:: Code example

    .. literalinclude:: doc_code/report_metrics_and_save_checkpoints.py
        :language: python
        :start-after: __air_session_start__
        :end-before: __air_session_end__

.. _air-trainers-tree:

Tree-based Trainers
-------------------

Ray Train offers 2 main tree-based trainers:
:class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>` and
:class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`.

See :ref:`here for a more detailed user-guide <air-trainers-gbdt-user-guide>`.


XGBoost Trainer
~~~~~~~~~~~~~~~

Ray AIR also provides an easy to use :class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>`
for training XGBoost models at scale.

To use this trainer, you will need to first run: ``pip install -U xgboost-ray``.

.. literalinclude:: doc_code/xgboost_trainer.py
    :language: python

LightGBMTrainer
~~~~~~~~~~~~~~~

Similarly, Ray AIR comes with a :class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`
for training LightGBM models at scale.

To use this trainer, you will need to first run ``pip install -U lightgbm-ray``.

.. literalinclude:: doc_code/lightgbm_trainer.py
    :language: python

.. _air-trainers-other:

Other Trainers
--------------

HuggingFace Trainer
~~~~~~~~~~~~~~~~~~~

:class:`HuggingFaceTrainer <ray.train.huggingface.HuggingFaceTrainer>` further extends :class:`TorchTrainer <ray.train.torch.TorchTrainer>`, built
for interoperability with the HuggingFace Transformers library.

Users are required to provide a ``trainer_init_per_worker`` function which returns a
``transformers.Trainer`` object. The ``trainer_init_per_worker`` function
will have access to preprocessed train and evaluation datasets.

Upon calling `HuggingFaceTrainer.fit()`, multiple workers (ray actors) will be spawned,
and each worker will create its own copy of a ``transformers.Trainer``.

Each worker will then invoke ``transformers.Trainer.train()``, which will perform distributed
training via Pytorch DDP.


.. dropdown:: Code example

    .. literalinclude:: doc_code/hf_trainer.py
        :language: python
        :start-after: __hf_trainer_start__
        :end-before: __hf_trainer_end__


Scikit-Learn Trainer
~~~~~~~~~~~~~~~~~~~~

.. note:: This trainer is not distributed.

The Scikit-Learn Trainer is a thin wrapper for one to launch scikit-learn training within Ray AIR.
It is not distributed but can still benefit from integrating with Ray Tune and batch/online prediction.

.. literalinclude:: doc_code/sklearn_trainer.py
    :language: python


RLlib Trainer
~~~~~~~~~~~~~

RLTrainer provides an interface to RL Trainables. This enables you to use the same abstractions
as in the other trainers to define the scaling behavior, and to use Ray Data for offline training.

Please note that some scaling behavior still has to be defined separately.
The :class:`scaling_config <ray.air.config.ScalingConfig>` will set the number of
training workers ("Rollout workers"). To set the number of e.g. evaluation workers, you will
have to specify this in the ``config`` parameter of the ``RLTrainer``:

.. literalinclude:: doc_code/rl_trainer.py
    :language: python


How to interpret training results?
----------------------------------

After specifying Trainer, one can then kick off training by simply calling ``fit()``.
The following is how you can interact with training result:

.. code-block:: python

    result = trainer.fit()


- ``result.checkpoint`` gives last saved checkpoint.
- ``result.best_checkpoints`` gives N best saved checkpoints, as configured in ``RunConfig.CheckpointConfig``.
- ``result.metrics`` gives the final metrics as reported.
- ``result.error`` will contain an Exception if training failed.

See :class:`the Result class <ray.air.result.Result>` for more details.
