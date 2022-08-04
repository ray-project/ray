.. _train-key-concepts:

Key Concepts
============

There are three broad categories of Trainers that AIR offers:

* :ref:`Deep Learning Trainers <air-trainers-dl>` (Pytorch, Tensorflow, Horovod)
* :ref:`Tree-based Trainers <air-trainers-tree>` (XGboost, LightGBM)
* :ref:`Other ML frameworks <air-trainers-other>` (HuggingFace, Scikit-Learn, RLlib)

Ray Train offers 3 main deep learning trainers:

* :class:`TorchTrainer <ray.train.torch.TorchTrainer>`
* :class:`TensorflowTrainer <ray.train.tensorflow.TensorflowTrainer>`
* :class:`HorovodTrainer <ray.train.horovod.HorovodTrainer>`

Ray Train also offers 2 tree-based trainers:

* :class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>`
* :class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`

Overview of Trainers
--------------------

All trainers inherit from the :class:`BaseTrainer <ray.train.base_trainer.BaseTrainer>` interface. To
construct a Trainer, you can provide:

* A :class:`scaling_config <ray.air.config.ScalingConfig>`, which specifies how many parallel training workers and what type of resources (CPUs/GPUs) to use per worker during training.
* A :class:`run_config <ray.air.config.RunConfig>`, which configures a variety of runtime parameters such as fault tolerance, logging, and callbacks.
* A collection of :ref:`datasets <air-ingest>` and a :ref:`preprocessor <air-preprocessors>` for the provided datasets, which configures preprocessing and the datasets to ingest from.
* ``resume_from_checkpoint``, which is a checkpoint path to resume from, should your training run be interrupted.

After instatiating a Trainer, you can invoke it by calling :meth:`Trainer.fit() <ray.air.Trainer.fit>`.

.. literalinclude:: /ray-air/doc_code/xgboost_trainer.py
    :language: python


Deep Learning Trainers
----------------------


