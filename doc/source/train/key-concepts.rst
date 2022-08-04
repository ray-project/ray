.. _train-key-concepts:

Key Concepts
============

Ray Train mainly revolves around the ``Trainer`` concept.


.. note::

    This page explains Ray Train concepts and trainers in more detail.
    For a quick overview of the available trainers, take a look at the
    :ref:`Ray AIR trainers <air-trainers>` page in the Ray AIR documentation.


Trainer
-------
The ``Trainer`` concept is the centerpiece of Ray Train.
A Trainer objects holds all the configuration and state to execute a (distributed) training run.
It can be configured with general setting that are applicable to all trainers (such as the
:class:`RunConfig <ray.air.config.run_config>` and the :class:`ScalingConfig <ray.air.config.scaling_config>`),
and with trainer-specific options.

Currently there are three broad categories of Trainers:

* :ref:`Deep Learning Trainers <train-key-concepts-dl-trainers>` (Pytorch, Tensorflow, Horovod)
* :ref:`Tree-based Trainers <train-key-concepts-tree>` (XGboost, LightGBM)
* :ref:`Other ML frameworks <train-key-concepts-other>` (HuggingFace, Scikit-Learn, RLlib)

.. _train-key-concepts-dl-trainers:

Deep learning trainers
~~~~~~~~~~~~~~~~~~~~~~
These trainers utilize deep learning frameworks such as PyTorch, Tensorflow, or Horovod
for training.

For these trainers, you usually define your own training function. Please see the
:ref:`Session <train-key-concepts-session>` section for details on how to interact with
Ray Train in these functions.

- :ref:`Deep learning user guide <train-dl-guide>`
- :ref:`Quick overview of deep-learning trainers in the Ray AIR documentation <air-trainers-dl>`

.. _train-key-concepts-tree:

Tree-based trainers
~~~~~~~~~~~~~~~~~~~~~~
These trainers utilize gradient-based decision trees for training. The most popular libraries
for this are XGBoost and LightGBM.

For these trainers, you just pass a dataset and parameters. Distributed training will be started
automatically.

- :ref:`XGBoost/LightGBM user guide <train-gbdt-guide>`
- :ref:`Quick overview of tree-based trainers in the Ray AIR documentation <air-trainers-tree>`

.. _train-key-concepts-other:

Other trainers
~~~~~~~~~~~~~~
Some trainers don't fit into the above categories, such as the
:class:`Huggingface trainer <ray.train.huggingface.HuggingfaceTrainer>` for NLP,
the :class:`RL trainer <ray.train.rl.RLTrainer>` for reinforcement learning, and
the :class:`SKlearn trainer <ray.train.sklearn.SKlearnTrainer>` for (non-distributed) training of
SKlearn models.

- :ref:`Quick overview of other trainers in the Ray AIR documentation <air-trainers-other>`


Overview of Trainers
--------------------

All trainers inherit from the :class:`BaseTrainer <ray.train.base_trainer.BaseTrainer>` interface. To
construct a Trainer, you can provide:

* A :class:`scaling_config <ray.air.config.ScalingConfig>`, which specifies how many parallel training workers and what type of resources (CPUs/GPUs) to use per worker during training.
* A :class:`run_config <ray.air.config.RunConfig>`, which configures a variety of runtime parameters such as fault tolerance, logging, and callbacks.
* A collection of :ref:`datasets <air-ingest>` and a :ref:`preprocessor <air-preprocessors>` for the provided datasets, which configures preprocessing and the datasets to ingest from.
* ``resume_from_checkpoint``, which is a checkpoint path to resume from, should your training run be interrupted.

After instantiating a Trainer, you can invoke it by calling :meth:`Trainer.fit() <ray.air.Trainer.fit>`.

.. literalinclude:: /ray-air/doc_code/xgboost_trainer.py
    :language: python


.. _train-key-concepts-config:
Configuration
-------------


.. _train-key-concepts-session:

Session
-------
The session object is used to interact with the Ray Train training run from within your custom functions.

For instance, a :class:`DataParallelTrainer <ray.train.data_parallel_trainer.DataParallelTrainer>` accepts
a ``train_loop_per_worker`` argument.
In this train loop, you can use :func:`session.report() <ray.air.session.report>` to report
training results or checkpoints to Ray Train.



.. tabbed::  Reporting results

    To report results, use :func:`session.report() <ray.air.session.report>` as in this example:

        .. literalinclude:: doc_code/key_concepts.py
            :language: python
            :start-after: __session_report_start__
            :end-before: __session_report_end__

.. tabbed::  Load and save checkpoints

    To load checkpoints, use :func:`session.get_checkpoint() <ray.air.session.get_checkpoint>`.

    To save checkpoints, pass them to :func:`session.report() <ray.air.session.report>`.

        .. literalinclude:: doc_code/key_concepts.py
            :language: python
            :start-after: __session_checkpoint_start__
            :end-before: __session_checkpoint_end__

.. tabbed::  Get dataset shard and worker info

    To get a sharded dataset in a data-parallel trainer, use :func:`session.get_dataset_shard() <ray.air.session.get_dataset_shard>`.

    To get information about the local/global rank and world sizes, see this example:

        .. literalinclude:: doc_code/key_concepts.py
            :language: python
            :start-after: __session_data_info_start__
            :end-before: __session_data_info_end__


Results
-------
