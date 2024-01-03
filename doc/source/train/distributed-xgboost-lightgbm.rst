.. _train-gbdt-guide:

Get Started with Distributed Training using XGBoost and LightGBM
================================================================

Ray Train has built-in support for XGBoost and LightGBM.


Quickstart
-----------
.. tab-set::

    .. tab-item:: XGBoost

        .. literalinclude:: doc_code/gbdt_user_guide.py
           :language: python
           :start-after: __xgboost_start__
           :end-before: __xgboost_end__

    .. tab-item:: LightGBM

        .. literalinclude:: doc_code/gbdt_user_guide.py
           :language: python
           :start-after: __lightgbm_start__
           :end-before: __lightgbm_end__


Basic training with tree-based models in Train
----------------------------------------------

Just as in the original `xgboost.train() <https://xgboost.readthedocs.io/en/stable/parameter.html>`__ and
`lightgbm.train() <https://lightgbm.readthedocs.io/en/latest/Parameters.html>`__ functions, the
training parameters are passed as the ``params`` dictionary.

.. tab-set::

    .. tab-item:: XGBoost

        Run ``pip install -U xgboost_ray``.

        .. literalinclude:: doc_code/gbdt_user_guide.py
            :language: python
            :start-after: __xgboost_start__
            :end-before: __xgboost_end__

    .. tab-item:: LightGBM

        Run ``pip install -U lightgbm_ray``.

        .. literalinclude:: doc_code/gbdt_user_guide.py
            :language: python
            :start-after: __lightgbm_start__
            :end-before: __lightgbm_end__


Trainer constructors pass Ray-specific parameters.


.. _train-gbdt-checkpoints:

Save and load XGBoost and LightGBM checkpoints
----------------------------------------------

When you train a new tree on every boosting round,
you can save a checkpoint to snapshot the training progress so far.
:class:`~ray.train.xgboost.XGBoostTrainer` and :class:`~ray.train.lightgbm.LightGBMTrainer`
both implement checkpointing out of the box. These checkpoints can be loaded into memory
using static methods :meth:`XGBoostTrainer.get_model <ray.train.xgboost.XGBoostTrainer.get_model>` and
:meth:`LightGBMTrainer.get_model <ray.train.lightgbm.LightGBMTrainer.get_model>`.

The only required change is to configure :class:`~ray.train.CheckpointConfig` to set
the checkpointing frequency. For example, the following configuration
saves a checkpoint on every boosting round and only keeps the latest checkpoint:

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_ckpt_freq_start__
    :end-before: __checkpoint_config_ckpt_freq_end__

.. tip::

    Once you enable checkpointing, you can follow :ref:`this guide <train-fault-tolerance>`
    to enable fault tolerance.


How to scale out training?
--------------------------

The benefit of using Ray Train is that you can seamlessly scale up your training by
adjusting the :class:`ScalingConfig <ray.train.ScalingConfig>`.

.. note::
    Ray Train doesn't modify or otherwise alter the working
    of the underlying XGBoost or LightGBM distributed training algorithms.
    Ray only provides orchestration, data ingest and fault tolerance.
    For more information on GBDT distributed training, refer to
    `XGBoost documentation <https://xgboost.readthedocs.io>`__ and
    `LightGBM documentation <https://lightgbm.readthedocs.io/>`__.


Following are some examples of common use-cases:

.. tab-set::

    .. tab-item:: Multi-node CPU

        Setup: 4 nodes with 8 CPUs each.

        Use-case: To utilize all resources in multi-node training.

        .. literalinclude:: doc_code/gbdt_user_guide.py
            :language: python
            :start-after: __scaling_cpu_start__
            :end-before: __scaling_cpu_end__


    .. tab-item:: Single-node multi-GPU

        Setup: 1 node with 8 CPUs and 4 GPUs.

        Use-case: If you have a single node with multiple GPUs, you need to use
        distributed training to leverage all GPUs.

        .. literalinclude:: doc_code/gbdt_user_guide.py
            :language: python
            :start-after: __scaling_gpu_start__
            :end-before: __scaling_gpu_end__

    .. tab-item:: Multi-node multi-GPU

        Setup: 4 node with 8 CPUs and 4 GPUs each.

        Use-case: If you have a multiple nodes with multiple GPUs, you need to
        schedule one worker per GPU.

        .. literalinclude:: doc_code/gbdt_user_guide.py
            :language: python
            :start-after: __scaling_gpumulti_start__
            :end-before: __scaling_gpumulti_end__

        Note that you just have to adjust the number of workers. Ray handles everything else
        automatically.


.. warning::

    Specifying a *shared storage location* (such as cloud storage or NFS) is
    *optional* for single-node clusters, but it is **required for multi-node clusters.**
    Using a local path will :ref:`raise an error <multinode-local-storage-warning>`
    during checkpointing for multi-node clusters.

    .. testcode:: python
        :skipif: True

        trainer = XGBoostTrainer(
            ..., run_config=ray.train.RunConfig(storage_path="s3://...")
        )


How many remote actors should you use?
--------------------------------------

This depends on your workload and your cluster setup.
Generally there is no inherent benefit of running more than
one remote actor per node for CPU-only training. This is because
XGBoost can already leverage multiple CPUs with threading.

However, in some cases, you should consider some starting
more than one actor per node:

* For **multi GPU training**, each GPU should have a separate
  remote actor. Thus, if your machine has 24 CPUs and 4 GPUs,
  you want to start 4 remote actors with 6 CPUs and 1 GPU
  each
* In a **heterogeneous cluster** , you might want to find the
  `greatest common divisor <https://en.wikipedia.org/wiki/Greatest_common_divisor>`_
  for the number of CPUs.
  For example, for a cluster with three nodes of 4, 8, and 12 CPUs, respectively,
  you should set the number of actors to 6 and the CPUs per
  actor to 4.

How to use GPUs for training?
-----------------------------

Ray Train enables multi-GPU training for XGBoost and LightGBM. The core backends
automatically leverage NCCL2 for cross-device communication.
All you have to do is to start one actor per GPU and set GPU-compatible parameters.
For example, XGBoost's ``tree_method`` to ``gpu_hist``. See XGBoost
documentation for more details.

For instance, if you have 2 machines with 4 GPUs each, you want
to start 8 workers, and set ``use_gpu=True``. There is usually
no benefit in allocating less (for example, 0.5) or more than one GPU per actor.

You should divide the CPUs evenly across actors per machine, so if your
machines have 16 CPUs in addition to the 4 GPUs, each actor should have
4 CPUs to use.


.. literalinclude:: doc_code/gbdt_user_guide.py
    :language: python
    :start-after: __gpu_xgboost_start__
    :end-before: __gpu_xgboost_end__


.. _data-ingest-gbdt:

How to preprocess data for training?
------------------------------------

Particularly for tabular data, Ray Data comes with out-of-the-box :ref:`preprocessors <data-preprocessors>` that implement common feature preprocessing operations.
You can use this with Ray Train Trainers by applying them on the dataset before passing the dataset into a Trainer. For example:


.. literalinclude:: ../data/doc_code/preprocessors.py
    :language: python
    :start-after: __trainer_start__
    :end-before: __trainer_end__


How to optimize XGBoost memory usage?
-------------------------------------

XGBoost uses a compute-optimized datastructure, the ``DMatrix``,
to hold training data. When converting a dataset to a ``DMatrix``,
XGBoost creates intermediate copies and ends up
holding a complete copy of the full data. XGBoost converts the data
into the local data format. On a 64-bit system the format is 64-bit floats.
Depending on the system and original dataset dtype, this matrix can
thus occupy more memory than the original dataset.

The **peak memory usage** for CPU-based training is at least
**3x** the dataset size, assuming dtype ``float32`` on a 64-bit system,
plus about **400,000 KiB** for other resources,
like operating system requirements and storing of intermediate
results.

**Example**


* Machine type: AWS m5.xlarge (4 vCPUs, 16 GiB RAM)
* Usable RAM: ~15,350,000 KiB
* Dataset: 1,250,000 rows with 1024 features, dtype float32.
  Total size: 5,000,000 KiB
* XGBoost DMatrix size: ~10,000,000 KiB

This dataset fits exactly on this node for training.

Note that the DMatrix size might be lower on a 32 bit system.

**GPUs**

Generally, the same memory requirements exist for GPU-based
training. Additionally, the GPU must have enough memory
to hold the dataset.

In the preceding example, the GPU must have at least
10,000,000 KiB (about 9.6 GiB) memory. However,
empirical data shows that using a ``DeviceQuantileDMatrix``
seems to result in more peak GPU memory usage, possibly
for intermediate storage when loading data (about 10%).

**Best practices**

In order to reduce peak memory usage, consider the following
suggestions:


* Store data as ``float32`` or less. You often don't need
  more precision is often, and keeping data in a smaller format
  helps reduce peak memory usage for initial data loading.
* Pass the ``dtype`` when loading data from CSV. Otherwise,
  floating point values are loaded as ``np.float64``
  per default, increasing peak memory usage by 33%.
