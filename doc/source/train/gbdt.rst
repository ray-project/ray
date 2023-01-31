.. _train-gbdt-guide:

XGBoost & LightGBM User Guide for Ray Train
===========================================

Ray Train has built-in support for XGBoost and LightGBM.

Basic Training with Tree-Based Models in Train
----------------------------------------------

Just as in the original `xgboost.train() <https://xgboost.readthedocs.io/en/stable/parameter.html>`__ and
`lightgbm.train() <https://lightgbm.readthedocs.io/en/latest/Parameters.html>`__ functions, the
training parameters are passed as the ``params`` dictionary.

.. tabbed:: XGBoost

    Run ``pip install -U xgboost_ray``.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __xgboost_start__
        :end-before: __xgboost_end__

.. tabbed:: LightGBM

    Run ``pip install -U lightgbm_ray``.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __lightgbm_start__
        :end-before: __lightgbm_end__


Ray-specific params are passed in through the trainer constructors.

How to scale out training?
--------------------------
The benefit of using Ray AIR is that you can seamlessly scale up your training by
adjusting the :class:`ScalingConfig <ray.air.config.ScalingConfig>`.

.. note::
    Ray Train does not modify or otherwise alter the working
    of the underlying XGBoost / LightGBM distributed training algorithms.
    Ray only provides orchestration, data ingest and fault tolerance.
    For more information on GBDT distributed training, refer to
    `XGBoost documentation <https://xgboost.readthedocs.io>`__ and
    `LightGBM documentation <https://lightgbm.readthedocs.io/>`__.


Here are some examples for common use-cases:


.. tabbed:: Multi-node CPU

    Setup: 4 nodes with 8 CPUs each.

    Use-case: To utilize all resources in multi-node training.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __scaling_cpu_start__
        :end-before: __scaling_cpu_end__

    Note that we pass 0 CPUs for the trainer resources, so that all resources can
    be allocated to the actual distributed training workers.


.. tabbed:: Single-node multi-GPU

    Setup: 1 node with 8 CPUs and 4 GPUs.

    Use-case: If you have a single node with multiple GPUs, you need to use
    distributed training to leverage all GPUs.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __scaling_gpu_start__
        :end-before: __scaling_gpu_end__

.. tabbed:: Multi-node multi-GPU

    Setup: 4 node with 8 CPUs and 4 GPUs each.

    Use-case: If you have a multiple nodes with multiple GPUs, you need to
    schedule one worker per GPU.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __scaling_gpumulti_start__
        :end-before: __scaling_gpumulti_end__

    Note that you just have to adjust the number of workers - everything else
    will be handled by Ray automatically.


How many remote actors should I use?
------------------------------------

This depends on your workload and your cluster setup.
Generally there is no inherent benefit of running more than
one remote actor per node for CPU-only training. This is because
XGBoost can already leverage multiple CPUs via threading.

However, there are some cases when you should consider starting
more than one actor per node:

* For **multi GPU training**, each GPU should have a separate
  remote actor. Thus, if your machine has 24 CPUs and 4 GPUs,
  you will want to start 4 remote actors with 6 CPUs and 1 GPU
  each
* In a **heterogeneous cluster** , you might want to find the
  `greatest common divisor <https://en.wikipedia.org/wiki/Greatest_common_divisor>`_
  for the number of CPUs.
  E.g. for a cluster with three nodes of 4, 8, and 12 CPUs, respectively,
  you should set the number of actors to 6 and the CPUs per
  actor to 4.

How to use GPUs for training?
-----------------------------

Ray AIR enables multi GPU training for XGBoost and LightGBM. The core backends
will automatically leverage NCCL2 for cross-device communication.
All you have to do is to start one actor per GPU and set GPU-compatible parameters,
e.g. XGBoost's ``tree_method`` to ``gpu_hist`` (see XGBoost
documentation for more details.)

For instance, if you have 2 machines with 4 GPUs each, you will want
to start 8 workers, and set ``use_gpu=True``. There is usually
no benefit in allocating less (e.g. 0.5) or more than one GPU per actor.

You should divide the CPUs evenly across actors per machine, so if your
machines have 16 CPUs in addition to the 4 GPUs, each actor should have
4 CPUs to use.


.. literalinclude:: doc_code/gbdt_user_guide.py
    :language: python
    :start-after: __gpu_xgboost_start__
    :end-before: __gpu_xgboost_end__


How to optimize XGBoost memory usage?
-------------------------------------

XGBoost uses a compute-optimized datastructure, the ``DMatrix``,
to hold training data. When converting a dataset to a ``DMatrix``,
XGBoost creates intermediate copies and ends up
holding a complete copy of the full data. The data will be converted
into the local dataformat (on a 64 bit system these are 64 bit floats.)
Depending on the system and original dataset dtype, this matrix can
thus occupy more memory than the original dataset.

The **peak memory usage** for CPU-based training is at least
**3x** the dataset size (assuming dtype ``float32`` on a 64bit system)
plus about **400,000 KiB** for other resources,
like operating system requirements and storing of intermediate
results.

**Example**


* Machine type: AWS m5.xlarge (4 vCPUs, 16 GiB RAM)
* Usable RAM: ~15,350,000 KiB
* Dataset: 1,250,000 rows with 1024 features, dtype float32.
  Total size: 5,000,000 KiB
* XGBoost DMatrix size: ~10,000,000 KiB

This dataset will fit exactly on this node for training.

Note that the DMatrix size might be lower on a 32 bit system.

**GPUs**

Generally, the same memory requirements exist for GPU-based
training. Additionally, the GPU must have enough memory
to hold the dataset.

In the example above, the GPU must have at least
10,000,000 KiB (about 9.6 GiB) memory. However,
empirically we found that using a ``DeviceQuantileDMatrix``
seems to show more peak GPU memory usage, possibly
for intermediate storage when loading data (about 10%).

**Best practices**

In order to reduce peak memory usage, consider the following
suggestions:


* Store data as ``float32`` or less. More precision is often
  not needed, and keeping data in a smaller format will
  help reduce peak memory usage for initial data loading.
* Pass the ``dtype`` when loading data from CSV. Otherwise,
  floating point values will be loaded as ``np.float64``
  per default, increasing peak memory usage by 33%.
