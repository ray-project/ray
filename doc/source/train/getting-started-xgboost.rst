.. _train-xgboost:

Get Started with Distributed Training using XGBoost
===================================================

This tutorial walks through the process of converting an existing XGBoost script to use Ray Train.

Learn how to:

1. Configure a :ref:`training function <train-overview-training-function>` to report metrics and save checkpoints.
2. Configure :ref:`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for a training job.
3. Launch a distributed training job with a :class:`~ray.train.xgboost.XGBoostTrainer`.

Quickstart
----------

For reference, the final code will look something like this:

.. testcode::
    :skipif: True

    import ray.train
    from ray.train.xgboost import XGBoostTrainer

    def train_func():
        # Your XGBoost training code here.
        ...

    scaling_config = ray.train.ScalingConfig(num_workers=2, resources_per_worker={"CPU": 4})
    trainer = XGBoostTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. `train_func` is the Python code that executes on each distributed training worker.
2. :class:`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
3. :class:`~ray.train.xgboost.XGBoostTrainer` launches the distributed training job.

Compare a XGBoost training script with and without Ray Train.

.. tab-set::

    .. tab-item:: XGBoost + Ray Train

        .. literalinclude:: ./doc_code/xgboost_quickstart.py
            :emphasize-lines: 3-4, 7-8, 11, 15-16, 19-20, 48, 53, 56-64
            :language: python
            :start-after: __xgboost_ray_start__
            :end-before: __xgboost_ray_end__

    .. tab-item:: XGBoost

        .. literalinclude:: ./doc_code/xgboost_quickstart.py
            :language: python
            :start-after: __xgboost_start__
            :end-before: __xgboost_end__


Set up a training function
--------------------------

First, update your training code to support distributed training.
Begin by wrapping your `native <https://xgboost.readthedocs.io/en/latest/python/python_intro.html>`_ 
or `scikit-learn estimator <https://xgboost.readthedocs.io/en/latest/python/sklearn_estimator.html>`_ 
XGBoost training code in a :ref:`training function <train-overview-training-function>`:

.. testcode::
    :skipif: True

    def train_func():
        # Your native XGBoost training code here.
        dmatrix = ...
        xgboost.train(...)

Each distributed training worker executes this function.

You can also specify the input argument for `train_func` as a dictionary via the Trainer's `train_loop_config`. For example:

.. testcode:: python
    :skipif: True

    def train_func(config):
        label_column = config["label_column"]
        num_boost_round = config["num_boost_round"]
        ...

    config = {"label_column": "y", "num_boost_round": 10}
    trainer = ray.train.xgboost.XGBoostTrainer(train_func, train_loop_config=config, ...)

.. warning::

    Avoid passing large data objects through `train_loop_config` to reduce the
    serialization and deserialization overhead. Instead,
    initialize large objects (e.g. datasets, models) directly in `train_func`.

    .. code-block:: diff

         def load_dataset():
             # Return a large in-memory dataset
             ...

         def load_model():
             # Return a large in-memory model instance
             ...

        -config = {"data": load_dataset(), "model": load_model()}

         def train_func(config):
        -    data = config["data"]
        -    model = config["model"]

        +    data = load_dataset()
        +    model = load_model()
             ...

         trainer = ray.train.xgboost.XGBoostTrainer(train_func, train_loop_config=config, ...)

Ray Train automatically performs the worker communication setup that is needed to do distributed xgboost training.

Report metrics and save checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To persist your checkpoints and monitor training progress, add a
:class:`ray.train.xgboost.RayTrainReportCallback` utility callback to your Trainer:


.. testcode:: python
    :skipif: True

    import xgboost
    from ray.train.xgboost import RayTrainReportCallback

    def train_func():
        ...
        bst = xgboost.train(
            ...,
            callbacks=[
                RayTrainReportCallback(
                    metrics=["eval-logloss"], frequency=1
                )
            ],
        )
        ...


Reporting metrics and checkpoints to Ray Train enables :ref:`fault-tolerant training <train-fault-tolerance>` and the integration with Ray Tune.

Loading data
------------

When running distributed XGBoost training, each worker should use a different shard of the dataset.


.. testcode:: python
    :skipif: True

    def get_train_dataset(world_rank: int) -> xgboost.DMatrix:
        # Define logic to get the DMatrix shard for this worker rank
        ...

    def get_eval_dataset(world_rank: int) -> xgboost.DMatrix:
        # Define logic to get the DMatrix for each worker
        ...

    def train_func():
        rank = ray.train.get_world_rank()
        dtrain = get_train_dataset(rank)
        deval = get_eval_dataset(rank)
        ...

A common way to do this is to pre-shard the dataset and then assign each worker a different set of files to read.

Pre-sharding the dataset is not very flexible to changes in the number of workers, since some workers may be assigned more data than others. For more flexibility, Ray Data provides a solution for sharding the dataset at runtime.

Use Ray Data to shard the dataset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Ray Data <data>` is a distributed data processing library that allows you to easily shard and distribute your data across multiple workers. 

First, load your **entire** dataset as a Ray Data Dataset. 
Reference the :ref:`Ray Data Quickstart <data_quickstart>` for more details on how to load and preprocess data from different sources.

.. testcode:: python
    :skipif: True

    train_dataset = ray.data.read_parquet("s3://path/to/entire/train/dataset/dir")
    eval_dataset = ray.data.read_parquet("s3://path/to/entire/eval/dataset/dir")

In the training function, you can access the dataset shards for this worker using :meth:`ray.train.get_dataset_shard`. 
Convert this into a native `xgboost.DMatrix <https://xgboost.readthedocs.io/en/stable/python/python_api.html#xgboost.DMatrix>`_.


.. testcode:: python
    :skipif: True

    def get_dmatrix(dataset_name: str) -> xgboost.DMatrix:
        shard = ray.train.get_dataset_shard(dataset_name)
        df = shard.materialize().to_pandas()
        X, y = df.drop("target", axis=1), df["target"]
        return xgboost.DMatrix(X, label=y)

    def train_func():
        dtrain = get_dmatrix("train")
        deval = get_dmatrix("eval")
        ...


Finally, pass the dataset to the Trainer. This will automatically shard the dataset across the workers. These keys must match the keys used when calling ``get_dataset_shard`` in the training function.


.. testcode:: python
    :skipif: True

    trainer = XGBoostTrainer(..., datasets={"train": train_dataset, "eval": eval_dataset})
    trainer.fit()


For more details, see :ref:`data-ingest-torch`.

Configure scale and GPUs
------------------------

Outside of your training function, create a :class:`~ray.train.ScalingConfig` object to configure:

1. :class:`num_workers <ray.train.ScalingConfig>` - The number of distributed training worker processes.
2. :class:`use_gpu <ray.train.ScalingConfig>` - Whether each worker should use a GPU (or CPU).
3. :class:`resources_per_worker <ray.train.ScalingConfig>` - The number of CPUs or GPUs per worker.

.. testcode::

    from ray.train import ScalingConfig
    
    # 4 nodes with 8 CPUs each.
    scaling_config = ScalingConfig(num_workers=4, resources_per_worker={"CPU": 8})

.. note::
    When using Ray Data with Ray Train, be careful not to request all available CPUs in your cluster with the `resources_per_worker` parameter. 
    Ray Data needs CPU resources to execute data preprocessing operations in parallel. 
    If all CPUs are allocated to training workers, Ray Data operations may be bottlenecked, leading to reduced performance. 
    A good practice is to leave some portion of CPU resources available for Ray Data operations.

    For example, if your cluster has 8 CPUs per node, you might allocate 6 CPUs to training workers and leave 2 CPUs for Ray Data:

    .. testcode::

        # Allocate 6 CPUs per worker, leaving resources for Ray Data operations
        scaling_config = ScalingConfig(num_workers=4, resources_per_worker={"CPU": 6})


In order to use GPUs, you will need to set the `use_gpu` parameter to `True` in your :class:`~ray.train.ScalingConfig` object.
This will request and assign a single GPU per worker.

.. testcode::
    # 1 node with 8 CPUs and 4 GPUs each.
    scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

    # 4 nodes with 8 CPUs and 4 GPUs each.
    scaling_config = ScalingConfig(num_workers=16, use_gpu=True)

When using GPUs, you will also need to update your training function to use the assigned GPU. 
This can be done by setting the `"device"` parameter as `"cuda"`. 
For more details on XGBoost's GPU support, see the `XGBoost GPU documentation <https://xgboost.readthedocs.io/en/stable/gpu/index.html>`__.

.. code-block:: diff

    def train_func():
        ...

        params = {
            ...,
  +         "device": "cuda",
        }

        bst = xgboost.train(
            params,
            ...
        )


Configure persistent storage
----------------------------

Create a :class:`~ray.train.RunConfig` object to specify the path where results
(including checkpoints and artifacts) will be saved.

.. testcode::

    from ray.train import RunConfig

    # Local path (/some/local/path/unique_run_name)
    run_config = RunConfig(storage_path="/some/local/path", name="unique_run_name")

    # Shared cloud storage URI (s3://bucket/unique_run_name)
    run_config = RunConfig(storage_path="s3://bucket", name="unique_run_name")

    # Shared NFS path (/mnt/nfs/unique_run_name)
    run_config = RunConfig(storage_path="/mnt/nfs", name="unique_run_name")


.. warning::

    Specifying a *shared storage location* (such as cloud storage or NFS) is
    *optional* for single-node clusters, but it is **required for multi-node clusters.**
    Using a local path will :ref:`raise an error <multinode-local-storage-warning>`
    during checkpointing for multi-node clusters.


For more details, see :ref:`persistent-storage-guide`.


Launch a training job
---------------------

Tying this all together, you can now launch a distributed training job
with a :class:`~ray.train.xgboost.XGBoostTrainer`.

.. testcode::
    :hide:

    from ray.train import ScalingConfig

    train_func = lambda: None
    scaling_config = ScalingConfig(num_workers=1)
    run_config = None

.. testcode::

    from ray.train.xgboost import XGBoostTrainer

    trainer = XGBoostTrainer(
        train_func, scaling_config=scaling_config, run_config=run_config
    )
    result = trainer.fit()


Access training results
-----------------------

After training completes, a :class:`~ray.train.Result` object is returned which contains
information about the training run, including the metrics and checkpoints reported during training.

.. testcode::

    result.metrics     # The metrics reported during training.
    result.checkpoint  # The latest checkpoint reported during training.
    result.path        # The path where logs are stored.
    result.error       # The exception that was raised, if training failed.

For more usage examples, see :ref:`train-inspect-results`.


Next steps
----------

After you have converted your XGBoost training script to use Ray Train:

* See :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :doc:`Examples <examples>` for end-to-end examples of how to use Ray Train.
* Consult the :ref:`API Reference <train-api>` for more details on the classes and methods from this tutorial.