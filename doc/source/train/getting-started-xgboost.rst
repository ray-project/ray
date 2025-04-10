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

For reference, the final code is as follows:

.. testcode::
    :skipif: True

    from ray.train.xgboost import XGBoostTrainer
    from ray.train import ScalingConfig

    def train_func():
        # Your XGBoost training code here.
        ...

    scaling_config = ScalingConfig(num_workers=2, resources_per_worker={"CPU": 4})
    trainer = XGBoostTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. `train_func` is the Python code that executes on each distributed training worker.
2. :class:`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
3. :class:`~ray.train.xgboost.XGBoostTrainer` launches the distributed training job.

Compare a XGBoost training script with and without Ray Train.

.. tab-set::

    .. tab-item:: XGBoost

        .. code-block:: python

            import xgboost
            from sklearn.datasets import load_iris
            from sklearn.model_selection import train_test_split

            # 1. Load your data as an `xgboost.DMatrix`.
            data = load_iris(as_frame=True)
            train_X, eval_X, train_y, eval_y = train_test_split(
                data['data'], data['target'], test_size=.2
            )

            dtrain = xgboost.DMatrix(train_X, label=train_y)
            deval = xgboost.DMatrix(eval_X, label=eval_y)

            # 2. Define your xgboost model training parameters.
            params = {
                "tree_method": "approx",
                "objective": "reg:squarederror",
                "eta": 1e-4,
                "subsample": 0.5,
                "max_depth": 2,
            }

            # 3. Do non-distributed training.
            bst = xgboost.train(
                params,
                dtrain=dtrain,
                evals=[(deval, "validation")],
                num_boost_round=10,
            )


    .. tab-item:: XGBoost + Ray Train

        .. code-block:: python
            :emphasize-lines: 5-7, 9, 12-13, 39, 43, 46-47, 50-53, 58-59, 62-66

            from sklearn.datasets import load_iris
            from sklearn.model_selection import train_test_split
            import xgboost

            import ray
            from ray.train import ScalingConfig, RunConfig, CheckpointConfig
            from ray.train.xgboost import XGBoostTrainer

            def train_func():
                # 1. Load your data as an `xgboost.DMatrix`.
                # This will be a Ray Data Dataset shard.
                dataset = ray.train.get_dataset_shard("iris")
                data = dataset.materialize().to_pandas()

                train_X, eval_X, train_y, eval_y = train_test_split(
                    data['data'], data['target'], test_size=.2
                )

                dtrain = xgboost.DMatrix(train_X, label=train_y)
                deval = xgboost.DMatrix(eval_X, label=eval_y)

                # 2. Define your xgboost model training parameters.
                params = {
                    "tree_method": "approx",
                    "objective": "reg:squarederror",
                    "eta": 1e-4,
                    "subsample": 0.5,
                    "max_depth": 2,
                }

                # 3. Do distributed data-parallel training.
                # Ray Train sets up the necessary coordinator processes and
                # environment variables for your workers to communicate with each other.
                bst = xgboost.train(
                    params,
                    dtrain=dtrain,
                    evals=[(deval, "validation")],
                    num_boost_round=10,
                    callbacks=[RayTrainReportCallback(metrics={"loss": "eval-logloss"})],
                )

            # Configure scaling and resource requirements.
            scaling_config = ScalingConfig(num_workers=2, resources_per_worker={"CPU": 4})

            # Load your data as a Ray Data Dataset.
            data = load_iris(as_frame=True)
            dataset = ray.data.from_pandas(data)

            # Launch distributed training job.
            trainer = XGBoostTrainer(
                train_func,
                scaling_config=scaling_config,
                datasets = {"iris": dataset},
                # If running in a multi-node cluster, this is where you
                # should configure the run's persistent storage that is accessible
                # across all worker nodes.
                # run_config=RunConfig(storage_path="s3://..."),
            )
            result = trainer.fit()

            # Load the trained model
            import os
            with result.checkpoint.as_directory() as checkpoint_dir:
                model_path = os.path.join(checkpoint_dir, "xgboost_model.json")
                model = xgb.Booster()
                model.load_model(model_path)


Set up a training function
--------------------------

First, update your training code to support distributed training.
Begin by wrapping your code in a :ref:`training function <train-overview-training-function>`:

.. testcode::
    :skipif: True

    def train_func():
        # Your model training code here.
        ...

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
    serialization and deserialization overhead. Instead, it's preferred to
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

Ray Train automatically sets up the Rabit communicator for XGBoost, which handles the distributed communication between workers.

Report metrics and save checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To persist your checkpoints and monitor training progress, add a
:class:`ray.train.xgboost.XGBoostTrainer.RayTrainReportCallback` utility callback to your Trainer:


.. code-block:: diff

     import xgboost
     from ray.train.xgboost import RayTrainReportCallback

     def train_func():
         ...
        bst = xgboost.train(
            ...,
            callbacks=[
                RayTrainReportCallback(
                    metrics={"loss": "eval-logloss"}, frequency=1
                )
            ],
        )
         ...


Reporting metrics and checkpoints to Ray Train enables integration with Ray Tune and :ref:`fault-tolerant training <train-fault-tolerance>`.

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

    # 1 node with 8 CPUs and 4 GPUs each.
    scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

    # 4 nodes with 8 CPUs and 4 GPUs each.
    scaling_config = ScalingConfig(num_workers=16, use_gpu=True)


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