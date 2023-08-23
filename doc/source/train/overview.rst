.. _train-overivew:

Ray Train Overview
==================

Ray Train distributes model training without writing distributed logic.

Four main concepts are in the Ray Train library:

1. :ref:`Ray trainers <train-overview-ray-trainers>` are framework-specific, top-level APIs that execute a single distributed training job.
2. :ref:`train_func <train-overview-train_func>` is your Python training loop.
3. :class:`ScalingConfig <ray.train.ScalingConfig>` is the Ray Train configuration class that specifies the aggregate compute resources to allocate to a single training job.
4. :ref:`Worker <train-overview-workers>` is a process that runs the `train_func` on a cluster. The number of workers generally equals the number of GPUs available in a cluster.

.. https://docs.google.com/drawings/d/1FezcdrXJuxLZzo6Rjz1CHyJzseH8nPFZp6IUepdn3N4/edit

.. _train-overview-ray-trainers:

Ray trainers
------------

Ray trainers are wrapper classes around third-party framework trainers. These classes are the interface between Ray Train and third-party trainers. 
Ray trainers abstract away the details of scaling compute resources that include orchestration of nodes and GPU resource management.
They are responsible for executing (distributed) training runs.
[TODO: Ask Matt for content]

.. _train-overview-train_func:

train_func
----------

The train_func is your training loop function to pass to the Ray trainer. 
It loads the model, gets the shard of data, does checkpointing, and can have evaluation logic and metrics reporting.
This function must contain the context that every worker needs to run the training job.
The Ray trainer dispatches train_func to each worker process.
[TODO Is this not needed for tree-based trainers?]

.. _train-key-overview-scalingconfig:

ScalingConfig
-------------

Configure the compute resources for distributing model training, using the :py:class:`~ray.train.ScalingConfig` class.
Two basic parameters scale your training compute resources:

* `num_workers`: Sets the number of workers to launch for a single distributed training job.
* `use_gpu`: Configures Ray Train to use GPUs or not. 

.. _train-overview-workers:

Workers
-------
A worker process runs your training computing. It has a GPU, executes, train_func. Actually a Python process that runs your train_func. Want to have one GPU per worker.
Can have multiple GPUs per worker. One worker can use multiple nodes and multiple workers can be on one node. Ray manages worker allocating
compute resources from a pool that the user defines, across compute boundaries.

Next steps
----------

Get started with a framework-specific guide to run distributed model training with Ray Train.

.. tab-set::

    .. tab-item:: Deep Learning Trainers

        Ray Train supports the following deep learning trainers:

        - :class:`TorchTrainer <ray.train.torch.TorchTrainer>`
        - :class:`TensorflowTrainer <ray.train.tensorflow.TensorflowTrainer>`
        - :class:`HorovodTrainer <ray.train.horovod.HorovodTrainer>`

        For these trainers, you usually define your own training function that loads the model
        and executes single-worker training steps. Refer to the following guides for more details:

        - :doc:`PyTorch Guide </train/getting-started-pytorch>`
        - :doc:`TensorFlow Guide </train/distributed-tensorflow-keras>`
        - :doc:`Horovod Guide </train/horovod>`

    .. tab-item:: Tree-Based Trainers

        Tree-based trainers use gradient-based decision trees for training. The most popular libraries are XGBoost and LightGBM:

        - :class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>`
        - :class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`

        For these trainers, pass a dataset and parameters. Ray Train configures the training loop
        automatically.

        - :doc:`Distributed XGBoost/LightGBM </train/distributed-xgboost-lightgbm>`
