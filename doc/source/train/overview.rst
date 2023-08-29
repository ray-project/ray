.. _train-key-concepts:

.. _train-overview:

Ray Train Overview
==================

.. hidden:: 
    
    image:: ./images/train-concepts.svg
        
.. visible::

To use Ray Train effectively, there are four main concepts you need to understand:

#. :ref:`Training function <train-overview-training-function>`: A Python function that contains your model training logic.
#. :ref:`Worker <train-overview-worker>`: A process that runs the training function.
#. :ref:`Scaling configuration: <train-overview-scaling-config>` A configuration of the number of workers and compute resources (CPU, GPU) per worker.
#. :ref:`Trainer <train-overview-trainers>`: A Python class that ties together the training function, workers, and scaling configuration to execute a distributed training job.

.. _train-overview-training-function:

Training function
-----------------

The training function is a user-defined Python function that contains the end-to-end model training loop logic.

When launching a distribued training job, each worker executes this training function.

Typically, this training function contains logic for loading the dataset, training the model, and saving checkpoints.

Ray Train documentation uses the following conventions:

#. `train_func` is user-defined function that contains the training code.
#. The `train_func` is passed into the Trainer's `train_loop_per_worker` parameter.

.. code-block:: python

    def train_func():
        """User-defined training function that runs on each distributed worker process.
        
        This typically contains logic for loading the model, loading the dataset, 
        training the model, saving checkpoints, and logging metrics.
        """
        ...

.. _train-overview-worker:

Worker
------

Ray Train distributes model training compute to individual worker processes across the cluster. 
Each worker is a process that executes the `train_func` on compute resources (e.g., CPU, GPU). 
The number of workers determines the parallelism of the training job, and can be configured through the `ScalingConfig`.

.. Ray Train abstracts away the allocation and orchestration of nodes and compute resources for workers.
.. The user defines the number of workers in the scaling configuration.
.. The number of workers typically equals the aggregate number of GPUs (or CPUs?) you are allocating to the entire training job.

.. _train-overview-scaling-config:

Scaling configuration
---------------------

The :class:`~ray.train.ScalingConfig` is the mechanism for defining the scale of the training job.
Two basic parameters scale the worker parallelism and compute resources:

* `num_workers`: The number of workers to launch for a distributed training job.
* `use_gpu`: Whether each worker should use GPUs or CPUs. 

.. code-block:: python

    from ray.train import ScalingConfig

    # Single CPU
    scaling_config = ScalingConfig(num_workers=1, use_gpu=False)

    # Single GPU
    scaling_config = ScalingConfig(num_workers=1, use_gpu=True)

    # Multiple GPUs
    scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

.. _train-overview-trainers:

Trainer
-------

The Trainer ties all three previous concepts together to execute distributed training runs.
It's the primary class that the you interface with for launching distributed training.
The Trainer creates multiple workers and runs your training function.

.. code-block:: python

    from ray.train.torch import TorchTrainer
    
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    trainer.fit()