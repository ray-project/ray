.. _train-key-concepts:

.. _train-overview:

Ray Train Overview
==================

Ray Train has four key concepts:

#. :ref:`Training function <train-overview-training-function>`: User-defined Python training loop.
#. :ref:`Worker <train-overview-worker>`: Runs the training function.
#. :ref:`Scaling configuration: <train-overview-scaling-config>` Configures the number of workers and the number of resources per worker.
#. :ref:`Trainer <train-overview-trainers>`: Ties the training function, workers, and scaling configuration together to execute a single distributed training job.

.. _train-overview-training-function:

Training function
-----------------

The training function is a user-defined wrapper function that the Trainer dispatches to the distributed processes.
It contains the training loop function with the addition of logic that provides the context that every worker needs to run the training job. 
It loads the model, gets the shard of data, does checkpointing, and can have evaluation logic and metrics reporting.

Ray Train documentation uses the following conventions:

#. Users write a `train_func` function that defines training code.
#. Users pass `train_func` to the Trainer through the `train_loop_per_worker` parameter because `train_func` executes the training loop on each worker process.

.. code-block:: python

    def train_func():
        """User-defined training function that runs on each distributed worker process."""
        
        # Load model
        ...
        # Load dataset
        ...
        # Train model
        ...
        # Report metrics and checkpoints
        ...

.. _train-overview-worker:

Worker
------

Ray Train distributes model training compute to Ray Workers. 
The user defines the number of workers in the scaling configuration.
Each Worker is a Python process that executes the train_func on a GPU or CPU resource.  
Ray Train abstracts away the allocation and orchestration of nodes and compute resources for Workers.
The number of workers typically equals the aggregate number of GPUs (or CPUs?) you are allocating to the entire training job.

.. _train-overview-scaling-config:

Scaling configuration
---------------------

Ray Train scales training based on high level scaling parameters. 
Specify the number of worker processes to distribute the training to, using the :py:class:`~ray.train.ScalingConfig` class.
Two basic parameters scale the training compute resources:

* `num_workers`: The number of workers to launch for a single distributed training job.
* `use_gpu`: The flag that configures Ray Train to use GPUs or not. 

.. code-block:: python

    from ray.train import ScalingConfig

    # Single CPU
    scaling_config = ScalingConfig(num_workers=1, use_gpu=False)

    # Single GPU
    scaling_config = ScalingConfig(num_workers=1, use_gpu=True)

    # Multiple GPUs
    scaling_config = ScalingConfig(num_workers=3, use_gpu=True)

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