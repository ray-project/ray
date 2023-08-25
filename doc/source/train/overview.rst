.. _train-key-concepts:

.. _train-overview:

Ray Train Overview
==================

.. image:: ./images/train-concepts.svg

<<<<<<<TODO: I submitted a request for this diagram to be professionally done by a designer we have on contract.>>>>>>>

Ray Train is based on four key concepts:

#. :ref:`Training function <train-overview-train_func>`: User-defined Python training loop.
#. :ref:`Worker <train-overview-workers>`: Runs the training function.
#. :class:`Scaling configuration <ray.train.ScalingConfig>` Configures the number of workers.
#. :ref:`Trainers <train-overview-ray-trainers>`: Ties the training function, workers, and ScalingConfig together to execute a single distributed training job.

.. _train-overview-train_func:

Training function
-----------------

The training function is a user-defined wrapper function that the Ray Trainer dispatches to the distributed processes.
It contains your training loop function with the addition of logic that provides the context that every worker needs to run the training job. 
It loads the model, gets the shard of data, does checkpointing, and can have evaluation logic and metrics reporting.
train_func is an parameter of the Ray Trainer.

[TODO Is this not needed for tree-based trainers?]

.. _train-overview-workers:

Workers
-------

Ray Train distributes model training compute to Ray Workers. 
The user defines the number of workers in the scaling configuration.
Each Worker is a Python process that executes the train_func on a GPU or CPU resource.  
Ray Train abstracts away the allocation and orchestration of nodes and compute resources for Workers.
The number of workers typically equals the aggregate number of GPUs (or CPUs?) that are allocated to the entire training job.

.. _train-key-overview-scalingconfig:

ScalingConfig
-------------

Ray Train scales training based on high level scaling parameters. 
Users specify the number of Worker processes to distribute the training to, using the :py:class:`~ray.train.ScalingConfig` class.
Two basic parameters scale the training compute resources:

* `num_workers`: The number of Workers to launch for a single distributed training job.
* `use_gpu`: The flag that configures Ray Train to use GPUs or not. 

Examples:

.. code-block:: python

    # Single CPU: num_workers=1, use_gpu=false
    ScalingConfig(num_workers=1, use_gpu=False)

    # Single GPU: num_workers=1, use_gpu=true
    ScalingConfig(num_workers=1, use_gpu=True)

    # Three GPUs: num_workers=3, use_gpu=true
    ScalingConfig(num_workers=3, use_gpu=True)


.. _train-overview-ray-trainers:

Trainers
--------

Trainers execute distributed training runs. 
They are wrapper classes around third-party framework trainers. 
These classes are the interface between Ray Train and third-party trainers. 
Ray Trainers abstract away the details of scaling compute resources that include orchestration of nodes and GPU resource management.

[TODO: Ask Matt for content]