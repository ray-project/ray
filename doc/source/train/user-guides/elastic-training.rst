.. _train-elastic-training:

Elastic training
================

Ray Train supports elastic training, enabling jobs to seamlessly adapt to changes in resource availability. This behavior ensures continuous execution despite hardware failures or node preemptions, avoiding idle or wasted time. As more nodes become available, the cluster dynamically scales up to speed up training with more worker processes.

To enable elastic training, use :attr:`~ray.train.ScalingConfig.num_workers` to specify ``(min_workers, max_workers)`` as a tuple instead of a fixed worker group size.

The following example shows how to configure elastic training with a range of 1–8 workers:

.. code-block:: python

    from ray.train.torch import TorchTrainer, ScalingConfig

    def train_func():
        # Your training code here
        ...

    # Elastic training with 1-8 workers
    scaling_config = ScalingConfig(num_workers=(1, 8), use_gpu=True)

    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    trainer.fit()

How it works
------------

Starting with available workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Train always requests ``max_workers`` number of workers. If it can't get all of them, it starts when ``min_workers`` is available so training can begin without waiting for the full set of resources.

When failures happen
~~~~~~~~~~~~~~~~~~~~

If any failures happen (for example, a worker crashes or a node is preempted), Ray Train restarts with fewer workers. It then attempts again to bring the worker group back up to ``max_workers``. Without a retry limit, the run would exit on the first such failure. To allow the run to retry when worker failures occur, configure :attr:`~ray.train.RunConfig.failure_config` with :attr:`~ray.train.FailureConfig.max_failures`:

.. code-block:: python
    :emphasize-lines: 4

    from ray.train import RunConfig, FailureConfig

    # Retry up to 3 times on worker failures (e.g. preemption, node loss)
    run_config = RunConfig(failure_config=FailureConfig(max_failures=3))

    trainer = TorchTrainer(
        train_func,
        scaling_config=scaling_config,
        run_config=run_config,
    )

When more nodes become available
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the cluster gets more nodes eventually, Ray Train can resize the worker group and restart with the new workers added, so training can use the extra capacity. By default, the controller considers resizing every 60 seconds while the worker group is healthy. To change how often resize decisions are made, set :attr:`~ray.train.ScalingConfig.elastic_resize_monitor_interval_s` in your scaling config:

.. code-block:: python

    # Consider resizing the worker group every 30 seconds (default is 60)
    scaling_config = ScalingConfig(
        num_workers=(1, 8),
        use_gpu=True,
        elastic_resize_monitor_interval_s=30.0,
    )

Limitations
-----------

Elastic training is supported for CPU and GPU backends only. It isn't supported yet for TPU training.
