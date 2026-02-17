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

    # Elastic training with 1-10 workers
    scaling_config = ScalingConfig(num_workers=(1, 8), use_gpu=True)

    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    trainer.fit()

How it works
------------

Ray Train always requests ``max_workers`` number of workers, but if it can't get all of them, it starts when ``min_workers`` is available.

If any failures happen, Ray Train restarts with fewer workers. Then it attempts again to bring up to ``max_workers`` number of workers.

If the cluster gets nodes eventually, Ray Train restarts with the new workers added to the group.
