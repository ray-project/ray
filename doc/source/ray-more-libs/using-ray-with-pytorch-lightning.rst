Using Ray with Pytorch Lightning
================================

.. note::
    For an overview of Ray's distributed training library,
    see :ref:`Ray Train <train-docs>`.

PyTorch Lightning is a framework which brings structure into training PyTorch models. It
aims to avoid boilerplate code, so you don't have to write the same training
loops all over again when building a new model.

.. image:: /images/pytorch_lightning_full.png
  :align: center

Using Ray with Pytorch Lightning allows you to easily distribute training and also run
distributed hyperparameter tuning experiments all from a single Python script. You can use the same code to run
Pytorch Lightning in a single process on your laptop, parallelize across the cores of your laptop, or parallelize across
a large multi-node cluster.

Ray provides 2 integration points with Pytorch Lightning.

1. `Ray Lightning Library <https://github.com/ray-project/ray_lightning>`_ for distributed Pytorch Lightning training with Ray

2. :ref:`Ray Tune with Pytorch Lightning <tune-pytorch-lightning-ref>` for distributed hyperparameter tuning of your PTL models.


Distributed Training with ``Ray Lightning``
-------------------------------------------

The `Ray Lightning Library <https://github.com/ray-project/ray_lightning>`__ provides plugins for distributed training with Ray.

These PyTorch Lightning Plugins on Ray enable quick and easy parallel training while still leveraging all the benefits of PyTorch Lightning. It offers the following plugins:

* `PyTorch Distributed Data Parallel <https://github.com/ray-project/ray_lightning#pytorch-distributed-data-parallel-plugin-on-ray>`__
* `Horovod <https://github.com/ray-project/ray_lightning#horovod-plugin-on-ray>`__
* `Fairscale <https://github.com/ray-project/ray_lightning#model-parallel-sharded-training-on-ray>`__ for model parallel training.

Once you add your plugin to the PyTorch Lightning Trainer, you can parallelize training to all the cores in your laptop, or across a massive multi-node, multi-GPU cluster with no additional code changes.

Install the Ray Lightning Library with the following commands:

.. code-block:: bash

    # To install from master
    pip install git+https://github.com/ray-project/ray_lightning#ray_lightning

To use, simply pass in the plugin to your Pytorch Lightning ``Trainer``. For full details, you can checkout the `README here <https://github.com/ray-project/ray_lightning#distributed-pytorch-lightning-training-on-ray>`__

Here is an example of using the ``RayStrategy`` for Distributed Data Parallel training on a Ray cluster:

First, let's define our PyTorch Lightning module.

.. literalinclude:: /../../python/ray/tests/ray_lightning/simple_example.py
    :language: python
    :dedent:
    :start-after: __pl_module_init__
    :end-before: __pl_module_end__


Then, we create a PyTorch Lightning Trainer, passing in ``RayStrategy``. We can also configure the number of training workers we want to use and whether to use GPU.

.. literalinclude:: /../../python/ray/tests/ray_lightning/simple_example.py
    :language: python
    :dedent:
    :start-after: __train_begin__
    :end-before: __train_end__

With this strategy, Pytorch DDP is used as the distributed training communication protocol, but Ray is used to launch and manage the training worker processes.

Multi-node Distributed Training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the same examples above, you can run distributed training on a multi-node cluster with just a couple simple steps.

First, use Ray's :ref:`Cluster Launcher <vm-cluster-quick-start>` to start a Ray cluster:

.. code-block:: bash

    ray up my_cluster_config.yaml

Then, run your Ray script using one of the following options:

1. on the head node of the cluster (``python train_script.py``)
2. via ``ray job submit`` (:ref:`docs <jobs-overview>`) from your laptop (``ray job submit -- python train.py``)
3. via the :ref:`Ray Client <ray-client-ref>` from your laptop.

.. _pytorch-lightning-tune:

Distributed Hyperparameter Optimization with Ray Tune
-----------------------------------------------------

You can also use :ref:`Ray Tune <tune-main>` with Pytorch Lightning to tune the hyperparameters of your model.
With this integration, you can run multiple training runs in parallel, with each run having a different set of hyperparameters
for your Pytorch Lightning model.

Hyperparameter Tuning with non-distributed training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you only want distributed hyperparameter tuning, but each training run doesn't need to be distributed,
you can use the ready-to-use Pytorch Lightning callbacks that Ray Tune provides.

We first wrap our training code into a function. To report metrics back to Tune after each validation epoch, we make sure to add the ``TuneReportCallback`` to the PyTorch Lightning Trainer. The learning rate is read from the provided ``config`` argument.

.. literalinclude:: /../../python/ray/tests/ray_lightning/simple_tune.py
    :language: python
    :dedent:
    :start-after: __train_func_begin__
    :end-before: __train_func_end__


Then, we use the Ray Tune ``Tuner`` to run our hyperparameter tuning experiment. We define a hyperparameter search space, and in this case we will try out different learning rate values. These hyperparameters get passed in as the ``config`` argument to the training function that we defined earlier.

.. literalinclude:: /../../python/ray/tests/ray_lightning/simple_tune.py
    :language: python
    :dedent:
    :start-after: __tune_run_begin__
    :end-before: __tune_run_end__


And if you want to add periodic checkpointing as well, you can use the ``TuneReportCheckpointCallback`` instead.

.. code-block:: python

    from ray.tune.integration.pytorch_lightning import TuneReportCheckpointCallback
    callback = TuneReportCheckpointCallback(
        metrics={"loss": "val_loss", "mean_accuracy": "val_accuracy"},
        filename="checkpoint",
        on="validation_end")


Check out the :ref:`Pytorch Lightning with Ray Tune tutorial <tune-pytorch-lightning-ref>` for a full example on how you can use these callbacks and run a tuning experiment for your Pytorch Lightning model.


Hyperparameter Tuning with distributed training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
These integrations also support the case where you want a distributed hyperparameter tuning experiment, but each trial (training run) needs to be distributed as well.
In this case, you want to use the `Ray Lightning Library's <https://github.com/ray-project/ray_lightning>`_ integration with Ray Tune.

With this integration, you can run multiple PyTorch Lightning training runs in parallel,
each with a different hyperparameter configuration, and each training run also parallelized.
All you have to do is move your training code to a function, pass the function to ``Tuner()``, and make sure to add the appropriate callback (Either ``TuneReportCallback`` or ``TuneReportCheckpointCallback``) to your PyTorch Lightning Trainer.

.. warning:: Make sure to use the callbacks from the Ray Lightning library and not the one from the Tune library, i.e. use ``ray_lightning.tune.TuneReportCallback`` and not ``ray.tune.integrations.pytorch_lightning.TuneReportCallback``.


As before, we first define our training function, this time making sure we specify ``RayStrategy`` and using the ``TuneReportCallback`` from the ``ray_lightning`` library.

.. literalinclude:: /../../python/ray/tests/ray_lightning/simple_tune.py
    :language: python
    :start-after: __train_func_distributed_begin__
    :end-before: __train_func_distributed_end__


Then, we use the ``Tuner`` to run our hyperparameter tuning experiment. We have to make sure we wrap our training function with ``tune.with_resources`` to tell Tune that each of the trials will also be distributed.

.. literalinclude:: /../../python/ray/tests/ray_lightning/simple_tune.py
    :language: python
    :dedent:
    :start-after: __tune_run_distributed_begin__
    :end-before: __tune_run_distributed_end__

    
