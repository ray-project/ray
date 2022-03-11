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

Here is an example of using the ``RayPlugin`` for Distributed Data Parallel training on a Ray cluster:

.. TODO: code snippet untested

.. code-block:: python

    import pytorch_lightning as pl
    from ray_lightning import RayPlugin

    # Create your PyTorch Lightning model here.
    ptl_model = MNISTClassifier(...)
    plugin = RayPlugin(num_workers=4, cpus_per_worker=1, use_gpu=True)

    # If using GPUs, set the ``gpus`` arg to a value > 0.
    # The actual number of GPUs is determined by ``num_workers``.
    trainer = pl.Trainer(..., gpus=1, plugins=[plugin])
    trainer.fit(ptl_model)

With this plugin, Pytorch DDP is used as the distributed training communication protocol, but Ray is used to launch and manage the training worker processes.

Multi-node Distributed Training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the same examples above, you can run distributed training on a multi-node cluster with just a couple simple steps.

First, use Ray's :ref:`Cluster Launcher <ref-cluster-quick-start>` to start a Ray cluster:

.. code-block:: bash

    ray up my_cluster_config.yaml

Then, run your Ray script using one of the following options:

1. on the head node of the cluster (``python train_script.py``)
2. via ``ray job submit`` (:ref:`docs <jobs-overview>`) from your laptop (``ray job submit -- python train.py``)
3. via the :ref:`Ray Client<ray-client>` from your laptop.

Distributed Hyperparameter Optimization with Ray Tune
-----------------------------------------------------

You can also use :ref:`Ray Tune<tune-main>` with Pytorch Lightning to tune the hyperparameters of your model.
With this integration, you can run multiple training runs in parallel, with each run having a different set of hyperparameters
for your Pytorch Lightning model.

Hyperparameter Tuning with non-distributed training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you only want distributed hyperparameter tuning, but each training run doesn't need to be distributed,
you can use the ready-to-use Pytorch Lightning callbacks that Ray Tune provides.

To report metrics back to Tune after each validation epoch, we can use the ``TuneReportCallback``:

.. code-block:: python

    from ray.tune.integration.pytorch_lightning import TuneReportCallback

    def train_mnist(config):

        # Create your PTL model.
        model = MNISTClassifier(config)

        # Create the Tune Reporting Callback
        metrics = {"loss": "ptl/val_loss", "acc": "ptl/val_accuracy"}
        callbacks = [TuneReportCallback(metrics, on="validation_end")]

        trainer = pl.Trainer(max_epochs=4, callbacks=callbacks)
        trainer.fit(model)

    config = {
        "layer_1": tune.choice([32, 64, 128]),
        "layer_2": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
    }

    # Make sure to specify how many actors each training run will create via the "extra_cpu" field.
    analysis = tune.run(
            train_mnist,
            metric="loss",
            mode="min",
            config=config,
            num_samples=num_samples,
            name="tune_mnist")

    print("Best hyperparameters found were: ", analysis.best_config)


And if you want to add periodic checkpointing as well, you can use the ``TuneReportCheckpointCallback`` instead.

.. code-block:: python

    from ray.tune.integration.pytorch_lightning import TuneReportCheckpointCallback
    callback = TuneReportCheckpointCallback(
        metrics={"loss": "val_loss", "mean_accuracy": "val_accuracy"},
        filename="checkpoint",
        on="validation_end")


Check out the :ref:`Pytorch Lightning with Ray Tune tutorial<tune-pytorch-lightning-ref>` for a full example on how you can use these callbacks and run a tuning experiment for your Pytorch Lightning model.


Hyperparameter Tuning with distributed training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
These integrations also support the case where you want a distributed hyperparameter tuning experiment, but each trial (training run) needs to be distributed as well.
In this case, you want to use the `Ray Lightning Library's <https://github.com/ray-project/ray_lightning>`_ integration with Ray Tune.

With this integration, you can run multiple PyTorch Lightning training runs in parallel,
each with a different hyperparameter configuration, and each training run also parallelized.
All you have to do is move your training code to a function, pass the function to ``tune.run``, and make sure to add the appropriate callback (Either ``TuneReportCallback`` or ``TuneReportCheckpointCallback``) to your PyTorch Lightning Trainer.

.. warning:: Make sure to use the callbacks from the Ray Lightning library and not the one from the Tune library, i.e. use ``ray_lightning.tune.TuneReportCallback`` and not ``ray.tune.integrations.pytorch_lightning.TuneReportCallback``.

Example using Ray Lightning with Tune:

.. code-block:: python

    from ray_lightning import RayPlugin
    from ray_lightning.tune import TuneReportCallback

    def train_mnist(config):

    # Create your PTL model.
    model = MNISTClassifier(config)

    # Create the Tune Reporting Callback
    metrics = {"loss": "ptl/val_loss", "acc": "ptl/val_accuracy"}
    callbacks = [TuneReportCallback(metrics, on="validation_end")]

    trainer = pl.Trainer(
        max_epochs=4,
        callbacks=callbacks,
        plugins=[RayPlugin(num_workers=4, use_gpu=False)])
    trainer.fit(model)

    config = {
        "layer_1": tune.choice([32, 64, 128]),
        "layer_2": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
    }

    # Make sure to specify how many actors each training run will create via the "extra_cpu" field.
    analysis = tune.run(
            train_mnist,
            metric="loss",
            mode="min",
            config=config,
            num_samples=num_samples,
            resources_per_trial={
                "cpu": 1,
                "extra_cpu": 4
            },
            name="tune_mnist")

    print("Best hyperparameters found were: ", analysis.best_config)