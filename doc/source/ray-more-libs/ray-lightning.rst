..
  This part of the docs is generated from the Ray Lightning readme using m2r
  To update:
  - run `m2r /path/to/ray_lightning/README.md`
  - copy the contents of README.rst here
  - remove the table of contents
  - remove the PyTorch Lightning Compatibility section
  - Be sure not to delete the API reference section in the bottom of this file.
  - add `.. _ray-lightning-tuning:` before the "Hyperparameter Tuning with Ray Tune" section
  - Adjust some link targets (e.g. for "Ray Tune") to anonymous references
    by adding a second underscore (use `target <link>`__)
  - Search for `\ **` and delete this from the links (bold links are not supported)

.. _ray-lightning:

Distributed PyTorch Lightning Training on Ray
=============================================

This library adds new PyTorch Lightning plugins for distributed training using the Ray distributed computing framework.

These PyTorch Lightning Plugins on Ray enable quick and easy parallel training while still leveraging all the benefits of PyTorch Lightning and using your desired training protocol, either `PyTorch Distributed Data Parallel <https://pytorch.org/tutorials/intermediate/ddp_tutorial.html>`_ or `Horovod <https://github.com/horovod/horovod>`_.

Once you add your plugin to the PyTorch Lightning Trainer, you can parallelize training to all the cores in your laptop, or across a massive multi-node, multi-GPU cluster with no additional code changes.

This library also comes with an integration with `Ray Tune <tune.io>`__ for distributed hyperparameter tuning experiments.

Installation
------------

You can install Ray Lightning via ``pip``\ :

``pip install ray_lightning``

Or to install master:

``pip install git+https://github.com/ray-project/ray_lightning#ray_lightning``


PyTorch Distributed Data Parallel Plugin on Ray
-----------------------------------------------

The ``RayPlugin`` provides Distributed Data Parallel training on a Ray cluster. PyTorch DDP is used as the distributed training protocol, and Ray is used to launch and manage the training worker processes.

Here is a simplified example:

.. code-block:: python

   import pytorch_lightning as pl
   from ray_lightning import RayPlugin

   # Create your PyTorch Lightning model here.
   ptl_model = MNISTClassifier(...)
   plugin = RayPlugin(num_workers=4, num_cpus_per_worker=1, use_gpu=True)

   # Don't set ``gpus`` in the ``Trainer``.
   # The actual number of GPUs is determined by ``num_workers``.
   trainer = pl.Trainer(..., plugins=[plugin])
   trainer.fit(ptl_model)

Because Ray is used to launch processes, instead of the same script being called multiple times, you CAN use this plugin even in cases when you cannot use the standard ``DDPPlugin`` such as


* Jupyter Notebooks, Google Colab, Kaggle
* Calling ``fit`` or ``test`` multiple times in the same script

Multi-node Distributed Training
-------------------------------

Using the same examples above, you can run distributed training on a multi-node cluster with just 2 simple steps.
1) `Use Ray's cluster launcher <https://docs.ray.io/en/master/cluster/cloud.html>`__ to start a Ray cluster- ``ray up my_cluster_config.yaml``.
2) `Execute your Python script on the Ray cluster <https://docs.ray.io/en/master/cluster/commands.html#running-ray-scripts-on-the-cluster-ray-submit>`__\ - ``ray submit my_cluster_config.yaml train.py``. This will ``rsync`` your training script to the head node, and execute it on the Ray cluster. (Note: The training script can also be executed using :ref:`Ray Job Submission <jobs-overview>`,
which is in beta starting with Ray 1.12.  Try it out!)

You no longer have to set environment variables or configurations and run your training script on every single node.

Multi-node Interactive Training from your Laptop
------------------------------------------------

Ray provides capabilities to run multi-node and GPU training all from your laptop through `Ray Client <ray-client>`__

You can follow the instructions `here <ray-client>`__ to set up the cluster.
Then, add this line to the beginning of your script to connect to the cluster:

.. code-block:: python

   # replace with the appropriate host and port
   ray.init("ray://<head_node_host>:10001")

Now you can run your training script on the laptop, but have it execute as if your laptop has all the resources of the cluster essentially providing you with an **infinite laptop**.

**Note:** When using with Ray Client, you must disable checkpointing and logging for your Trainer by setting ``checkpoint_callback`` and ``logger`` to ``False``.

Horovod Plugin on Ray
---------------------

Or if you prefer to use Horovod as the distributed training protocol, use the ``HorovodRayPlugin`` instead.

.. code-block:: python

   import pytorch_lightning as pl
   from ray_lightning import HorovodRayPlugin

   # Create your PyTorch Lightning model here.
   ptl_model = MNISTClassifier(...)

   # 2 nodes, 4 workers per node, each using 1 CPU and 1 GPU.
   plugin = HorovodRayPlugin(num_hosts=2, num_slots=4, use_gpu=True)

   # Don't set ``gpus`` in the ``Trainer``.
   # The actual number of GPUs is determined by ``num_slots``.
   trainer = pl.Trainer(..., plugins=[plugin])
   trainer.fit(ptl_model)

Model Parallel Sharded Training on Ray
--------------------------------------

The ``RayShardedPlugin`` integrates with `FairScale <https://github.com/facebookresearch/fairscale>`__ to provide sharded DDP training on a Ray cluster.
With sharded training, leverage the scalability of data parallel training while drastically reducing memory usage when training large models.

.. code-block:: python

   import pytorch_lightning as pl
   from ray_lightning import RayShardedPlugin

   # Create your PyTorch Lightning model here.
   ptl_model = MNISTClassifier(...)
   plugin = RayShardedPlugin(num_workers=4, num_cpus_per_worker=1, use_gpu=True)

   # Don't set ``gpus`` in the ``Trainer``.
   # The actual number of GPUs is determined by ``num_workers``.
   trainer = pl.Trainer(..., plugins=[plugin])
   trainer.fit(ptl_model)

See the `Pytorch Lightning docs <https://pytorch-lightning.readthedocs.io/en/stable/advanced/multi_gpu.html#sharded-training>`__ for more information on sharded training.

.. _ray-lightning-tuning:

Hyperparameter Tuning with Ray Tune
-----------------------------------

``ray_lightning`` also integrates with Ray Tune to provide distributed hyperparameter tuning for your distributed model training. You can run multiple PyTorch Lightning training runs in parallel, each with a different hyperparameter configuration, and each training run parallelized by itself. All you have to do is move your training code to a function, pass the function to tune.run, and make sure to add the appropriate callback (Either ``TuneReportCallback`` or ``TuneReportCheckpointCallback``\ ) to your PyTorch Lightning Trainer.

Example using ``ray_lightning`` with Tune:

.. code-block:: python

   from ray import tune

   from ray_lightning import RayPlugin
   from ray_lightning.tune import TuneReportCallback, get_tune_ddp_resources

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

   # Make sure to pass in ``resources_per_trial`` using the ``get_tune_ddp_resources`` utility.
   analysis = tune.run(
           train_mnist,
           metric="loss",
           mode="min",
           config=config,
           num_samples=num_samples,
           resources_per_trial=get_tune_ddp_resources(num_workers=4),
           name="tune_mnist")

   print("Best hyperparameters found were: ", analysis.best_config)

FAQ
---

..

   RaySGD already has a `Pytorch Lightning integration <https://docs.ray.io/en/master/raysgd/raysgd_ref.html>`__. What's the difference between this integration and that?


The key difference is which Trainer you'll be interacting with. In this library, you will still be using Pytorch Lightning's ``Trainer``. You'll be able to leverage all the features of Pytorch Lightning, and Ray is used just as a backend to handle distributed training.

With RaySGD's integration, you'll be converting your ``LightningModule`` to be RaySGD compatible, and will be interacting with RaySGD's ``TorchTrainer``. RaySGD's ``TorchTrainer`` is not as feature rich nor as easy to use as Pytorch Lightning's ``Trainer`` (no built in support for logging, early stopping, etc.). However, it does have built in support for fault-tolerant and elastic training. If these are hard requirements for you, then RaySGD's integration with PTL might be a better option.

..

   I see that ``RayPlugin`` is based off of Pytorch Lightning's ``DDPSpawnPlugin``. However, doesn't the PTL team discourage the use of spawn?


As discussed `here <https://github.com/pytorch/pytorch/issues/51688#issuecomment-773539003>`__\ , using a spawn approach instead of launch is not all that detrimental. The original factors for discouraging spawn were:


#. not being able to use 'spawn' in a Jupyter or Colab notebook, and
#. not being able to use multiple workers for data loading.

Neither of these should be an issue with the ``RayPlugin`` due to Ray's serialization mechanisms. The only thing to keep in mind is that when using this plugin, your model does have to be serializable/pickleable.


API Reference
-------------

.. autoclass:: ray_lightning.RayPlugin

.. autoclass:: ray_lightning.HorovodRayPlugin

.. autoclass:: ray_lightning.RayShardedPlugin


Tune Integration
^^^^^^^^^^^^^^^^

.. autoclass:: ray_lightning.tune.TuneReportCallback

.. autoclass:: ray_lightning.tune.TuneReportCheckpointCallback

.. autofunction:: ray_lightning.tune.get_tune_resources

