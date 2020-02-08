RaySGD Pytorch
==============

.. warning:: This is still an experimental API and is subject to change in the near future.

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/26EMwdahdgm7Lscy9>`_!

.. image:: raysgd-pytorch.svg
    :align: center

The RaySGD ``PyTorchTrainer`` simplifies distributed model training for PyTorch. The ``PyTorchTrainer`` is a wrapper around ``torch.distributed.launch`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to needing to execute training outside of Python. For end to end examples, see :ref:`raysgd-pytorch-example`.

Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``), each of which is managed by a Ray actor.

.. image:: raysgd-actors.svg
    :align: center



Setting up training
-------------------

The ``PyTorchTrainer`` can be constructed with functions that wrap components of the training script. Specifically, it needs constructors for the Model, Data, Optimizer, and Loss to create replicated copies across different devices and machines.

For example:

.. code-block:: python

    import numpy as np
    import torch
    import torch.nn as nn
    from torch import distributed

    from ray.experimental.sgd import PyTorchTrainer
    from ray.experimental.sgd.examples.train_example import LinearDataset


    def model_creator(config):
        """Constructor function for the model(s) to be optimized.

        You will also need to provide a custom training
        function to specify the optimization procedure for multiple models.

        Args:
            config (dict): Configuration dictionary passed into ``PyTorchTrainer``.

        Returns:
            One or more torch.nn.Module objects.
        """
        return nn.Linear(1, 1)


    def optimizer_creator(models, config):
        """Constructor of one or more Torch optimizers.

        Args:
            models: The return values from ``model_creator``. This can be one
                or more torch nn modules.
            config (dict): Configuration dictionary passed into ``PyTorchTrainer``.

        Returns:
            One or more Torch optimizer objects.
        """
      return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


    def data_creator(config):
        """Constructs torch.utils.data.Dataset objects.

        Note that even though two Dataset objects can be returned,
        only one dataset will be used for training.

        Args:
            config: Configuration dictionary passed into ``PyTorchTrainer``

        Returns:
            One or Two Dataset objects. If only one Dataset object is provided,
            ``trainer.validate()`` will throw a ValueError.
        """
        return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


    def loss_creator(config):
        """Constructs the Torch Loss object.

        Note that optionally, you can pass in a Torch Loss constructor directly
        into the PyTorchTrainer (i.e., ``PyTorchTrainer(loss_creator=nn.BCELoss, ...)``).

        Args:
            config: Configuration dictionary passed into ``PyTorchTrainer``

        Returns:
            Torch Loss object.
        """
        return torch.nn.BCELoss()


    def scheduler_creator(optimizers, config):
        """Constructor of one or more Torch optimizer schedulers.

        Note that optionally, you can pass in a Torch Scheduler constructor directly
        into the PyTorchTrainer. For example:

        .. code-block:: python

            PyTorchTrainer(
                scheduler_creator=torch.optim.lr_scheduler.ReduceLROnPlateau,
                ...)``.

        Args:
            optimizers: The return values from ``optimizer_creator``. This can be one
                or more torch optimizer objects.
            config: Configuration dictionary passed into ``PyTorchTrainer``

        Returns:
            One or more Torch scheduler objects.
        """
        return torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer)



Before instantiating the trainer, you'll have to start or connect to a Ray cluster:

.. code-block:: python

    ray.init()
    # or ray.init(address="auto") if a cluster has been started.

Instantiate the trainer object:

.. code-block:: python

    from ray.experimental.sgd import PyTorchTrainer

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        scheduler_creator=scheduler_creator,
        config={"lr": 0.001})


You can also set the number of workers and whether the workers will use GPUs:

.. code-block:: python

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        scheduler_creator=scheduler_creator,
        config={"lr": 0.001},
        num_replicas=100,
        use_gpu=True)


See the documentation on the PyTorchTrainer here: :ref:`ref-pytorch-trainer`.
We'll look at the training APIs next.

Training APIs
-------------

Now that the trainer is constructed, you'll naturally want to train the model.

.. code-block:: python

    trainer.train()

This takes one pass over the training data.

To run the model on the validation data passed in by the ``data_creator``, you can simply call:

.. code-block:: python

    trainer.validate()

You can customize the exact function that is called by using a customized training function (see :ref:`raysgd-custom-training`).


Shutting down training
----------------------

After training, you may want to reappropriate the Ray cluster. To release Ray resources obtained by the Trainer:

.. code-block:: python

    trainer.shutdown()

.. note:: Be sure to call ``trainer.save()`` or ``trainer.get_model()`` before shutting down.

Initialization Functions
------------------------

You may want to run some initializers on each worker when they are started. This may be something like setting an environment variable or downloading some data. You can do this via the ``initialization_hook`` parameter:

.. code-block:: python


    def initialization_hook(runner):
        print("NCCL DEBUG SET")
        # Need this for avoiding a connection restart issue
        os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
        os.environ["NCCL_LL_THRESHOLD"] = "0"
        os.environ["NCCL_DEBUG"] = "INFO"

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        initialization_hook=initialization_hook,
        config={"lr": 0.001}
        num_replicas=100,
        use_gpu=True)

Save and Load
-------------

If you want to save or reload the training procedure, you can use ``trainer.save``
and ``trainer.load``, which wraps the relevant ``torch.save`` and ``torch.load`` calls. This should work across a distributed cluster even without a NFS because it takes advantage of Ray's distributed object store.

.. code-block::

    trainer_1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=num_replicas)
    trainer_1.train()

    checkpoint_path = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer_1.save(checkpoint_path)

    trainer_2 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=num_replicas)
    trainer_2.restore(checkpoint_path)


Exporting a model for inference
-------------------------------

The trained torch model can be extracted for use within the same Python program with ``trainer.get_model()``. This will load the state dictionary of the model(s).

.. code-block::

    trainer.train()
    model = trainer.get_model()

Mixed Precision (FP16) Training
-------------------------------

You can enable mixed precision training for PyTorch by the ``use_fp16`` flag. This automatically converts the model(s) and optimizer(s) to train using mixed-precision. This requires NVIDIA ``Apex``, which can be installed from `the NVIDIA/Apex repository <https://github.com/NVIDIA/apex#quick-start>`_:

.. code-block::

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=4,
        use_fp16=True
    )

``Apex`` is a Pytorch extension with NVIDIA-maintained utilities to streamline mixed precision and distributed training. When using ``use_fp16``, you should not manually cast their model or data to .half(). The flag informs the Trainer to call ``amp.initialize`` on the provided models and optimizers underneath the hood, along with using the scaled loss: ``amp.scale_loss(loss, optimizer)``.

To specify particular parameters for ``amp.initialize``, you can use the ``apex_args`` field for the PyTorchTrainer constructor, as follows:

.. code-block::

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=4,
        use_fp16=True,
        apex_args={
            opt_level="O3",
            num_losses=2,
            verbosity=0
        }
    )

Note that if using a custom training function, you will need to manage loss scaling manually.


Distributed Multi-node Training
-------------------------------

You can scale out your training onto multiple nodes without making any modifications to your training code. To train across a cluster, simply make sure that the Ray cluster is started.

You can start a Ray cluster `via the Ray cluster launcher <autoscaling.html>`_ or `manually <using-ray-on-a-cluster.html>`_.

.. code-block:: bash

    ray up CLUSTER.yaml
    ray submit train.py --args="--address='auto'"

Then, within ``train.py`` you'll be able to scale up the number of workers seamlessly across multiple nodes:

.. code-block:: python

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=100)


Advanced: Fault Tolerance
-------------------------

For distributed deep learning, jobs are often run on infrastructure where nodes can be pre-empted frequently (i.e., spot instances in the cloud). To overcome this, RaySGD provides **fault tolerance** features that enable training to continue regardless of node failures.

.. code-block:: bash

    trainer.train(max_retries=N)


During each ``train`` method, each parallel worker iterates through the dataset, synchronizing gradients and parameters at each batch. These synchronization primitives can hang when one or more of the parallel workers becomes unresponsive (i.e., when a node is lost). To address this, we've implemented the following protocol.

  1. If any worker node is lost, Ray will mark the training task as complete (``ray.wait`` will return).
  2. Ray will throw ``RayActorException`` when fetching the result for any worker, so the Trainer class will call ``ray.get`` on the "finished" training task.
  3. Upon catching this exception, the Trainer class will kill all of its workers.
  4. The Trainer will then detect the quantity of available resources (either CPUs or GPUs). It will then restart as many workers as it can, each resuming from the last checkpoint. Note that this may result in fewer workers than initially specified.
  5. If there are no available resources, the Trainer will apply an exponential backoff before retrying to create workers.
  6. If there are available resources and the Trainer has fewer workers than initially specified, then it will scale up its worker pool until it reaches the initially specified ``num_workers``.

Note that we assume the Trainer itself is not on a pre-emptible node. It is currently not possible to recover from a Trainer node failure.

Users can set ``checkpoint="auto"`` to always checkpoint the current model before executing a pass over the training dataset.

.. code-block:: bash

    trainer.train(max_retries=N, checkpoint="auto")


Advanced: Hyperparameter Tuning
-------------------------------

``PyTorchTrainer`` naturally integrates with Tune via the ``PyTorchTrainable`` interface. The same arguments to ``PyTorchTrainer`` should be passed into the ``tune.run(config=...)`` as shown below.

.. literalinclude:: ../../../python/ray/experimental/sgd/pytorch/examples/tune_example.py
   :language: python
   :start-after: __torch_tune_example__


Simultaneous Multi-model training
---------------------------------

In certain scenarios such as training GANs, you may want to use multiple models in the training loop. You can do this in the ``PyTorchTrainer`` by allowing the ``model_creator`` and the ``optimizer_creator`` to return multiple values.

If multiple models are returned, you will need to provide a custom training function (and custom validation function if you plan to call ``validate``).

You can see the `DCGAN script <https://github.com/ray-project/ray/blob/master/python/ray/experimental/sgd/pytorch/examples/dcgan.py>`_ for an end-to-end example.

.. code-block:: python

    def model_creator(config):
        netD = Discriminator()
        netD.apply(weights_init)

        netG = Generator()
        netG.apply(weights_init)
        return netD, netG


    def optimizer_creator(models, config):
        net_d, net_g = models
        discriminator_opt = optim.Adam(
            net_d.parameters(), lr=config.get("lr", 0.01), betas=(0.5, 0.999))
        generator_opt = optim.Adam(
            net_g.parameters(), lr=config.get("lr", 0.01), betas=(0.5, 0.999))
        return discriminator_opt, generator_opt


    def custom_train(models, dataloader, criterion, optimizers, config):
        result = {}
        for i, (model, optimizer) in enumerate(zip(models, optimizers)):
            result["model_{}".format(i)] = train(model, dataloader, criterion,
                                                 optimizer, config)
        return result

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.BCELoss,
        train_function=custom_train)

.. _raysgd-custom-training:

Custom Training and Validation Functions
----------------------------------------

``PyTorchTrainer`` allows you to run a custom training and validation step in parallel on each worker, providing a flexibility similar to using PyTorch natively. This is done via the ``train_function`` and ``validation_function`` parameters.

Note that this is needed if the model creator returns multiple models.

.. code-block:: python

    def train(models, dataloader, criterion, optimizers, config):
        """A custom training function.

        Args:
            models: Output of the model_creator passed into PyTorchTrainer.
            data_loader: A dataloader wrapping the training dataset created by the ``data_creator`` passed into PyTorchTrainer.
            criterion: The instantiation of the ``loss_creator``.
            optimizers: Output of the optimizer_creator passed into PyTorchTrainer.
            config: The configuration dictionary passed into PyTorchTrainer.

        Returns:
            A dictionary of values/metrics.
        """

        netD, netG = models
        optimD, optimG = optimizers
        real_label = 1
        fake_label = 0
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        for i, data in enumerate(dataloader, 0):
            netD.zero_grad()
            real_cpu = data[0].to(device)
            b_size = real_cpu.size(0)
            label = torch.full((b_size, ), real_label, device=device)
            output = netD(real_cpu).view(-1)
            errD_real = criterion(output, label)
            errD_real.backward()

            noise = torch.randn(b_size, latent_vector_size, 1, 1, device=device)
            fake = netG(noise)
            label.fill_(fake_label)
            output = netD(fake.detach()).view(-1)
            errD_fake = criterion(output, label)
            errD_fake.backward()
            errD = errD_real + errD_fake
            optimD.step()

            netG.zero_grad()
            label.fill_(real_label)
            output = netD(fake).view(-1)
            errG = criterion(output, label)
            errG.backward()
            optimG.step()

            is_score, is_std = inception_score(fake)

        return {
            "loss_g": errG.item(),
            "loss_d": errD.item(),
            "inception": is_score
        }


    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        nn.BCELoss,
        train_function=train,
        ...
    )
