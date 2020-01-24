RaySGD Pytorch
==============

.. warning:: This is still an experimental API and is subject to change in the near future.

.. tip:: Help us make RaySGD better; take this 1 minute `User Survey <https://forms.gle/26EMwdahdgm7Lscy9>`_!

The RaySGD ``PyTorchTrainer`` simplifies distributed model training for PyTorch. The ``PyTorchTrainer`` is a wrapper around ``torch.distributed.launch`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to needing to execute training outside of Python.

Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``) which are each managed by a Ray actor.

Setting up training
-------------------

The ``PyTorchTrainer`` can be constructed with functions that construct components of the training script. Specifically, it needs constructors for the Model, Data, Optimizer, and Loss. This is so that we can create replicated copies of the neural network/optimizer across different devices and machines.

For example:

.. code-block:: python

    def model_creator(config):
        """Constructor function for the model(s) to be optimized.

        Args:
            config (dict): Configuration dictionary passed into ``PyTorchTrainer``.

        Returns:
            One or more torch.nn.Module objects.
        """
        return nn.Linear(1, 1)


    def optimizer_creator(model, config):
        """Constructor of the optimizer.

        Args:
            model: The return values from ``model_creator``. This can be one
                or more torch neural networks.
            config (dict): Configuration dictionary passed into ``PyTorchTrainer``.

        Returns:
            One or more Torch optimizer objects. You must return as many optimizers
            as you have models.
        """
      return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


    def data_creator(config):
      """Returns training dataset, validation dataset."""
      return LinearDataset(2, 5), LinearDataset(2, 5, size=400)

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
        config={"lr": 0.001})

You can also set the number of workers and whether the workers are using GPU:

.. code-block:: python

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        config={"lr": 0.001}
        num_replicas=100,
        use_gpu=True,)

Shutting down training
~~~~~~~~~~~~~~~~~~~~~~

After training, you may want to reappropriate the Ray cluster. To release Ray resources obtained by the trainer:

.. code-block:: python

    trainer.shutdown()

.. note:: Be sure to call ``save`` or ``get_model`` before shutting down.

Training APIs
-------------

Now that the trainer is constructed, you'll naturally want to train the model.

.. code-block:: python

    trainer.train()

This takes one pass over the training data.

To run the model on the validation data, call:

.. code-block:: python

    trainer.validate()

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
        loss_creator=lambda config: nn.MSELoss(),
        num_replicas=num_replicas)
    trainer_2.restore(checkpoint_path)


Exporting a model for inference
-------------------------------

The trained torch model can be extracted for use within the same Python program with ``trainer.get_model()``. This will load the state dictionary of the model(s).

.. code-block::

    trainer.train()
    model = trainer.get_model()


Distributed Multi-node Training
-------------------------------

You can scale out your training onto multiple nodes without making any modifications to your training code. To train across a cluster, simply make sure that the Ray cluster is started.

You can start a Ray cluster `via the Ray cluster launcher <autoscaling.html>`_ or `manually <using-ray-on-a-cluster.html>`_.

.. code-block:: bash

    ray up CLUSTER.yaml
    python train.py --address="auto"

Then, you'll be able to scale up the number of workers seamlessly across multiple nodes:

.. code-block:: python

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
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

In certain scenarios such as training GANs, you may want to use multiple models in the training loop. You can do this in the ``PyTorchTrainer`` by allowing the ``model_creator`` and the ``optimizer_creator`` to return multiple values. You can see the `DCGAN script <https://github.com/ray-project/ray/blob/master/python/ray/experimental/sgd/pytorch/examples/dcgan.py>`_ for an end-to-end example.

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


    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        nn.BCELoss)


Custom Training and Validation Functions
----------------------------------------

``PyTorchTrainer`` allows you to run a custom training and validation in parallel on each worker, providing a flexibility similar to using PyTorch natively. This is done via the ``train_function`` and ``validation_function`` parameters.

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
