Distributed PyTorch
===================

The RaySGD ``PyTorchTrainer`` simplifies distributed model training for PyTorch. The ``PyTorchTrainer`` is a wrapper around ``torch.distributed.launch`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to needing to wrap your training code in bash scripts.

Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``), each of which is managed by a Ray actor.

.. image:: raysgd-actors.svg
    :align: center

For end to end examples leveraging RaySGD PyTorchTrainer, jump to :ref:`raysgd-pytorch-examples`.

Setting up training
-------------------

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/26EMwdahdgm7Lscy9>`_!

The ``PyTorchTrainer`` can be constructed with functions that wrap components of the training script. Specifically, it requires constructors for the Model, Data, Optimizer, Loss, and ``lr_scheduler`` to create replicated copies across different devices and machines.

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_trainer_start__
   :end-before: __torch_trainer_end__

The below section covers the expected signatures of creator functions. Jump to :ref:`starting-pytorch-trainer`.

Model Creator
~~~~~~~~~~~~~

This is the signature needed for ``PyTorchTrainer(model_creator=...)``.

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_model_start__
   :end-before: __torch_model_end__


Optimizer Creator
~~~~~~~~~~~~~~~~~

This is the signature needed for ``PyTorchTrainer(optimizer_creator=...)``.

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_optimizer_start__
   :end-before: __torch_optimizer_end__



Data Creator
~~~~~~~~~~~~

This is the signature needed for ``PyTorchTrainer(data_creator=...)``.

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_data_start__
   :end-before: __torch_data_end__



Loss Creator
~~~~~~~~~~~~

This is the signature needed for ``PyTorchTrainer(loss_creator=...)``.

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_loss_start__
   :end-before: __torch_loss_end__


Scheduler Creator
~~~~~~~~~~~~~~~~~

Optionally, you can provide a creator function for the learning rate scheduler. This is the signature needed
for ``PyTorchTrainer(scheduler_creator=...)``.

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_scheduler_start__
   :end-before: __torch_scheduler_end__

.. _starting-pytorch-trainer:

Putting things together
~~~~~~~~~~~~~~~~~~~~~~~

Before instantiating the trainer, first start or connect to a Ray cluster:

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_ray_start__
   :end-before: __torch_ray_end__

Instantiate the trainer object:

.. literalinclude:: ../../examples/doc_code/raysgd_torch_signatures.py
   :language: python
   :start-after: __torch_trainer_start__
   :end-before: __torch_trainer_end__

You can also set the number of workers and whether the workers will use GPUs:

.. code-block:: python
    :emphasize-lines: 8,9

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        scheduler_creator=scheduler_creator,
        config={"lr": 0.001},
        num_replicas=100,
        use_gpu=True)

See the documentation on the PyTorchTrainer here: :ref:`ref-pytorch-trainer`. We'll look at the training APIs next.

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

.. code-block:: python

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

.. code-block:: python

    trainer.train()
    model = trainer.get_model()  # Returns multiple models if the model_creator does.

Mixed Precision (FP16) Training
-------------------------------

You can enable mixed precision training for PyTorch with the ``use_fp16`` flag. This automatically converts the model(s) and optimizer(s) to train using mixed-precision. This requires NVIDIA ``Apex``, which can be installed from `the NVIDIA/Apex repository <https://github.com/NVIDIA/apex#quick-start>`_:

.. code-block:: python
    :emphasize-lines: 7

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=4,
        use_fp16=True
    )

``Apex`` is a Pytorch extension with NVIDIA-maintained utilities to streamline mixed precision and distributed training. When ``use_fp16=True``,
you should not manually cast your model or data to ``.half()``. The flag informs the Trainer to call ``amp.initialize`` on the created models and optimizers and optimize using the scaled loss: ``amp.scale_loss(loss, optimizer)``.

To specify particular parameters for ``amp.initialize``, you can use the ``apex_args`` field for the PyTorchTrainer constructor. Valid arguments can be found on the `Apex documentation <https://nvidia.github.io/apex/amp.html#apex.amp.initialize>`_:

.. code-block:: python
    :emphasize-lines: 7-12

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

Then, within ``train.py`` you can scale up the number of workers seamlessly across multiple nodes:

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

.. code-block:: python

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

.. code-block:: python

    trainer.train(max_retries=N, checkpoint="auto")


Advanced: Hyperparameter Tuning
-------------------------------

``PyTorchTrainer`` naturally integrates with Tune via the ``PyTorchTrainable`` interface. The same arguments to ``PyTorchTrainer`` should be passed into the ``tune.run(config=...)`` as shown below.

.. literalinclude:: ../../../python/ray/util/sgd/pytorch/examples/tune_example.py
   :language: python
   :start-after: __torch_tune_example__


Simultaneous Multi-model Training
---------------------------------

In certain scenarios such as training GANs, you may want to use multiple models in the training loop. You can do this in the ``PyTorchTrainer`` by allowing the ``model_creator``, ``optimizer_creator``, and ``scheduler_creator`` to return multiple values.

If multiple models, optimizers, or schedulers are returned, you will need to provide a custom training function (and custom validation function if you plan to call ``validate``).

You can see the `DCGAN script <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/dcgan.py>`_ for an end-to-end example.

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

Note that this is needed if the model creator returns multiple models, optimizers, or schedulers.

.. code-block:: python

    def train(config, model, train_iterator, criterion, optimizer, scheduler=None):
        """Runs one standard training pass over the train_iterator.

        Raises:
            ValueError if multiple models/optimizers/schedulers are provided. You
                are expected to have a custom training function if you wish
                to use multiple models/optimizers/schedulers.

        Args:
            config: (dict): A user configuration provided into the Trainer
                constructor.
            model: The model(s) as created by the model_creator.
            train_iterator: An iterator created from the DataLoader which
                wraps the provided Dataset.
            criterion: The loss object created by the loss_creator.
            optimizer: The torch.optim.Optimizer(s) object
                as created by the optimizer_creator.
            scheduler (optional): The torch.optim.lr_scheduler(s) object
                as created by the scheduler_creator.

        Returns:
            A dict of metrics from training.
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


    def custom_validate(config, model, val_iterator, criterion, scheduler=None):
        """Runs one standard validation pass over the val_iterator.

        Args:
            config: (dict): A user configuration provided into the Trainer
                constructor.
            model: The model(s) as created by the model_creator.
            train_iterator: An iterator created from the DataLoader which
                wraps the provided Dataset.
            criterion: The loss object created by the loss_creator.
            scheduler (optional): The torch.optim.lr_scheduler object(s)
                as created by the scheduler_creator.

        Returns:
            A dict of metrics from the evaluation.
        """
        ...
        return {"validation_accuracy": 0.5}


    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        nn.BCELoss,
        train_function=train,
        validation_function=custom_validate,
        ...
    )

Feature Requests
----------------

Have features that you'd really like to see in RaySGD? Feel free to `open an issue <https://github.com/ray-project/ray>`_.

.. _raysgd-pytorch-examples:

PyTorchTrainer Examples
-----------------------

Here are some examples of using RaySGD for training PyTorch models. If you'd like
to contribute an example, feel free to create a `pull request here <https://github.com/ray-project/ray/>`_.

- `PyTorch training example <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/train_example.py>`__:
   Simple example of using Ray's PyTorchTrainer.

- `CIFAR10 example <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/cifar_pytorch_example.py>`__:
   Training a ResNet18 model on CIFAR10. It uses a custom training
   function, a custom validation function, and custom initialization code for each worker.

- `DCGAN example <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/dcgan.py>`__:
   Training a Deep Convolutional GAN on MNIST. It constructs
   two models and two optimizers and uses a custom training and validation function.
