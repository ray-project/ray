Distributed PyTorch
===================

The RaySGD ``PyTorchTrainer`` simplifies distributed model training for PyTorch. The ``PyTorchTrainer`` is a wrapper around ``torch.distributed.launch`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to needing to wrap your training code in bash scripts.

Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``), each of which is managed by a Ray actor.

.. image:: raysgd-actors.svg
    :align: center

For end to end examples leveraging RaySGD PyTorchTrainer, jump to :ref:`raysgd-pytorch-examples`.

.. contents:: :local:

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


Now that the trainer is constructed, here's how to train the model.

.. code-block:: python

    for i in range(10):
        metrics = trainer.train()
        val_metrics = trainer.validate()


Each ``train`` call makes one pass over the training data, and each ``validate`` call runs the model on the validation data passed in by the ``data_creator``. Provide a custom training operator (:ref:`raysgd-custom-training`) to calculate custom metrics or customize the training/validation process.

After training, you may want to reappropriate the Ray cluster. To release Ray resources obtained by the Trainer:

.. code-block:: python

    trainer.shutdown()

.. note:: Be sure to call ``trainer.save()`` or ``trainer.get_model()`` before shutting down.

See the documentation on the PyTorchTrainer here: :ref:`ref-pytorch-trainer`.


.. _raysgd-custom-training:

Custom Training and Validation (Operators)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``PyTorchTrainer`` allows you to run a custom training and validation loops in parallel on each worker, providing a flexible interface similar to using PyTorch natively.
This is done via the :ref:`ref-pytorch-operator` interface.

For both training and validation, there are two granularities that you can provide customization - per epoch and per batch. These correspond to ``train_batch``,
``train_epoch``, ``validate``, and ``validate_batch``. Other useful methods to override include ``setup``, ``save`` and ``restore``. You can use these
to manage state (like a classifier neural network for calculating inception score, or a heavy tokenizer).

Providing a custom operator is necessary if creator functions return multiple models, optimizers, or schedulers.

Below is a partial example of a custom ``TrainingOperator`` that provides a ``train_batch`` implementation for a Deep Convolutional GAN.

.. code-block:: python

    import torch
    from ray.util.sgd.pytorch import TrainingOperator

    class GANOperator(TrainingOperator):
        def setup(self, config):
            """Custom setup for this operator.

            Args:
                config (dict): Custom configuration value to be passed to
                    all creator and operator constructors. Same as ``self.config``.
            """
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        def train_batch(self, batch, batch_info):
            """Trains on one batch of data from the data creator.

            Example taken from:
                https://github.com/eriklindernoren/PyTorch-GAN/blob/
                a163b82beff3d01688d8315a3fd39080400e7c01/implementations/dcgan/dcgan.py

            Args:
                batch: One item of the validation iterator.
                batch_info (dict): Information dict passed in from ``train_epoch``.

            Returns:
                A dict of metrics. Defaults to "loss" and "num_samples",
                    corresponding to the total number of datapoints in the batch.
            """
            Tensor = torch.cuda.FloatTensor if cuda else torch.FloatTensor
            discriminator, generator = self.models
            optimizer_D, optimizer_G = self.optimizers

            # Adversarial ground truths
            valid = Variable(Tensor(batch.shape[0], 1).fill_(1.0), requires_grad=False)
            fake = Variable(Tensor(batch.shape[0], 1).fill_(0.0), requires_grad=False)

            # Configure input
            real_imgs = Variable(batch.type(Tensor))

            # -----------------
            #  Train Generator
            # -----------------

            optimizer_G.zero_grad()

            # Sample noise as generator input
            z = Variable(Tensor(np.random.normal(0, 1, (
                    batch.shape[0], self.config["latent_dim"]))))

            # Generate a batch of images
            gen_imgs = generator(z)

            # Loss measures generator's ability to fool the discriminator
            g_loss = adversarial_loss(discriminator(gen_imgs), valid)

            g_loss.backward()
            optimizer_G.step()

            # ---------------------
            #  Train Discriminator
            # ---------------------

            optimizer_D.zero_grad()

            # Measure discriminator's ability to classify real from generated samples
            real_loss = adversarial_loss(discriminator(real_imgs), valid)
            fake_loss = adversarial_loss(discriminator(gen_imgs.detach()), fake)
            d_loss = (real_loss + fake_loss) / 2

            d_loss.backward()
            optimizer_D.step()

            return {
                "loss_g": g_loss.item(),
                "loss_d": d_loss.item(),
                "num_samples": imgs.shape[0]
            }

    trainer = PyTorchTrainer(
            model_creator,
            data_creator,
            optimizer_creator,
            nn.BCELoss,
            training_operator_cls=GANOperator,
            num_replicas=num_replicas,
            config=config,
            use_gpu=True,
            batch_size=128)

    for i in range(5):
        stats = trainer.train()
        print(stats)

See the `DCGAN example <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/dcgan.py>`__ for an end to end example. It constructs two models and two optimizers and uses a custom training operator to provide a non-standard training loop.


Initialization Functions
------------------------

Use the ``initialization_hook`` parameter to initialize state on each worker process when they are started. This is useful when setting an environment variable:

.. code-block:: python

    def initialization_hook():
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


Retrieving the model
--------------------

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

Note that if using a custom training operator (:ref:`raysgd-custom-training`), you will need to manage loss scaling manually.


Distributed Multi-node Training
-------------------------------

You can scale your training to multiple nodes without making any modifications to your training code.

To train across a cluster, first make sure that the Ray cluster is started. You can start a Ray cluster `via the Ray cluster launcher <autoscaling.html>`_ or `manually <using-ray-on-a-cluster.html>`_.

Then, in your program, you'll need to connect to this cluster via ``ray.init``:

.. code-block:: python

    ray.init(address="auto")  # or a specific redis address of the form "ip-address:port"

After connecting, you can scale up the number of workers seamlessly across multiple nodes:

.. code-block:: python

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=100
    )
    trainer.train()
    model = trainer.get_model()


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

In certain scenarios, such as training GANs, you may want to use multiple models in the training loop. You can do this in the ``PyTorchTrainer`` by allowing the ``model_creator``, ``optimizer_creator``, and ``scheduler_creator`` to return multiple values. Provide a custom TrainingOperator (:ref:`raysgd-custom-training`) to train across multiple models.

You can see the `DCGAN script <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/dcgan.py>`_ for an end-to-end example.

.. code-block:: python

    from ray.util.sgd.pytorch import PyTorchTrainer, TrainingOperator

    def train(*, model=None, criterion=None, optimizer=None, dataloader=None):
        model.train()
        train_loss = 0
        correct = 0
        total = 0
        for batch_idx, (inputs, targets) in enumerate(dataloader):
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()

            train_loss += loss.item()
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()
        return {
            "accuracy": correct / total,
            "train_loss": train_loss / (batch_idx + 1)
        }

    def model_creator(config):
        return Discriminator(), Generator()

    def optimizer_creator(models, config):
        net_d, net_g = models
        discriminator_opt = optim.Adam(
            net_d.parameters(), lr=config.get("lr", 0.01), betas=(0.5, 0.999))
        generator_opt = optim.Adam(
            net_g.parameters(), lr=config.get("lr", 0.01), betas=(0.5, 0.999))
        return discriminator_opt, generator_opt

    class CustomOperator(TrainingOperator):
        def train_epoch(self, dataloader, info):
            result = {}
            for i, (model, optimizer) in enumerate(
                    zip(self.models, self.optimizers)):
                result["model_{}".format(i)] = train(
                    model=model,
                    criterion=self.criterion,
                    optimizer=optimizer,
                    dataloader=dataloader)
            return result

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.BCELoss,
        training_operator_cls=CustomOperator)

    trainer.train()


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
   Training a ResNet18 model on CIFAR10.

- `DCGAN example <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/pytorch/examples/dcgan.py>`__:
   Training a Deep Convolutional GAN on MNIST. It constructs two models and two optimizers and uses a custom training operator.
