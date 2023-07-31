Advanced topics
===============

.. _torch-amp:

Automatic Mixed Precision
-------------------------

Automatic mixed precision (AMP) lets you train your models faster by using a lower
precision datatype for operations like linear layers and convolutions.

.. tab-set::

    .. tab-item:: PyTorch

        You can train your Torch model with AMP by:

        1. Adding :func:`ray.train.torch.accelerate` with ``amp=True`` to the top of your training function.
        2. Wrapping your optimizer with :func:`ray.train.torch.prepare_optimizer`.
        3. Replacing your backward call with :func:`ray.train.torch.backward`.

        .. code-block:: diff

             def train_func():
            +    train.torch.accelerate(amp=True)

                 model = NeuralNetwork()
                 model = train.torch.prepare_model(model)

                 data_loader = DataLoader(my_dataset, batch_size=worker_batch_size)
                 data_loader = train.torch.prepare_data_loader(data_loader)

                 optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
            +    optimizer = train.torch.prepare_optimizer(optimizer)

                 model.train()
                 for epoch in range(90):
                     for images, targets in dataloader:
                         optimizer.zero_grad()

                         outputs = model(images)
                         loss = torch.nn.functional.cross_entropy(outputs, targets)

            -            loss.backward()
            +            train.torch.backward(loss)
                         optimizer.step()
                ...


.. note:: The performance of AMP varies based on GPU architecture, model type,
        and data shape. For certain workflows, AMP may perform worse than
        full-precision training.

.. _train-reproducibility:

Reproducibility
---------------

.. tab-set::

    .. tab-item:: PyTorch

        To limit sources of nondeterministic behavior, add
        :func:`ray.train.torch.enable_reproducibility` to the top of your training
        function.

        .. code-block:: diff

             def train_func():
            +    train.torch.enable_reproducibility()

                 model = NeuralNetwork()
                 model = train.torch.prepare_model(model)

                 ...

        .. warning:: :func:`ray.train.torch.enable_reproducibility` can't guarantee
            completely reproducible results across executions. To learn more, read
            the `PyTorch notes on randomness <https://pytorch.org/docs/stable/notes/randomness.html>`_.

..
    import ray
    from ray import tune

    def training_func(config):
        dataloader = ray.train.get_dataset()\
            .get_shard(torch.rank())\
            .iter_torch_batches(batch_size=config["batch_size"])

        for i in config["epochs"]:
            ray.train.report(...)  # use same intermediate reporting API

    # Declare the specification for training.
    trainer = Trainer(backend="torch", num_workers=12, use_gpu=True)
    dataset = ray.dataset.window()

    # Convert this to a trainable.
    trainable = trainer.to_tune_trainable(training_func, dataset=dataset)

    tuner = tune.Tuner(trainable,
        param_space={"lr": tune.uniform(), "batch_size": tune.randint(1, 2, 3)},
        tune_config=tune.TuneConfig(num_samples=12))
    results = tuner.fit()
