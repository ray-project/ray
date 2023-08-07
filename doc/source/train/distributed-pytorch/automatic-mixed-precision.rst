
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
