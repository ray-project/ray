:orphan:

.. _sgd-v2-docs:

RaySGD: Distributed Training Wrappers
=====================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

RaySGD is a lightweight library for distributed deep learning, allowing you to scale up and speed up training for your deep learning models.

The main features are:

- **Ease of use**: Scale your single process training code to a cluster in just a couple lines of code.
- **Composability**: RaySGD interoperates with Ray Tune to allow you to easily do hyperparameter tuning at scale, and Ray Datasets to allow you to train over large amounts of data.
- **Interactivity**: RaySGD fits in your workflow with support to run from any environment, including seamless Jupyter notebook support.


Quickstart
----------

You can run the following on your local machine:

.. code-block:: python

    import torch

    def train_func(config=None):
        use_cuda = torch.cuda.is_available()
        device = torch.device("cuda" if use_cuda else "cpu")
        train_loader, test_loader = get_data_loaders()
        model = ConvNet().to(device)
        optimizer = optim.SGD(model.parameters(), lr=0.1)
        model = DistributedDataParallel(model)
        all_results = []

        for epoch in range(40):
            train(model, optimizer, train_loader, device)
            acc = test(model, test_loader, device)
            all_results.append(acc)

        return model._module, all_results

    trainer = Trainer(
        num_workers=8,
        use_gpu=True,
        backend_args=TorchConfig())

    print(trainer)
    # prints a table of resource usage

    model = trainer.run(train_func)  # scale out here!

Links
-----

* :ref:`API reference <sgd-api>`
* :ref:`User guide <sgd-user-guide>`
* :ref:`Architecture <sgd-arch>`
* :ref:`Examples <sgd-v2-examples>`


**Next steps:** Check out the :ref:`user guide here <sgd-user-guide>`