:orphan:

.. _sgd-v2-docs:

RaySGD: Distributed Training Wrappers
=====================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

RaySGD is a lightweight library for distributed deep learning, allowing you to scale up and speed up training for your deep learning models.

The main features are:

- **Ease of use**: Scale your single process training code to a cluster in just a couple lines of code.
- **Composability**: RaySGD interoperates with :ref:`Ray Tune <tune-main>` to tune your distributed model and :ref:`Ray Datasets <datasets>` to train on large amounts of data.
- **Interactivity**: RaySGD fits in your workflow with support to run from any environment, including seamless Jupyter notebook support.


Intro to RaySGD
---------------

RaySGD is a library that aims to simplify distributed deep learning.

**Frameworks**: RaySGD is built to abstract away the coordination/configuration setup of distributed deep learning frameworks such as Pytorch Distributed and Tensorflow Distributed, allowing users to only focus on implementing training logic.

* For Pytorch, RaySGD automatically handles the construction of the distributed process group.
* For Tensorflow, RaySGD automatically handles the coordination of the ``TF_CONFIG``. The current implementation assumes that the user will use a MultiWorkerMirroredStrategy, but this will change in the near future.
* For Horovod, RaySGD automatically handles the construction of the Horovod runtime and Rendezvous server.

**Built for data scientists/ML practitioners**: RaySGD has support for standard ML tools and features that practitioners love:

* Callbacks for early stopping
* Checkpointing
* Integration with Tensorboard, Weights/Biases, and MLflow
* Jupyter notebooks

**Integration with Ray Ecosystem**: Distributed deep learning often comes with a lot of complexity.


* Use :ref:`Ray Datasets <datasets>` with RaySGD to handle and train on large amounts of data.
* Use :ref:`Ray Tune <tune-main>` with RaySGD to leverage cutting edge hyperparameter techniques and distribute both your training and tuning.
* You can leverage the :ref:`Ray cluster launcher <cluster-cloud>` to launch autoscaling or spot instance clusters to train your model at scale on any cloud.


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
        backend=TorchConfig())

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
