.. _sgd-v2-docs:

RaySGD: Deep Learning on Ray
=============================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

RaySGD is a lightweight library for distributed deep learning, allowing you
to scale up and speed up training for your deep learning models.

The main features are:

- **Ease of use**: Scale your single process training code to a cluster in just a couple lines of code.
- **Composability**: RaySGD interoperates with :ref:`Ray Tune <tune-main>` to tune your distributed model and :ref:`Ray Datasets <datasets>` to train on large amounts of data.
- **Interactivity**: RaySGD fits in your workflow with support to run from any environment, including seamless Jupyter notebook support.

.. note::

  This API is in its Alpha release (as of Ray 1.7) and may be revised in
  future Ray releases. If you encounter any bugs, please file an
  `issue on GitHub`_.
  If you are looking for the previous API documentation, see :ref:`sgd-index`.


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


Quick Start
-----------

.. TODO: make this runnable and show both Tensorflow/Torch

RaySGD abstracts away the complexity of setting up a distributed training system. Let's take this simple example function:

.. code-block:: python

    class Net(nn.Module):
        def __init__(self):
            super(Net, self).__init__()
            self.fc1 = nn.Linear(1, 128)
            self.fc2 = nn.Linear(128, 1)

        def forward(self, x):
            x = self.fc1(x)
            x = F.relu(x)
            x = self.fc2(x)
            return x

    def train_func():
        model = Net()
        for x in data:
            results = model(x)
        return results

We can simply construct the trainer function and specify the backend we want (torch, tensorflow, or horovod) and the number of workers to use for training:

.. code-block:: python

    from ray.util.sgd.v2 import Trainer

    trainer = Trainer(backend = "torch", num_workers=2)

Then, we can pass the function to the trainer. This will cause the trainer to start the necessary processes and execute the training function:

.. code-block:: python


    trainer.start()
    results = trainer.run(train_func)
    print(results)

Now, let's leverage Pytorch's Distributed Data Parallel. With RaySGD, you
just pass in your distributed data parallel code as as you would normally run
it with ``torch.distributed.launch``:

.. code-block:: python

    import torch.nn as nn
    from torch.nn.parallel import DistributedDataParallel
    import torch.optim as optim

    def train_simple(config: Dict):

        # N is batch size; D_in is input dimension;
        # H is hidden dimension; D_out is output dimension.
        N, D_in, H, D_out = 8, 5, 5, 5

        # Create random Tensors to hold inputs and outputs
        x = torch.randn(N, D_in)
        y = torch.randn(N, D_out)
        loss_fn = nn.MSELoss()

        # Use the nn package to define our model and loss function.
        model = torch.nn.Sequential(
            torch.nn.Linear(D_in, H),
            torch.nn.ReLU(),
            torch.nn.Linear(H, D_out),
        )
        optimizer = optim.SGD(model.parameters(), lr=0.1)

        model = DistributedDataParallel(model)
        results = []

        for epoch in range(config.get("epochs", 10)):
            optimizer.zero_grad()
            output = model(x)
            loss = loss_fn(output, y)
            loss.backward()
            results.append(loss.item())
            optimizer.step()
        return results

Running this with RaySGD is as simple as the following:

.. code-block:: python

    all_results = trainer.run(train_simple)


**Next steps:** Check out the :ref:`User Guide <sgd-user-guide>`!
