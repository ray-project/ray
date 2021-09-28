.. _sgd-v2-docs:

RaySGD: Deep Learning on Ray
=============================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/PXFcJmHwszCwQhqX7>`_!

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

RaySGD abstracts away the complexity of setting up a distributed training
system. Let's take following simple examples:

.. tabs::

  .. group-tab:: PyTorch

    This example shows how you can use RaySGD with PyTorch.

    First, set up your dataset and model.

    .. code-block:: python

        import torch
        import torch.nn as nn

        num_samples = 20
        input_size = 10
        layer_size = 15
        output_size = 5

        class NeuralNetwork(nn.Module):
            def __init__(self):
                super(NeuralNetwork, self).__init__()
                self.layer1 = nn.Linear(input_size, layer_size)
                self.relu = nn.ReLU()
                self.layer2 = nn.Linear(layer_size, output_size)

            def forward(self, input):
                return self.layer2(self.relu(self.layer1(input)))

        # In this example we use a randomly generated dataset.
        input = torch.randn(num_samples, input_size)
        labels = torch.randn(num_samples, output_size)


    Now define your single-worker PyTorch training function.

    .. code-block:: python

        import torch.optim as optim

        def train_func():
            num_epochs = 3
            model = NeuralNetwork()
            loss_fn = nn.MSELoss()
            optimizer = optim.SGD(model.parameters(), lr=0.1)

            for epoch in range(num_epochs):
                output = model(input)
                loss = loss_fn(output, labels)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                print(f"epoch: {epoch}, loss: {loss.item()}")


    This training function can be executed with:

    .. code-block:: python

        train_func()


    Now let's convert this to a distributed multi-worker training function!

    First, update the training function code to use PyTorch's
    ``DistributedDataParallel``. With RaySGD, you just pass in your distributed
    data parallel code as as you would normally run it with
    ``torch.distributed.launch``.

    .. code-block:: python

        from torch.nn.parallel import DistributedDataParallel

        def train_func_distributed():
            num_epochs = 3
            model = NeuralNetwork()
            model = DistributedDataParallel(model)
            loss_fn = nn.MSELoss()
            optimizer = optim.SGD(model.parameters(), lr=0.1)

            for epoch in range(num_epochs):
                output = model(input)
                loss = loss_fn(output, labels)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                print(f"epoch: {epoch}, loss: {loss.item()}")

    Then, instantiate a ``Trainer`` that uses a ``"torch"`` backend
    with 4 workers, and use it to run the new training function!

    .. code-block:: python

        from ray.sgd import Trainer

        trainer = Trainer(backend="torch", num_workers=4)
        trainer.start()
        results = trainer.run(train_func_distributed)
        trainer.shutdown()


    See :ref:`sgd-porting-code` for a more comprehensive example.


  .. group-tab:: TensorFlow

    This example shows how you can use RaySGD to set up `Multi-worker training
    with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_.

    First, set up your dataset and model.

    .. code-block:: python

        import numpy as np
        import tensorflow as tf

        def mnist_dataset(batch_size):
            (x_train, y_train), _ = tf.keras.datasets.mnist.load_data()
            # The `x` arrays are in uint8 and have values in the [0, 255] range.
            # You need to convert them to float32 with values in the [0, 1] range.
            x_train = x_train / np.float32(255)
            y_train = y_train.astype(np.int64)
            train_dataset = tf.data.Dataset.from_tensor_slices(
                (x_train, y_train)).shuffle(60000).repeat().batch(batch_size)
            return train_dataset


        def build_and_compile_cnn_model():
            model = tf.keras.Sequential([
                tf.keras.layers.InputLayer(input_shape=(28, 28)),
                tf.keras.layers.Reshape(target_shape=(28, 28, 1)),
                tf.keras.layers.Conv2D(32, 3, activation='relu'),
                tf.keras.layers.Flatten(),
                tf.keras.layers.Dense(128, activation='relu'),
                tf.keras.layers.Dense(10)
            ])
            model.compile(
                loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                optimizer=tf.keras.optimizers.SGD(learning_rate=0.001),
                metrics=['accuracy'])
            return model

    Now define your single-worker TensorFlow training function.

    .. code-block:: python

        def train_func():
            batch_size = 64
            single_worker_dataset = mnist.mnist_dataset(batch_size)
            single_worker_model = mnist.build_and_compile_cnn_model()
            single_worker_model.fit(single_worker_dataset, epochs=3, steps_per_epoch=70)

    This training function can be executed with:

    .. code-block:: python

        train_func()

    Now let's convert this to a distributed multi-worker training function!
    All you need to do is:

    1. Set the *global* batch size - each worker will process the same size
       batch as in the single-worker code.
    2. Choose your TensorFlow distributed training strategy. In this example
       we use the ``MultiWorkerMirroredStrategy``.

    .. code-block:: python

        import json
        import os

        def train_func_distributed():
            per_worker_batch_size = 64
            # This environment variable will be set by Ray SGD.
            tf_config = json.loads(os.environ['TF_CONFIG'])
            num_workers = len(tf_config['cluster']['worker'])

            strategy = tf.distribute.MultiWorkerMirroredStrategy()

            global_batch_size = per_worker_batch_size * num_workers
            multi_worker_dataset = mnist_dataset(global_batch_size)

            with strategy.scope():
                # Model building/compiling need to be within `strategy.scope()`.
                multi_worker_model = build_and_compile_cnn_model()

            multi_worker_model.fit(multi_worker_dataset, epochs=3, steps_per_epoch=70)

    Then, instantiate a ``Trainer`` that uses a ``"tensorflow"`` backend
    with 4 workers, and use it to run the new training function!

    .. code-block:: python

        from ray.sgd import Trainer

        trainer = Trainer(backend="tensorflow", num_workers=4)
        trainer.start()
        results = trainer.run(train_func_distributed)
        trainer.shutdown()


    See :ref:`sgd-porting-code` for a more comprehensive example.


**Next steps:** Check out the :ref:`User Guide <sgd-user-guide>`!
