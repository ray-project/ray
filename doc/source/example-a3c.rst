Asynchronous Advantage Actor Critic (A3C)
===========================

This document provides a walkthrough of an implementation of  `A3C`_, 
a state-of-the-art reinforcement learning algorithm. 

.. _`A3C`: https://arxiv.org/abs/1602.01783

Note that this is a modified version of OpenAI's `Universe Starter Agent`_.
The main modifications to the original code are the usage of Ray to start 
new processes as opposed to starting subprocesses and coordinating with the
TensorFlow server and the removal of the ``universe`` dependency. 

.. _`Universe Starter Agent`: https://github.com/openai/universe-starter-agent 

To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow
  pip install six
  pip install gym[atari]
  conda install -y -c https://conda.binstar.org/menpo opencv3
  conda install -y numpy
  conda install -y scipy

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/richardliaw/ray/tree/master/examples/a3c


Code Walkthrough
-------------

There are two main parts to the A3C implementation: the driver and the 
worker. 

.. code-block:: python

  import numpy as np
  import ray

  @ray.remote
  def train_cnn_and_compute_accuracy(hyperparameters,
                                     train_images,
                                     train_labels,
                                     validation_images,
                                     validation_labels):
    # Construct a deep network, train it, and return the accuracy on the
    # validation data.
    return np.random.uniform(0, 1)

Basic random search
-------------------

Something that works surprisingly well is to try random values for the
hyperparameters. For example, we can write a function that randomly generates
hyperparameter configurations.

.. code-block:: python

  def generate_hyperparameters():
    # Randomly choose values for the hyperparameters.
    return {"learning_rate": 10 ** np.random.uniform(-5, 5),
            "batch_size": np.random.randint(1, 100),
            "dropout": np.random.uniform(0, 1),
            "stddev": 10 ** np.random.uniform(-5, 5)}

In addition, let's assume that we've started Ray and loaded some data.

.. code-block:: python

  import ray

  ray.init()

  from tensorflow.examples.tutorials.mnist import input_data
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  train_images = ray.put(mnist.train.images)
  train_labels = ray.put(mnist.train.labels)
  validation_images = ray.put(mnist.validation.images)
  validation_labels = ray.put(mnist.validation.labels)


Then basic random hyperparameter search looks something like this. We launch a
bunch of experiments, and we get the results.

.. code-block:: python

  # Generate a bunch of hyperparameter configurations.
  hyperparameter_configurations = [generate_hyperparameters() for _ in range(20)]

  # Launch some experiments.
  results = []
  for hyperparameters in hyperparameter_configurations:
    results.append(train_cnn_and_compute_accuracy.remote(hyperparameters,
                                                         train_images,
                                                         train_labels,
                                                         validation_images,
                                                         validation_labels))

  # Get the results.
  accuracies = ray.get(results)

Then we can inspect the contents of `accuracies` and see which set of
hyperparameters worked the best. Note that in the above example, the for loop
will run instantaneously and the program will block in the call to ``ray.get``,
which will wait until all of the experiments have finished.

Processing results as they become available
-------------------------------------------

One problem with the above approach is that you have to wait for all of the
experiments to finish before you can process the results. Instead, you may want
to process the results as they become available, perhaps in order to adaptively
choose new experiments to run, or perhaps simply so you know how well the
experiments are doing. To process the results as they become available, we can
use the ``ray.wait`` primitive.

The most simple usage is the following. This example is implemented in more
detail in driver.py_.

.. code-block:: python

  # Launch some experiments.
  remaining_ids = []
  for hyperparameters in hyperparameter_configurations:
    remaining_ids.append(train_cnn_and_compute_accuracy.remote(hyperparameters,
                                                               train_images,
                                                               train_labels,
                                                               validation_images,
                                                               validation_labels))

  # Whenever a new experiment finishes, print the value and start a new
  # experiment.
  for i in range(100):
    ready_ids, remaining_ids = ray.wait(remaining_ids, num_returns=1)
    accuracy = ray.get(ready_ids[0])
    print("Accuracy is {}".format(accuracy))
    # Start a new experiment.
    new_hyperparameters = generate_hyperparameters()
    remaining_ids.append(train_cnn_and_compute_accuracy.remote(new_hyperparameters,
                                                               train_images,
                                                               train_labels,
                                                               validation_images,
                                                               validation_labels))

.. _driver.py: https://github.com/ray-project/ray/blob/master/examples/hyperopt/driver.py


