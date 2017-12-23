Hyperparameter Optimization
===========================

This document provides a walkthrough of the hyperparameter optimization example.

.. note::

     To learn about Ray's built-in hyperparameter optimization framework, see `Ray.tune <http://ray.readthedocs.io/en/latest/tune.html>`__.

To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/hyperopt

The simple script that processes results as they become available and launches
new experiments can be run as follows.

.. code-block:: bash

  python ray/examples/hyperopt/hyperopt_simple.py --trials=5 --steps=10

The variant that divides training into multiple segments and aggressively
terminates poorly performing models can be run as follows.

.. code-block:: bash

  python ray/examples/hyperopt/hyperopt_adaptive.py --num-starting-segments=5 \
                                                    --num-segments=10 \
                                                    --steps-per-segment=20

Machine learning algorithms often have a number of *hyperparameters* whose
values must be chosen by the practitioner. For example, an optimization
algorithm may have a step size, a decay rate, and a regularization coefficient.
In a deep network, the network parameterization itself (e.g., the number of
layers and the number of units per layer) can be considered a hyperparameter.

Choosing these parameters can be challenging, and so a common practice is to
search over the space of hyperparameters. One approach that works surprisingly
well is to randomly sample different options.

Problem Setup
-------------

Suppose that we want to train a convolutional network, but we aren't sure how to
choose the following hyperparameters:

- the learning rate
- the batch size
- the dropout probability
- the standard deviation of the distribution from which to initialize the
  network weights

Suppose that we've defined a remote function ``train_cnn_and_compute_accuracy``,
which takes values for these hyperparameters as its input (along with the
dataset), trains a convolutional network using those hyperparameters, and
returns the accuracy of the trained model on a validation set.

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

More sophisticated hyperparameter search
----------------------------------------

Hyperparameter search algorithms can get much more sophisticated. So far, we've
been treating the function ``train_cnn_and_compute_accuracy`` as a black box,
that we can choose its inputs and inspect its outputs, but once we decide to run
it, we have to run it until it finishes.

However, there is often more structure to be exploited. For example, if the
training procedure is going poorly, we can end the session early and invest more
resources in the more promising hyperparameter experiments. And if we've saved
the state of the training procedure, we can always restart it again later.

This is one of the ideas of the Hyperband_ algorithm. Start with a huge number
of hyperparameter configurations, aggressively stop the bad ones, and invest
more resources in the promising experiments.

To implement this, we can first adapt our training method to optionally take a
model and to return the updated model.

.. code-block:: python

  @ray.remote
  def train_cnn_and_compute_accuracy(hyperparameters, model=None):
      # Construct a deep network, train it, and return the accuracy on the
      # validation data as well as the latest version of the model. If the model
      # argument is not None, this will continue training an existing model.
      validation_accuracy = np.random.uniform(0, 1)
      new_model = model
      return validation_accuracy, new_model

Here's a different variant that uses the same principles. Divide each training
session into a series of shorter training sessions. Whenever a short session
finishes, if it still looks promising, then continue running it. If it isn't
doing well, then terminate it and start a new experiment.

.. code-block:: python

  import numpy as np

  def is_promising(model):
      # Return true if the model is doing well and false otherwise. In practice,
      # this function will want more information than just the model.
      return np.random.choice([True, False])

  # Start 10 experiments.
  remaining_ids = []
  for _ in range(10):
      experiment_id = train_cnn_and_compute_accuracy.remote(hyperparameters, model=None)
      remaining_ids.append(experiment_id)

  accuracies = []
  for i in range(100):
      # Whenever a segment of an experiment finishes, decide if it looks promising
      # or not.
      ready_ids, remaining_ids = ray.wait(remaining_ids, num_returns=1)
      experiment_id = ready_ids[0]
      current_accuracy, current_model = ray.get(experiment_id)
      accuracies.append(current_accuracy)

      if is_promising(experiment_id):
          # Continue running the experiment.
          experiment_id = train_cnn_and_compute_accuracy.remote(hyperparameters,
                                                                model=current_model)
      else:
          # Start a new experiment.
          experiment_id = train_cnn_and_compute_accuracy.remote(hyperparameters)

      remaining_ids.append(experiment_id)

.. _Hyperband: https://arxiv.org/abs/1603.06560
