# Most of the tensorflow code is adapted from Tensorflow's tutorial on using CNNs to train MNIST
# https://www.tensorflow.org/versions/r0.9/tutorials/mnist/pros/index.html#build-a-multilayer-convolutional-network

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import argparse

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

import hyperopt

parser = argparse.ArgumentParser(description="Run the hyperparameter optimization example.")
parser.add_argument("--trials", default=2, type=int, help="The number of random trials to do.")
parser.add_argument("--steps", default=10, type=int, help="The number of steps of training to do per network.")

if __name__ == "__main__":
  args = parser.parse_args()

  ray.init(num_workers=10)

  # The number of sets of random hyperparameters to try.
  trials = args.trials
  # The number of training passes over the dataset to use for network.
  steps = args.steps

  # Load the mnist data and turn the data into remote objects.
  print("Downloading the MNIST dataset. This may take a minute.")
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  train_images = ray.put(mnist.train.images)
  train_labels = ray.put(mnist.train.labels)
  validation_images = ray.put(mnist.validation.images)
  validation_labels = ray.put(mnist.validation.labels)

  # Keep track of the best parameters and the best accuracy.
  best_params = None
  best_accuracy = 0
  # This list holds the object IDs for all of the experiments that we have
  # launched and that have not yet been processed.
  remaining_ids = []
  # This is a dictionary mapping the object ID of an experiment to the
  # parameters used for that experiment.
  params_mapping = {}

  # A function for generating random hyperparameters.
  def generate_random_params():
    learning_rate = 10 ** np.random.uniform(-5, 5)
    batch_size = np.random.randint(1, 100)
    dropout = np.random.uniform(0, 1)
    stddev = 10 ** np.random.uniform(-5, 5)
    return {"learning_rate": learning_rate, "batch_size": batch_size, "dropout": dropout, "stddev": stddev}

  # Randomly generate some hyperparameters, and launch a task for each set.
  for i in range(trials):
    params = generate_random_params()
    accuracy_id = hyperopt.train_cnn_and_compute_accuracy.remote(params, steps, train_images, train_labels, validation_images, validation_labels)
    remaining_ids.append(accuracy_id)
    # Keep track of which parameters correspond to this experiment.
    params_mapping[accuracy_id] = params

  # Fetch and print the results of the tasks in the order that they complete.
  for i in range(trials):
    # Use ray.wait to get the object ID of the first task that completes.
    ready_ids, remaining_ids = ray.wait(remaining_ids)
    # Process the output of this task.
    result_id = ready_ids[0]
    params = params_mapping[result_id]
    accuracy = ray.get(result_id)
    print("""We achieve accuracy {:.3}% with
        learning_rate: {:.2}
        batch_size: {}
        dropout: {:.2}
        stddev: {:.2}
      """.format(100 * accuracy, params["learning_rate"], params["batch_size"], params["dropout"], params["stddev"]))
    if accuracy > best_accuracy:
      best_params = params
      best_accuracy = accuracy

  # Record the best performing set of hyperparameters.
  print("""Best accuracy over {} trials was {:.3} with
        learning_rate: {:.2}
        batch_size: {}
        dropout: {:.2}
        stddev: {:.2}
    """.format(trials, 100 * best_accuracy, best_params["learning_rate"], best_params["batch_size"], best_params["dropout"], best_params["stddev"]))
