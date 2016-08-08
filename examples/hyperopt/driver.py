# Most of the tensorflow code is adapted from Tensorflow's tutorial on using CNNs to train MNIST
# https://www.tensorflow.org/versions/r0.9/tutorials/mnist/pros/index.html#build-a-multilayer-convolutional-network
import numpy as np
import ray
import argparse

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

import hyperopt

parser = argparse.ArgumentParser(description="Run the hyperparameter optimization example.")
parser.add_argument("--node-ip-address", default=None, type=str, help="The IP address of this node.")
parser.add_argument("--scheduler-address", default=None, type=str, help="The address of the scheduler.")
parser.add_argument("--trials", default=2, type=int, help="The number of random trials to do.")
parser.add_argument("--steps", default=10, type=int, help="The number of steps of training to do per network.")

if __name__ == "__main__":
  args = parser.parse_args()

  # If node_ip_address and scheduler_address are provided, then this command
  # will connect the driver to the existing scheduler. If not, it will start
  # a local scheduler and connect to it.
  ray.init(start_ray_local=(args.node_ip_address is None),
           node_ip_address=args.node_ip_address,
           scheduler_address=args.scheduler_address,
           num_workers=(10 if args.node_ip_address is None else None))

  # The number of sets of random hyperparameters to try.
  trials = args.trials
  # The number of training passes over the dataset to use for network.
  steps = args.steps

  # Load the mnist data and turn the data into remote objects.
  print "Downloading the MNIST dataset. This may take a minute."
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  train_images = ray.put(mnist.train.images)
  train_labels = ray.put(mnist.train.labels)
  validation_images = ray.put(mnist.validation.images)
  validation_labels = ray.put(mnist.validation.labels)

  # Store the best parameters, the best accuracy, and all of the results.
  best_params = None
  best_accuracy = 0
  results = []

  # Randomly generate some hyperparameters, and launch a task for each set.
  for i in range(trials):
    learning_rate = 10 ** np.random.uniform(-5, 5)
    batch_size = np.random.randint(1, 100)
    dropout = np.random.uniform(0, 1)
    stddev = 10 ** np.random.uniform(-5, 5)
    params = {"learning_rate": learning_rate, "batch_size": batch_size, "dropout": dropout, "stddev": stddev}
    results.append((params, hyperopt.train_cnn_and_compute_accuracy.remote(params, steps, train_images, train_labels, validation_images, validation_labels)))

  # Fetch the results of the tasks and print the results.
  for i in range(trials):
    params, result_id = results[i]
    accuracy = ray.get(result_id)
    print """We achieve accuracy {:.3}% with
        learning_rate: {:.2}
        batch_size: {}
        dropout: {:.2}
        stddev: {:.2}
      """.format(100 * accuracy, params["learning_rate"], params["batch_size"], params["dropout"], params["stddev"])
    if accuracy > best_accuracy:
      best_params = params
      best_accuracy = accuracy

  # Record the best performing set of hyperparameters.
  print """Best accuracy over {} trials was {:.3} with
        learning_rate: {:.2}
        batch_size: {}
        dropout: {:.2}
        stddev: {:.2}
    """.format(trials, 100 * best_accuracy, best_params["learning_rate"], best_params["batch_size"], best_params["dropout"], best_params["stddev"])
