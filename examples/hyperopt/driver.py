# Most of the tensorflow code is adapted from Tensorflow's tutorial on using CNNs to train MNIST
# https://www.tensorflow.org/versions/r0.9/tutorials/mnist/pros/index.html#build-a-multilayer-convolutional-network
import numpy as np
import ray
import os

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

import hyperopt

if __name__ == "__main__":
  ray.services.start_ray_local(num_workers=3)

  # The number of sets of random hyperparameters to try.
  trials = 2
  # The number of training passes over the dataset to use for network.
  epochs = 10

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
    results.append((params, hyperopt.train_cnn_and_compute_accuracy.remote(params, epochs, train_images, train_labels, validation_images, validation_labels)))

  # Fetch the results of the tasks and print the results.
  for i in range(trials):
    params, ref = results[i]
    accuracy = ray.get(ref)
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
