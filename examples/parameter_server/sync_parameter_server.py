from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import numpy as np
from tensorflow.examples.tutorials.mnist import input_data

import ray
import model

parser = argparse.ArgumentParser(description="Run the parameter server "
                                             "example.")
parser.add_argument("--num-workers", default=4, type=int,
                    help="The number of workers to use.")
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")


@ray.remote
class ParameterServer(object):

    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.net = model.SimpleCNN(learning_rate=1e-4 * num_workers)

    def apply_gradients(self, *gradients):
        self.net.apply_gradients(np.sum(np.array(gradients), axis=0)/self.num_workers)
        return self.net.variables.get_flat()

    def get_weights(self):
        return self.net.variables.get_flat()


@ray.remote
class Worker(object):

    def __init__(self, worker_index, num_workers, batch_size=50, seed=1337):
        self.worker_index = worker_index
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.mnist = input_data.read_data_sets("MNIST_data", one_hot=True, seed=seed)
        self.net = model.SimpleCNN()

    def compute_gradients(self, weights):
        self.net.variables.set_flat(weights)
        s = self.worker_index * self.batch_size
        e = s + self.batch_size
        xs, ys = self.mnist.train.next_batch(self.num_workers * self.batch_size)
        return self.net.compute_gradients(xs[s:e], ys[s:e])


if __name__ == '__main__':
    args = parser.parse_args()

    ray.init(redis_address=args.redis_address)

    # Create a parameter server.
    net = model.SimpleCNN()
    ps = ParameterServer.remote(args.num_workers)

    # Create workers.
    workers = [Worker.remote(worker_index, args.num_workers)
               for worker_index in range(args.num_workers)]

    # Download MNIST.
    mnist = input_data.read_data_sets("MNIST_data", one_hot=True)

    i = 0
    current_weights = ps.get_weights.remote()
    while True:
        # Compute and apply gradients.
        gradients = [worker.compute_gradients.remote(current_weights) for worker in workers]
        current_weights = ps.apply_gradients.remote(*gradients)

        if i % 10 == 0:
            # Evaluate the current model.
            net.variables.set_flat(ray.get(current_weights))
            test_xs, test_ys = mnist.test.next_batch(1000)
            accuracy = net.compute_accuracy(test_xs, test_ys)
            print("Iteration {}: accuracy is {}".format(i, accuracy))
        i += 1
