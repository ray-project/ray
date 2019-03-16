#!/usr/bin/env python
"""Example of how to train a model with Ray SGD.

We use a small model here, so no speedup for distributing the computation is
expected. This example shows:
    - How to set up a simple input pipeline
    - How to evaluate model accuracy during training
    - How to get and set model weights
    - How to train with ray.experimental.sgd.DistributedSGD
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import time

from tensorflow.examples.tutorials.mnist import input_data
import tensorflow as tf

import ray
from ray.tune import run_experiments
from ray.tune.examples.tune_mnist_ray import deepnn
from ray.experimental.sgd.model import Model
from ray.experimental.sgd.sgd import DistributedSGD
import ray.experimental.tf_utils as ray_tf_utils

parser = argparse.ArgumentParser()
parser.add_argument("--redis-address", default=None, type=str)
parser.add_argument("--num-iters", default=10000, type=int)
parser.add_argument("--batch-size", default=50, type=int)
parser.add_argument("--num-workers", default=1, type=int)
parser.add_argument("--devices-per-worker", default=1, type=int)
parser.add_argument("--tune", action="store_true", help="Run in Ray Tune")
parser.add_argument(
    "--strategy", default="ps", type=str, help="One of 'simple' or 'ps'")
parser.add_argument(
    "--gpu", action="store_true", help="Use GPUs for optimization")


class MNISTModel(Model):
    def __init__(self):
        # Import data
        error = None
        for _ in range(10):
            try:
                self.mnist = input_data.read_data_sets(
                    "/tmp/tensorflow/mnist/input_data", one_hot=True)
                error = None
                break
            except Exception as e:
                error = e
                time.sleep(5)
        if error:
            raise ValueError("Failed to import data", error)

        # Set seed and build layers
        tf.set_random_seed(0)

        self.x = tf.placeholder(tf.float32, [None, 784], name="x")
        self.y_ = tf.placeholder(tf.float32, [None, 10], name="y_")
        y_conv, self.keep_prob = deepnn(self.x)

        # Need to define loss and optimizer attributes
        self.loss = tf.reduce_mean(
            tf.nn.softmax_cross_entropy_with_logits(
                labels=self.y_, logits=y_conv))
        self.optimizer = tf.train.AdamOptimizer(1e-4)
        self.variables = ray_tf_utils.TensorFlowVariables(
            self.loss, tf.get_default_session())

        # For evaluating test accuracy
        correct_prediction = tf.equal(
            tf.argmax(y_conv, 1), tf.argmax(self.y_, 1))
        self.accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

    def get_loss(self):
        return self.loss

    def get_optimizer(self):
        return self.optimizer

    def get_variables(self):
        return self.variables

    def get_feed_dict(self):
        batch = self.mnist.train.next_batch(50)
        return {
            self.x: batch[0],
            self.y_: batch[1],
            self.keep_prob: 0.5,
        }

    def get_metrics(self):
        accuracy = self.accuracy.eval(
            feed_dict={
                self.x: self.mnist.test.images,
                self.y_: self.mnist.test.labels,
                self.keep_prob: 1.0,
            })
        return {"accuracy": accuracy}

    def get_weights(self):
        return self.variables.get_flat()

    def set_weights(self, weights):
        self.variables.set_flat(weights)


def train_mnist(config, reporter):
    args = config["args"]
    sgd = DistributedSGD(
        lambda w_i, d_i: MNISTModel(),
        num_workers=args.num_workers,
        devices_per_worker=args.devices_per_worker,
        gpu=args.gpu,
        strategy=args.strategy)

    # Important: synchronize the initial weights of all model replicas
    w0 = sgd.for_model(lambda m: m.get_variables().get_flat())
    sgd.foreach_model(lambda m: m.get_variables().set_flat(w0))

    for i in range(args.num_iters):
        if i % 10 == 0:
            start = time.time()
            loss = sgd.step(fetch_stats=True)["loss"]
            metrics = sgd.foreach_model(lambda model: model.get_metrics())
            acc = [m["accuracy"] for m in metrics]
            print("Iter", i, "loss", loss, "accuracy", acc)
            print("Time per iteration", time.time() - start)
            assert len(set(acc)) == 1, ("Models out of sync", acc)
            reporter(timesteps_total=i, mean_loss=loss, mean_accuracy=acc[0])
        else:
            sgd.step()


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(redis_address=args.redis_address)

    if args.tune:
        run_experiments({
            "mnist_sgd": {
                "run": train_mnist,
                "config": {
                    "args": args,
                },
            },
        })
    else:
        train_mnist({"args": args}, lambda **kw: None)
