#!/usr/bin/env python
#
# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""A deep MNIST classifier using convolutional layers.
See extensive documentation at
https://www.tensorflow.org/get_started/mnist/pros
"""
# Disable linter warnings to maintain consistency with tutorial.
# pylint: disable=invalid-name
# pylint: disable=g-bad-import-order

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import time

import ray
from ray import tune
from ray.tune import grid_search, Trainable, sample_from
from ray.tune.schedulers import HyperBandScheduler
from tensorflow.examples.tutorials.mnist import input_data

import tensorflow as tf
import numpy as np

activation_fn = None  # e.g. tf.nn.relu


def setupCNN(x):
    """setupCNN builds the graph for a deep net for classifying digits.
    Args:
        x: an input tensor with the dimensions (N_examples, 784), where 784 is
        the number of pixels in a standard MNIST image.
    Returns:
        A tuple (y, keep_prob). y is a tensor of shape (N_examples, 10), with
        values equal to the logits of classifying the digit into one of 10
        classes (the digits 0-9). keep_prob is a scalar placeholder for the
        probability of dropout.
    """
    # Reshape to use within a convolutional neural net.
    # Last dimension is for "features" - there is only one here, since images
    # are grayscale -- it would be 3 for an RGB image, 4 for RGBA, etc.
    with tf.name_scope('reshape'):
        x_image = tf.reshape(x, [-1, 28, 28, 1])

    # First convolutional layer - maps one grayscale image to 32 feature maps.
    with tf.name_scope('conv1'):
        W_conv1 = weight_variable([5, 5, 1, 32])
        b_conv1 = bias_variable([32])
        h_conv1 = activation_fn(conv2d(x_image, W_conv1) + b_conv1)

    # Pooling layer - downsamples by 2X.
    with tf.name_scope('pool1'):
        h_pool1 = max_pool_2x2(h_conv1)

    # Second convolutional layer -- maps 32 feature maps to 64.
    with tf.name_scope('conv2'):
        W_conv2 = weight_variable([5, 5, 32, 64])
        b_conv2 = bias_variable([64])
        h_conv2 = activation_fn(conv2d(h_pool1, W_conv2) + b_conv2)

    # Second pooling layer.
    with tf.name_scope('pool2'):
        h_pool2 = max_pool_2x2(h_conv2)

    # Fully connected layer 1 -- after 2 round of downsampling, our 28x28 image
    # is down to 7x7x64 feature maps -- maps this to 1024 features.
    with tf.name_scope('fc1'):
        W_fc1 = weight_variable([7 * 7 * 64, 1024])
        b_fc1 = bias_variable([1024])

        h_pool2_flat = tf.reshape(h_pool2, [-1, 7 * 7 * 64])
        h_fc1 = activation_fn(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)

    # Dropout - controls the complexity of the model, prevents co-adaptation of
    # features.
    with tf.name_scope('dropout'):
        keep_prob = tf.placeholder(tf.float32)
        h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)

    # Map the 1024 features to 10 classes, one for each digit
    with tf.name_scope('fc2'):
        W_fc2 = weight_variable([1024, 10])
        b_fc2 = bias_variable([10])

        y_conv = tf.matmul(h_fc1_drop, W_fc2) + b_fc2
    return y_conv, keep_prob


def conv2d(x, W):
    """conv2d returns a 2d convolution layer with full stride."""
    return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding='SAME')


def max_pool_2x2(x):
    """max_pool_2x2 downsamples a feature map by 2X."""
    return tf.nn.max_pool(
        x, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME')


def weight_variable(shape):
    """weight_variable generates a weight variable of a given shape."""
    initial = tf.truncated_normal(shape, stddev=0.1)
    return tf.Variable(initial)


def bias_variable(shape):
    """bias_variable generates a bias variable of a given shape."""
    initial = tf.constant(0.1, shape=shape)
    return tf.Variable(initial)


class TrainMNIST(Trainable):
    """Example MNIST trainable."""

    def _setup(self, config):
        global activation_fn

        self.timestep = 0

        # Import data
        for _ in range(10):
            try:
                self.mnist = input_data.read_data_sets(
                    "/tmp/mnist_ray_demo", one_hot=True)
                break
            except Exception as e:
                print("Error loading data, retrying", e)
                time.sleep(5)

        assert self.mnist

        self.x = tf.placeholder(tf.float32, [None, 784])
        self.y_ = tf.placeholder(tf.float32, [None, 10])

        activation_fn = getattr(tf.nn, config['activation'])

        # Build the graph for the deep net
        y_conv, self.keep_prob = setupCNN(self.x)

        with tf.name_scope('loss'):
            cross_entropy = tf.nn.softmax_cross_entropy_with_logits(
                labels=self.y_, logits=y_conv)
        cross_entropy = tf.reduce_mean(cross_entropy)

        with tf.name_scope('adam_optimizer'):
            train_step = tf.train.AdamOptimizer(
                config['learning_rate']).minimize(cross_entropy)

        self.train_step = train_step

        with tf.name_scope('accuracy'):
            correct_prediction = tf.equal(
                tf.argmax(y_conv, 1), tf.argmax(self.y_, 1))
            correct_prediction = tf.cast(correct_prediction, tf.float32)
        self.accuracy = tf.reduce_mean(correct_prediction)

        self.sess = tf.Session()
        self.sess.run(tf.global_variables_initializer())
        self.iterations = 0
        self.saver = tf.train.Saver()

    def _train(self):
        for i in range(10):
            batch = self.mnist.train.next_batch(50)
            self.sess.run(
                self.train_step,
                feed_dict={
                    self.x: batch[0],
                    self.y_: batch[1],
                    self.keep_prob: 0.5
                })

        batch = self.mnist.train.next_batch(50)
        train_accuracy = self.sess.run(
            self.accuracy,
            feed_dict={
                self.x: batch[0],
                self.y_: batch[1],
                self.keep_prob: 1.0
            })

        self.iterations += 1
        return {"mean_accuracy": train_accuracy}

    def _save(self, checkpoint_dir):
        prefix = self.saver.save(
            self.sess, checkpoint_dir + "/save", global_step=self.iterations)
        return {"prefix": prefix}

    def _restore(self, ckpt_data):
        prefix = ckpt_data["prefix"]
        return self.saver.restore(self.sess, prefix)


# !!! Example of using the ray.tune Python API !!!
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--smoke-test', action='store_true', help='Finish quickly for testing')
    args, _ = parser.parse_known_args()
    mnist_spec = {
        'stop': {
            'mean_accuracy': 0.99,
            'time_total_s': 600,
        },
        'config': {
            'learning_rate': sample_from(
                lambda spec: 10**np.random.uniform(-5, -3)),
            'activation': grid_search(['relu', 'elu', 'tanh']),
        },
        "num_samples": 10,
    }

    if args.smoke_test:
        mnist_spec['stop']['training_iteration'] = 20
        mnist_spec['num_samples'] = 2

    ray.init()
    hyperband = HyperBandScheduler(
        time_attr="training_iteration", reward_attr="mean_accuracy", max_t=10)

    tune.run(
        TrainMNIST,
        name='mnist_hyperband_test',
        scheduler=hyperband,
        **mnist_spec)
