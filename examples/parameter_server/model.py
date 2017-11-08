# Most of the tensorflow code is adapted from Tensorflow's tutorial on using
# CNNs to train MNIST
# https://www.tensorflow.org/get_started/mnist/pros#build-a-multilayer-convolutional-network.  # noqa: E501

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import tensorflow as tf

import mnist_deep


class SimpleCNN(object):
    def __init__(self):
        with tf.Graph().as_default():

            # Create the model
            self.x = tf.placeholder(tf.float32, [None, 784])

            # Define loss and optimizer
            self.y_ = tf.placeholder(tf.float32, [None, 10])

            # Build the graph for the deep net
            self.y_conv, self.keep_prob = mnist_deep.deepnn(self.x)

            with tf.name_scope('loss'):
                cross_entropy = tf.nn.softmax_cross_entropy_with_logits(
                    labels=self.y_, logits=self.y_conv)
            self.cross_entropy = tf.reduce_mean(cross_entropy)

            with tf.name_scope('adam_optimizer'):
                self.optimizer = tf.train.AdamOptimizer(1e-4)
                self.train_step = self.optimizer.minimize(
                    self.cross_entropy)

            with tf.name_scope('accuracy'):
                correct_prediction = tf.equal(tf.argmax(self.y_conv, 1),
                                              tf.argmax(self.y_, 1))
                correct_prediction = tf.cast(correct_prediction, tf.float32)
            self.accuracy = tf.reduce_mean(correct_prediction)

            self.sess = tf.Session(config=tf.ConfigProto(
                intra_op_parallelism_threads=1,
                inter_op_parallelism_threads=1))
            self.sess.run(tf.global_variables_initializer())

            # Helper values.

            self.variables = ray.experimental.TensorFlowVariables(
                self.cross_entropy, self.sess)

            self.grads = self.optimizer.compute_gradients(
                self.cross_entropy)

    def compute_update(self, x, y):
        # TODO(rkn): Computing the weights before and after the training step
        # and taking the diff is awful.
        weights = self.get_weights()[1]
        self.sess.run(self.train_step, feed_dict={self.x: x,
                                                  self.y_: y,
                                                  self.keep_prob: 0.5})
        new_weights = self.get_weights()[1]
        return [x - y for x, y in zip(new_weights, weights)]

    def compute_gradients(self, x, y):
        return self.sess.run([grad[0] for grad in self.grads],
                             feed_dict={self.x: x,
                                        self.y_: y,
                                        self.keep_prob: 0.5})

    def compute_accuracy(self, x, y):
        return self.sess.run(self.accuracy,
                             feed_dict={self.x: x,
                                        self.y_: y,
                                        self.keep_prob: 1.0})

    def set_weights(self, variable_names, weights):
        self.variables.set_weights(dict(zip(variable_names, weights)))

    def get_weights(self):
        weights = self.variables.get_weights()
        return list(weights.keys()), list(weights.values())
