#!/usr/bin/env python
# coding: utf-8

# ##### Copyright 2019 The TensorFlow Authors.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import tensorflow as tf

from tensorflow.keras.layers import Dense, Flatten, Conv2D
from tensorflow.keras import Model
from tensorflow.keras.datasets.mnist import load_data
from ray import tune


def create_mnist_data():
    (x_train, y_train), (x_test, y_test) = load_data()
    x_train, x_test = x_train / 255.0, x_test / 255.0

    # Add a channels dimension
    x_train = x_train[..., tf.newaxis]
    x_test = x_test[..., tf.newaxis]
    return (x_train, y_train), (x_test, y_test)


class MyModel(Model):
    def __init__(self):
        super(MyModel, self).__init__()
        self.conv1 = Conv2D(32, 3, activation="relu")
        self.flatten = Flatten()
        self.d1 = Dense(128, activation="relu")
        self.d2 = Dense(10, activation="softmax")

    def call(self, x):
        x = self.conv1(x)
        x = self.flatten(x)
        x = self.d1(x)
        return self.d2(x)


# import ipdb; ipdb.set_trace()

class MNISTTrainable(tune.Trainable):
    def _setup(self, config):
        (x_train, y_train), (x_test, y_test) = create_mnist_data()

        self.train_ds = tf.data.Dataset.from_tensor_slices(
                (x_train, y_train)).shuffle(10000).batch(32)

        self.test_ds = tf.data.Dataset.from_tensor_slices(
            (x_test, y_test)).batch(32)

        self.model = config["my_model"]()
        # self.loss_object = tf.keras.losses.SparseCategoricalCrossentropy()
        self.optimizer = tf.keras.optimizers.Adam()
        # self.train_loss = tf.keras.metrics.Mean(name="train_loss")
        # self.train_accuracy = tf.keras.metrics.SparseCategoricalAccuracy(
        #     name="train_accuracy")

        # self.test_loss = tf.keras.metrics.Mean(name="test_loss")
        # self.test_accuracy = tf.keras.metrics.SparseCategoricalAccuracy(
        #     name="test_accuracy")
        self.tf_train_step = tf.function(self.train_step)
        self.tf_test_step = tf.function(self.test_step)

    def train_step(self, images, labels):
        with tf.GradientTape() as tape:
            predictions = self.model(images)
            loss = self.loss_object(labels, predictions)
        gradients = tape.gradient(loss, self.model.trainable_variables)
        self.optimizer.apply_gradients(zip(gradients, self.model.trainable_variables))

        self.train_loss(loss)
        self.train_accuracy(labels, predictions)

    def test_step(self, images, labels):
        predictions = self.model(images)
        t_loss = self.loss_object(labels, predictions)

        self.test_loss(t_loss)
        self.test_accuracy(labels, predictions)

    def _train(self):
        self.train_loss.reset_states()
        self.train_accuracy.reset_states()
        self.test_loss.reset_states()
        self.test_accuracy.reset_states()

        for images, labels in train_ds:
            self.tf_train_step(images, labels)

        for test_images, test_labels in test_ds:
            self.tf_test_step(test_images, test_labels)

        return {
            "epoch": epoch+1,
            "loss": self.train_loss.result(),
            "accuracy": self.train_accuracy.result()*100,
            "test_loss": self.test_loss.result(),
            "test_accuracy": self.test_accuracy.result()*100
        }

        # Reset the metrics for the next epoch


# The image classifier is now trained to ~98% accuracy on this dataset. To learn more, read the [TensorFlow tutorials](https://www.tensorflow.org/tutorials).
if __name__ == '__main__':
    tune.run(
        MNISTTrainable,
        stop={"training_iteration": 5},
        config={"my_model": MyModel})
