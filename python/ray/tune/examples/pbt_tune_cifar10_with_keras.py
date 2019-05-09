#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Train keras CNN on the CIFAR10 small images dataset.

The model comes from: https://zhuanlan.zhihu.com/p/29214791,
and it gets to about 87% validation accuracy in 100 epochs.

Note that the script requires a machine with 4 GPUs. You
can set {"gpu": 0} to use CPUs for training, although
it is less efficient.
"""

from __future__ import print_function

import argparse

import numpy as np
import tensorflow as tf
from tensorflow.python.keras.datasets import cifar10
from tensorflow.python.keras.layers import Input, Dense, Dropout, Flatten
from tensorflow.python.keras.layers import Convolution2D, MaxPooling2D
from tensorflow.python.keras.models import Model
from tensorflow.python.keras.preprocessing.image import ImageDataGenerator

import ray
from ray.tune import grid_search, run, sample_from
from ray.tune import Trainable
from ray.tune.schedulers import PopulationBasedTraining

num_classes = 10


class Cifar10Model(Trainable):
    def _read_data(self):
        # The data, split between train and test sets:
        (x_train, y_train), (x_test, y_test) = cifar10.load_data()

        # Convert class vectors to binary class matrices.
        y_train = tf.keras.utils.to_categorical(y_train, num_classes)
        y_test = tf.keras.utils.to_categorical(y_test, num_classes)

        x_train = x_train.astype("float32")
        x_train /= 255
        x_test = x_test.astype("float32")
        x_test /= 255

        return (x_train, y_train), (x_test, y_test)

    def _build_model(self, input_shape):
        x = Input(shape=(32, 32, 3))
        y = x
        y = Convolution2D(
            filters=64,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal")(y)
        y = Convolution2D(
            filters=64,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal")(y)
        y = MaxPooling2D(pool_size=2, strides=2, padding="same")(y)

        y = Convolution2D(
            filters=128,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal")(y)
        y = Convolution2D(
            filters=128,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal")(y)
        y = MaxPooling2D(pool_size=2, strides=2, padding="same")(y)

        y = Convolution2D(
            filters=256,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal")(y)
        y = Convolution2D(
            filters=256,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal")(y)
        y = MaxPooling2D(pool_size=2, strides=2, padding="same")(y)

        y = Flatten()(y)
        y = Dropout(self.config["dropout"])(y)
        y = Dense(
            units=10, activation="softmax", kernel_initializer="he_normal")(y)

        model = Model(inputs=x, outputs=y, name="model1")
        return model

    def _setup(self, config):
        self.train_data, self.test_data = self._read_data()
        x_train = self.train_data[0]
        model = self._build_model(x_train.shape[1:])

        opt = tf.keras.optimizers.Adadelta(
            lr=self.config["lr"], decay=self.config["decay"])
        model.compile(
            loss="categorical_crossentropy",
            optimizer=opt,
            metrics=["accuracy"])
        self.model = model

    def _train(self):
        x_train, y_train = self.train_data
        x_test, y_test = self.test_data

        aug_gen = ImageDataGenerator(
            # set input mean to 0 over the dataset
            featurewise_center=False,
            # set each sample mean to 0
            samplewise_center=False,
            # divide inputs by dataset std
            featurewise_std_normalization=False,
            # divide each input by its std
            samplewise_std_normalization=False,
            # apply ZCA whitening
            zca_whitening=False,
            # randomly rotate images in the range (degrees, 0 to 180)
            rotation_range=0,
            # randomly shift images horizontally (fraction of total width)
            width_shift_range=0.1,
            # randomly shift images vertically (fraction of total height)
            height_shift_range=0.1,
            # randomly flip images
            horizontal_flip=True,
            # randomly flip images
            vertical_flip=False,
        )

        aug_gen.fit(x_train)
        gen = aug_gen.flow(
            x_train, y_train, batch_size=self.config["batch_size"])
        self.model.fit_generator(
            generator=gen,
            steps_per_epoch=50000 // self.config["batch_size"],
            epochs=self.config["epochs"],
            validation_data=None)

        # loss, accuracy
        _, accuracy = self.model.evaluate(x_test, y_test, verbose=0)
        return {"mean_accuracy": accuracy}

    def _save(self, checkpoint_dir):
        file_path = checkpoint_dir + "/model"
        self.model.save_weights(file_path)
        return file_path

    def _restore(self, path):
        self.model.load_weights(path)

    def _stop(self):
        # If need, save your model when exit.
        # saved_path = self.model.save(self.logdir)
        # print("save model at: ", saved_path)
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    train_spec = {
        "resources_per_trial": {
            "cpu": 1,
            "gpu": 1
        },
        "stop": {
            "mean_accuracy": 0.80,
            "training_iteration": 30,
        },
        "config": {
            "epochs": 1,
            "batch_size": 64,
            "lr": grid_search([10**-4, 10**-5]),
            "decay": sample_from(lambda spec: spec.config.lr / 100.0),
            "dropout": grid_search([0.25, 0.5]),
        },
        "num_samples": 4,
    }

    if args.smoke_test:
        train_spec["config"]["lr"] = 10**-4
        train_spec["config"]["dropout"] = 0.5

    ray.init()

    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        reward_attr="mean_accuracy",
        perturbation_interval=10,
        hyperparam_mutations={
            "dropout": lambda _: np.random.uniform(0, 1),
        })

    run(Cifar10Model, name="pbt_cifar10", scheduler=pbt, **train_spec)
