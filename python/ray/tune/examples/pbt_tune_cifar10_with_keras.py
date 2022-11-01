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
from tensorflow.keras.datasets import cifar10
from tensorflow.keras.layers import Input, Dense, Dropout, Flatten
from tensorflow.keras.layers import Convolution2D, MaxPooling2D
from tensorflow.keras.models import Model, load_model
from tensorflow.keras.preprocessing.image import ImageDataGenerator

from ray import air, tune
from ray.tune import Trainable
from ray.tune.schedulers import PopulationBasedTraining

num_classes = 10
NUM_SAMPLES = 128


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
            kernel_initializer="he_normal",
        )(y)
        y = Convolution2D(
            filters=64,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal",
        )(y)
        y = MaxPooling2D(pool_size=2, strides=2, padding="same")(y)

        y = Convolution2D(
            filters=128,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal",
        )(y)
        y = Convolution2D(
            filters=128,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal",
        )(y)
        y = MaxPooling2D(pool_size=2, strides=2, padding="same")(y)

        y = Convolution2D(
            filters=256,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal",
        )(y)
        y = Convolution2D(
            filters=256,
            kernel_size=3,
            strides=1,
            padding="same",
            activation="relu",
            kernel_initializer="he_normal",
        )(y)
        y = MaxPooling2D(pool_size=2, strides=2, padding="same")(y)

        y = Flatten()(y)
        y = Dropout(self.config.get("dropout", 0.5))(y)
        y = Dense(units=10, activation="softmax", kernel_initializer="he_normal")(y)

        model = Model(inputs=x, outputs=y, name="model1")
        return model

    def setup(self, config):
        self.train_data, self.test_data = self._read_data()
        x_train = self.train_data[0]
        model = self._build_model(x_train.shape[1:])

        opt = tf.keras.optimizers.Adadelta(
            lr=self.config.get("lr", 1e-4), decay=self.config.get("decay", 1e-4)
        )
        model.compile(
            loss="categorical_crossentropy", optimizer=opt, metrics=["accuracy"]
        )
        self.model = model

    def step(self):
        x_train, y_train = self.train_data
        x_train, y_train = x_train[:NUM_SAMPLES], y_train[:NUM_SAMPLES]
        x_test, y_test = self.test_data
        x_test, y_test = x_test[:NUM_SAMPLES], y_test[:NUM_SAMPLES]

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
        batch_size = self.config.get("batch_size", 64)
        gen = aug_gen.flow(x_train, y_train, batch_size=batch_size)
        self.model.fit_generator(
            generator=gen, epochs=self.config.get("epochs", 1), validation_data=None
        )

        # loss, accuracy
        _, accuracy = self.model.evaluate(x_test, y_test, verbose=0)
        return {"mean_accuracy": accuracy}

    def save_checkpoint(self, checkpoint_dir):
        file_path = checkpoint_dir + "/model"
        self.model.save(file_path)
        return file_path

    def load_checkpoint(self, path):
        # See https://stackoverflow.com/a/42763323
        del self.model
        self.model = load_model(path)

    def cleanup(self):
        # If need, save your model when exit.
        # saved_path = self.model.save(self.logdir)
        # print("save model at: ", saved_path)
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    space = {
        "epochs": 1,
        "batch_size": 64,
        "lr": tune.grid_search([10**-4, 10**-5]),
        "decay": tune.sample_from(lambda spec: spec.config.lr / 100.0),
        "dropout": tune.grid_search([0.25, 0.5]),
    }
    if args.smoke_test:
        space["lr"] = 10**-4
        space["dropout"] = 0.5

    perturbation_interval = 10
    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=perturbation_interval,
        hyperparam_mutations={
            "dropout": lambda _: np.random.uniform(0, 1),
        },
    )

    tuner = tune.Tuner(
        tune.with_resources(
            Cifar10Model,
            resources={"cpu": 1, "gpu": 1},
        ),
        run_config=air.RunConfig(
            name="pbt_cifar10",
            stop={
                "mean_accuracy": 0.80,
                "training_iteration": 30,
            },
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=perturbation_interval,
                checkpoint_score_attribute="mean_accuracy",
                num_to_keep=2,
            ),
        ),
        tune_config=tune.TuneConfig(
            scheduler=pbt,
            num_samples=4,
            metric="mean_accuracy",
            mode="max",
        ),
        param_space=space,
    )
    results = tuner.fit()
    print("Best hyperparameters found were: ", results.get_best_result().config)
