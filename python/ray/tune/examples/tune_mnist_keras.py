from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import numpy as np
import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import (Dense, Dropout, Flatten, Conv2D, MaxPooling2D)

from ray.tune.examples.utils import (TuneKerasCallback, get_mnist_data,
                                     set_keras_threads)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test", action="store_true", help="Finish quickly for testing")
args, _ = parser.parse_known_args()


def train_mnist(config, reporter):
    set_keras_threads(config["threads"])
    batch_size = 128
    num_classes = 10
    epochs = 12

    x_train, y_train, x_test, y_test, input_shape = get_mnist_data()

    model = Sequential()
    model.add(
        Conv2D(
            32, kernel_size=(3, 3), activation="relu",
            input_shape=input_shape))
    model.add(Conv2D(64, (3, 3), activation="relu"))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    model.add(Dropout(0.5))
    model.add(Flatten())
    model.add(Dense(config["hidden"], activation="relu"))
    model.add(Dropout(0.5))
    model.add(Dense(num_classes, activation="softmax"))

    model.compile(
        loss=keras.losses.categorical_crossentropy,
        optimizer=keras.optimizers.SGD(
            lr=config["lr"], momentum=config["momentum"]),
        metrics=["accuracy"])

    model.fit(
        x_train,
        y_train,
        batch_size=batch_size,
        epochs=epochs,
        verbose=0,
        validation_data=(x_test, y_test),
        callbacks=[TuneKerasCallback(reporter)])


if __name__ == "__main__":
    import ray
    from ray import tune
    from ray.tune.schedulers import AsyncHyperBandScheduler
    mnist.load_data()  # we do this on the driver because it's not threadsafe

    ray.init()
    sched = AsyncHyperBandScheduler(
        time_attr="timesteps_total",
        reward_attr="mean_accuracy",
        max_t=400,
        grace_period=20)

    tune.run(
        train_mnist,
        name="exp",
        scheduler=sched,
        stop={
            "mean_accuracy": 0.99,
            "training_iteration": 5 if args.smoke_test else 300
        },
        num_samples=10,
        resources_per_trial={
            "cpu": 2,
            "gpu": 0
        },
        config={
            "threads": 2,
            "lr": tune.sample_from(lambda spec: np.random.uniform(0.001, 0.1)),
            "momentum": tune.sample_from(
                lambda spec: np.random.uniform(0.1, 0.9)),
            "hidden": tune.sample_from(
                lambda spec: np.random.randint(32, 512)),
        })
