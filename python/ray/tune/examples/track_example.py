from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import (Dense, Dropout, Flatten, Conv2D, MaxPooling2D)

from ray.tune import track
from ray.tune.examples.utils import TuneReporterCallback, get_mnist_data

parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test", action="store_true", help="Finish quickly for testing")
parser.add_argument(
    "--lr",
    type=float,
    default=0.01,
    metavar="LR",
    help="learning rate (default: 0.01)")
parser.add_argument(
    "--momentum",
    type=float,
    default=0.5,
    metavar="M",
    help="SGD momentum (default: 0.5)")
parser.add_argument(
    "--hidden", type=int, default=64, help="Size of hidden layer.")
args, _ = parser.parse_known_args()


def train_mnist(args):
    track.init(trial_name="track-example", trial_config=vars(args))
    batch_size = 128
    num_classes = 10
    epochs = 1 if args.smoke_test else 12
    mnist.load()
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
    model.add(Dense(args.hidden, activation="relu"))
    model.add(Dropout(0.5))
    model.add(Dense(num_classes, activation="softmax"))

    model.compile(
        loss="categorical_crossentropy",
        optimizer=keras.optimizers.SGD(lr=args.lr, momentum=args.momentum),
        metrics=["accuracy"])

    model.fit(
        x_train,
        y_train,
        batch_size=batch_size,
        epochs=epochs,
        validation_data=(x_test, y_test),
        callbacks=[TuneReporterCallback(track.metric)])
    track.shutdown()


if __name__ == "__main__":
    train_mnist(args)
