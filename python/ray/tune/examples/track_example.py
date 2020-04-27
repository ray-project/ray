import argparse
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.datasets import mnist

from ray.tune import track
from ray.tune.integration.keras import TuneReporterCallback

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

    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train, x_test = x_train / 255.0, x_test / 255.0
    model = tf.keras.models.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(args.hidden, activation="relu"),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(num_classes, activation="softmax")
    ])

    model.compile(
        loss="sparse_categorical_crossentropy",
        optimizer=keras.optimizers.SGD(lr=args.lr, momentum=args.momentum),
        metrics=["accuracy"])

    model.fit(
        x_train,
        y_train,
        batch_size=batch_size,
        epochs=epochs,
        validation_data=(x_test, y_test),
        callbacks=[TuneReporterCallback()])
    track.shutdown()


if __name__ == "__main__":
    train_mnist(args)
